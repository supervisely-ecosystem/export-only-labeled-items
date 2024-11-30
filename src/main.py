import os
from datetime import datetime
from distutils import util

import supervisely as sly
from dotenv import load_dotenv
from supervisely.api.module_api import ApiField
from supervisely.io.json import dump_json_file
from supervisely.project.pointcloud_project import PointcloudProject
from supervisely.project.project import OpenMode, Progress, Project
from supervisely.project.video_project import VideoProject
from supervisely.video_annotation.key_id_map import KeyIdMap

import workflow as w
import time
import asyncio

class Timer:
    def __init__(self, message=None, items_cnt=None):
        self.message = message
        self.items_cnt = items_cnt
        self.elapsed = 0

    def __enter__(self):
        self.start = time.perf_counter()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end = time.perf_counter()
        self.elapsed = self.end - self.start
        msg = self.message or f"Block execution"
        if self.items_cnt is not None:
            log_msg = f"{msg} time: {self.elapsed:.3f} seconds per {self.items_cnt} item  ({self.elapsed/self.items_cnt:.3f} seconds per item)"
        else:
            log_msg = f"{msg} time: {self.elapsed:.3f} seconds"
        sly.logger.info(log_msg)

if sly.is_development():
    sly.logger.info("Launching locally")
    load_dotenv("local.env")
    load_dotenv(os.path.expanduser("~/supervisely.env"))

# region envvars
team_id = sly.env.team_id()
workspace_id = sly.env.workspace_id()
project_id = sly.env.project_id()
task_id = sly.env.task_id(raise_not_found=False)
# endregion

# region constants
SIZE_LIMIT = 10 if sly.is_community() else 100
SIZE_LIMIT_BYTES = SIZE_LIMIT * (1024 ** 3)
SPLIT_MODE = "MB"
SPLIT_SIZE = 500  # do not increase this value (memory issues)
RESULT_DIR_NAME = "export_only_labeled_items"
DATA_DIR = os.path.join(os.getcwd(), "data")
# endregion
sly.fs.mkdir(DATA_DIR, remove_content_if_exists=True)


try:
    os.environ["modal.state.items"]
except KeyError:
    sly.logger.warn(
        "The option to download items is not selected, project will be downloaded with items"
    )
    DOWNLOAD_ITEMS = True
else:
    DOWNLOAD_ITEMS = bool(util.strtobool(os.environ["modal.state.items"]))


def export_only_labeled_items(api: sly.Api):
    project = api.project.get_info_by_id(project_id)
    if project is None:
        raise RuntimeError(f"Project with the given ID {project_id} not found")
    w.workflow_input(api, project.id)
    project_name = project.name
    meta_json = api.project.get_meta(project_id)
    meta = sly.ProjectMeta.from_json(meta_json)

    if len(meta.obj_classes) == 0 and len(meta.tag_metas) == 0:
        sly.logger.warn("Project {} have no labeled items".format(project_name))

    timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")

    RESULT_DIR = os.path.join(DATA_DIR, RESULT_DIR_NAME, project_name)
    RESULT_PROJECT_DIR = os.path.join(DATA_DIR, RESULT_DIR_NAME)
    ARCHIVE_NAME = f"{project_id}_{project_name}.tar.gz"
    RESULT_ARCHIVE_DIR = os.path.join(DATA_DIR, timestamp)
    sly.fs.mkdir(RESULT_ARCHIVE_DIR, remove_content_if_exists=True)
    RESULT_ARCHIVE = os.path.join(RESULT_ARCHIVE_DIR, ARCHIVE_NAME)
    remote_path = os.path.join(
        sly.team_files.RECOMMENDED_EXPORT_PATH,
        "export-only-labeled-items",
        timestamp,
    )

    sly.fs.mkdir(RESULT_DIR, True)
    sly.logger.info("Export folder has been created")

    if project.type == str(sly.ProjectType.IMAGES):
        project_fs = Project(RESULT_DIR, OpenMode.CREATE)
        project_fs.set_meta(meta)
        for parents, dataset_info in api.dataset.tree(project_id):
            dataset_path = sly.Dataset._get_dataset_path(dataset_info.name, parents)
            dataset_id = dataset_info.id
            dataset_fs = project_fs.create_dataset(dataset_info.name, dataset_path)

            images = api.image.get_list(dataset_id)

            total_items_cnt = len(images)
            not_labeled_items_cnt = 0

            ids = [info.id for info in images]
            img_names = [info.name for info in images]
            try:
                ann_progress = Progress("Downloading annotations...", total_items_cnt, min_report_percent=10)
                anns = []
                with Timer("Annotation downloading", total_items_cnt):
                    coro = api.annotation.download_bulk_async(
                        dataset_id, ids, ann_progress.iters_done_report
                    )
                    loop = sly.utils.get_or_create_event_loop()
                    if loop.is_running():
                        future = asyncio.run_coroutine_threadsafe(coro, loop)
                        anns.extend(future.result())
                    else:
                        anns.extend(loop.run_until_complete(coro))
                ann_jsons = [ann_info.annotation for ann_info in anns]
            except Exception as e:
                sly.logger.warn(
                    f"Can not download {total_items_cnt} annotations from dataset {dataset_info.name}: {repr(e)}. Skip batch."
                )
                continue

            if DOWNLOAD_ITEMS:
                try:
                    image_progress = Progress("Downloading images...", total_items_cnt, min_report_percent=10)
                    img_bytes = []
                    with Timer("Image downloading", total_items_cnt):
                        coro = api.image.download_bytes_many_async(
                            ids, progress_cb=image_progress.iters_done_report
                        )
                        loop = sly.utils.get_or_create_event_loop()
                        if loop.is_running():
                            future = asyncio.run_coroutine_threadsafe(coro, loop)
                            img_bytes.extend(future.result())
                        else:
                            img_bytes.extend(loop.run_until_complete(coro))
                    for name, img_bytes, ann_json in zip(img_names, img_bytes, ann_jsons):
                        ann = sly.Annotation.from_json(ann_json, meta)
                        if ann.is_empty():
                            not_labeled_items_cnt += 1
                            continue
                        dataset_fs.add_item_raw_bytes(name, img_bytes, ann_json)
                except:
                    sly.logger.warning(
                        f"Can not download {total_items_cnt} images from dataset {dataset_info.name}: {repr(e)}. Skip batch."
                    )
                    continue
            else:
                ann_dir = os.path.join(RESULT_DIR, dataset_info.name, "ann")
                sly.fs.mkdir(ann_dir)
                for image_name, ann_json in zip(img_names, ann_jsons):
                    ann = sly.Annotation.from_json(ann_json, meta)
                    if ann.is_empty():
                        not_labeled_items_cnt += 1
                        continue
                    sly.io.json.dump_json_file(
                        ann_json, os.path.join(ann_dir, image_name + ".json")
                    )

            if total_items_cnt == not_labeled_items_cnt:
                sly.logger.warning(
                    "There are no labeled items in dataset {}".format(dataset_info.name)
                )
            else:
                sly.logger.info(f"Dataset {dataset_info.name} has {total_items_cnt-not_labeled_items_cnt}/{total_items_cnt} items labeled")

    elif project.type == str(sly.ProjectType.VIDEOS):
        key_id_map = KeyIdMap()
        project_fs = VideoProject(RESULT_DIR, OpenMode.CREATE)
        project_fs.set_meta(meta)
        for dataset_info in api.dataset.get_list(project_id):
            dataset_fs = project_fs.create_dataset(dataset_info.name)
            videos = api.video.get_list(dataset_info.id)
            not_labeled_items_cnt = 0
            total_items_cnt = len(videos)

            video_ids = [video_info.id for video_info in videos]
            video_names = [video_info.name for video_info in videos]
            try:
                video_ann_progress = Progress(
                    "Downloading video annotations...", total_items_cnt, min_report_percent=10
                )
                with Timer("Video annotation downloading", total_items_cnt):
                    coro = api.video.annotation.download_bulk_async(
                        dataset_info.id, video_ids, progress_cb=video_ann_progress.iters_done_report
                    )  # not implemented yet
                    loop = sly.utils.get_or_create_event_loop()
                    if loop.is_running():
                        future = asyncio.run_coroutine_threadsafe(coro, loop)
                        future.result()
                    else:
                        loop.run_until_complete(coro)
            except Exception as e:
                sly.logger.warn(
                    f"Can not download {len(video_ids)} annotations: {repr(e)}. Skip batch."
                )
                continue
            video_paths = []
            for i, (video_name, ann_json) in enumerate(zip(video_names, ann_jsons)):
                video_ann = sly.VideoAnnotation.from_json(ann_json, meta, key_id_map)
                if video_ann.is_empty():
                    not_labeled_items_cnt += 1
                    video_ids.pop(i)
                    continue

                video_path = None
                if DOWNLOAD_ITEMS:
                    video_path = dataset_fs.generate_item_path(video_name)
                    video_paths.append(video_path)
                dataset_fs.add_item_file(
                    video_name, video_path, ann=video_ann, _validate_item=False
                )
            if len(video_paths) == len(video_ids):
                progress = Progress("Downloading videos", len(video_ids))
                try:
                    with Timer("Video downloading", len(video_ids)):
                        coro = api.video.download_paths_async(
                            video_ids, video_paths, progress_cb=progress.iters_done_report
                        )
                        loop = sly.utils.get_or_create_event_loop()
                        if loop.is_running():
                            future = asyncio.run_coroutine_threadsafe(coro, loop)
                            future.result()
                        else:
                            loop.run_until_complete(coro)
                except Exception as e:
                    sly.logger.warning(
                        f"An error occured while downloading videos. Error: {repr(e)}"
                    )

            if total_items_cnt == not_labeled_items_cnt:
                sly.logger.warning(
                    "There are no labeled items in dataset {}".format(dataset_info.name)
                )
            else:
                sly.logger.info(
                    f"Dataset {dataset_info.name} has {total_items_cnt-not_labeled_items_cnt}/{total_items_cnt} items labeled"
                )

        project_fs.set_key_id_map(key_id_map)

    elif project.type == str(sly.ProjectType.POINT_CLOUDS):
        key_id_map = KeyIdMap()
        project_fs = PointcloudProject(RESULT_DIR, OpenMode.CREATE)
        project_fs.set_meta(meta)
        for dataset_info in api.dataset.get_list(project_id):
            dataset_fs = project_fs.create_dataset(dataset_info.name)
            pointclouds = api.pointcloud.get_list(dataset_info.id)
            not_labeled_items_cnt = 0
            total_items_cnt = len(pointclouds)

            pointcloud_ids = [pointcloud_info.id for pointcloud_info in pointclouds]
            pointcloud_names = [pointcloud_info.name for pointcloud_info in pointclouds]
            pcd_file_paths = [dataset_fs.generate_item_path(name) for name in pointcloud_names]

            anns_json = []
            try:
                ann_progress = Progress(
                    "Downloading annotations...", total_items_cnt, min_report_percent=10
                )
                with Timer("Annotation downloading", total_items_cnt):
                    coro = api.pointcloud.annotation.download_bulk_async(  # not implemented yet
                        dataset_id, ids, progress_cb=ann_progress.iters_done_report
                    )
                    loop = sly.utils.get_or_create_event_loop()
                    if loop.is_running():
                        future = asyncio.run_coroutine_threadsafe(coro, loop)
                        anns_json.extend(future.result())
                    else:
                        anns_json.extend(loop.run_until_complete(coro))
            except Exception as e:
                sly.logger.warn(
                    f"Can not download {total_items_cnt} annotations from dataset {dataset_info.name}: {repr(e)}. Skip batch."
                )
                continue

            if DOWNLOAD_ITEMS:
                try:
                    with Timer("Pointcloud downloading", total_items_cnt):
                        coro = api.pointcloud.download_paths_async(
                            pointcloud_ids,
                            pcd_file_paths,
                        )
                        loop = sly.utils.get_or_create_event_loop()
                        if loop.is_running():
                            future = asyncio.run_coroutine_threadsafe(coro, loop)
                            future.result()
                        else:
                            loop.run_until_complete(coro)
                except Exception as e:
                    sly.logger.warning(
                        f"An error occured while downloading PCD items from dataset: {dataset_info.name}. Error: {repr(e)}"
                    )
                    continue

                rimage_paths = []
                rimage_ids = []
                for pcd_id, pcd_name in zip(pointcloud_ids, pointcloud_names):
                    rimage_path = dataset_fs.get_related_images_path(pcd_name)
                    rimage_info = api.pointcloud.get_list_related_images(pcd_id)
                    name = rimage_info[ApiField.NAME]
                    rimage_ids.append(rimage_info[ApiField.ID])
                    rimage_paths.append(os.path.join(rimage_path, name))
                    path_json = os.path.join(rimage_path, name + ".json")
                    dump_json_file(rimage_info, path_json)
                try:
                    with Timer("Related image downloading", len(rimage_ids)):
                        coro = api.pointcloud.download_related_images_async(
                            rimage_ids, rimage_paths
                        )
                        loop = sly.utils.get_or_create_event_loop()
                        if loop.is_running():
                            future = asyncio.run_coroutine_threadsafe(coro, loop)
                            future.result()
                        else:
                            loop.run_until_complete(coro)
                except Exception as e:
                    sly.logger.warning(
                        f"An error occured while downloading PCD related images. Error: {repr(e)}"
                    )
                    continue

            for pcd_path, pointcloud_name, ann_json in zip(
                pcd_file_paths, pointcloud_names, anns_json
            ):
                pc_ann = sly.PointcloudAnnotation.from_json(ann_json, meta, key_id_map)
                if pc_ann.is_empty():
                    not_labeled_items_cnt += 1
                    continue
                dataset_fs.add_item_file(
                    pointcloud_name,
                    pcd_path,
                    ann=pc_ann,
                    _validate_item=False,
                )
            if total_items_cnt == not_labeled_items_cnt:
                sly.logger.warning(
                    "There are no labeled items in dataset {}".format(dataset_info.name)
                )
            else:
                sly.logger.info(
                    f"Dataset {dataset_info.name} has {total_items_cnt-not_labeled_items_cnt}/{total_items_cnt} items labeled"
                )
        project_fs.set_key_id_map(key_id_map)

    dir_size = sly.fs.get_directory_size(RESULT_PROJECT_DIR)
    dir_size_gb = round(dir_size / (1024 * 1024 * 1024), 2)

    if dir_size < SIZE_LIMIT_BYTES:
        sly.logger.debug(f"Result archive size ({dir_size_gb} GB) less than limit {SIZE_LIMIT} GB")
        file_info = sly.output.set_download(RESULT_PROJECT_DIR)
        w.workflow_output(api, file_info)
        sly.logger.info(f"Project {project_name} has been successfully exported.")
        return

    # TODO: Add option to split archive by parts into sly.output.set_download() method and remove the code below.

    sly.logger.info(f"Result archive size ({dir_size_gb} GB) more than {SIZE_LIMIT} GB")
    split = f"{SPLIT_SIZE}{SPLIT_MODE}"
    sly.logger.info(f"It will be uploaded with splitting by {split}")
    splits = sly.fs.archive_directory(RESULT_PROJECT_DIR, RESULT_ARCHIVE, split=split)
    sly.logger.info(f"Result directory is archived {'with splitting' if splits else ''}")

    sly.fs.remove_dir(RESULT_PROJECT_DIR)  # remove dir

    upload_progress = []
    upload_msg = f"Uploading splitted archive {ARCHIVE_NAME} to Team Files"

    def _print_progress(monitor, upload_progress):
        if len(upload_progress) == 0:
            upload_progress.append(
                sly.Progress(
                    message=upload_msg,
                    total_cnt=monitor.len,
                    ext_logger=sly.logger,
                    is_size=True,
                )
            )
        upload_progress[0].set_current_value(monitor.bytes_read)

    res_remote_dir = api.file.upload_directory(
        team_id,
        RESULT_ARCHIVE_DIR,
        remote_path,
        progress_size_cb=lambda m: _print_progress(m, upload_progress),
    )
    main_part_name = os.path.basename(splits[0])
    main_part = os.path.join(res_remote_dir, main_part_name)
    file_info = api.file.get_info_by_path(team_id, main_part)
    try:
        api.task.set_output_directory(
            task_id=task_id, file_id=file_info.id, directory_path=res_remote_dir
        )
    except Exception:
        # For convinient debugging without setting TASK_ID in local.env.
        sly.logger.debug(
            "Task ID is not set in local.env file, it has no effect in development mode."
        )
    w.workflow_output(api, file_info)
    sly.logger.info(f"Uploaded to Team-Files: {res_remote_dir}")


if __name__ == "__main__":
    api = sly.Api.from_env()
    export_only_labeled_items(api)
