import asyncio
import os
from datetime import datetime
from distutils import util
from typing import Dict, List, Literal, Optional, Tuple

import supervisely as sly
from dotenv import load_dotenv
from supervisely.api.module_api import ApiField
from supervisely.io.json import dump_json_file
from supervisely.project.pointcloud_project import PointcloudDataset, PointcloudProject
from supervisely.project.project import OpenMode, Project
from supervisely.task.progress import Progress
from supervisely.project.video_project import VideoProject
from supervisely.video_annotation.key_id_map import KeyIdMap

import workflow as w

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
SIZE_LIMIT_BYTES = SIZE_LIMIT * (1024**3)
SPLIT_MODE = "MB"
SPLIT_SIZE = 500  # do not increase this value (memory issues)
RESULT_DIR_NAME = "export_only_labeled_items"
DATA_DIR = os.path.join(os.getcwd(), "data")
# endregion
sly.fs.mkdir(DATA_DIR, remove_content_if_exists=True)


try:
    os.environ["modal.state.items"]
except KeyError:
    sly.logger.warning(
        "The option to download items is not selected, project will be downloaded with items"
    )
    DOWNLOAD_ITEMS = True
else:
    DOWNLOAD_ITEMS = bool(util.strtobool(os.environ["modal.state.items"]))


def filter_unlabeled_items(
    item_type: Literal["image", "video", "pointcloud"],
    meta: sly.ProjectMeta,
    ann_jsons: List[dict],
    items_ids: List[int],
    items_names: List[str],
    not_labeled_items_cnt: int,
    key_id_map: Optional[KeyIdMap] = None,  # for video and pointcloud projects
) -> Tuple[
    List[Dict],
    List[int],
    List[str],
    int,
    List[sly.Annotation],
]:
    """Creates new lists of labeled items and annotations.

    Returns Tuple of filtered:
     - list of annotations
     - list of item IDs
     - list of item names
     - number of not labeled items
     - list of annotations objects
    """
    ds_progress = sly.tqdm_sly(
        desc=f"Filter unlabeled items",
        total=len(ann_jsons),
    )
    ann_jsons_filtered = []
    item_ids_filtered = []
    item_names_filtered = []
    ann_objects = []
    for idx, ann_json in enumerate(ann_jsons):
        if item_type == "image":
            ann = sly.Annotation.from_json(ann_json, meta)
        elif item_type == "video":
            ann = sly.VideoAnnotation.from_json(ann_json, meta, key_id_map)
        elif item_type == "pointcloud":
            ann = sly.PointcloudAnnotation.from_json(ann_json, meta, key_id_map)
        if ann.is_empty():
            not_labeled_items_cnt += 1
        else:
            ann_objects.append(ann)
            ann_jsons_filtered.append(ann_json)
            item_ids_filtered.append(items_ids[idx])
            item_names_filtered.append(items_names[idx])
        ds_progress(1)
    sly.logger.info(f"Labeled items to download: {len(ann_jsons_filtered)}")
    return (
        ann_jsons_filtered,
        item_ids_filtered,
        item_names_filtered,
        not_labeled_items_cnt,
        ann_objects,
    )


def export_only_labeled_items(api: sly.Api):
    project = api.project.get_info_by_id(project_id)
    if project is None:
        raise RuntimeError(f"Project with the given ID {project_id} not found")
    w.workflow_input(api, project.id)
    project_name = project.name
    meta_json = api.project.get_meta(project_id)
    meta = sly.ProjectMeta.from_json(meta_json)

    if len(meta.obj_classes) == 0 and len(meta.tag_metas) == 0:
        sly.logger.warning("Project {} have no labeled items".format(project_name))

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
            sly.logger.info(f"Processing dataset {dataset_info.name}...")
            dataset_path = sly.Dataset._get_dataset_path(dataset_info.name, parents)
            dataset_id = dataset_info.id
            dataset_fs = project_fs.create_dataset(dataset_info.name, dataset_path)

            images = api.image.get_list(dataset_id)

            total_items_cnt = len(images)
            not_labeled_items_cnt = 0

            ids = [info.id for info in images]
            img_names = [info.name for info in images]
            ann_progress = sly.tqdm_sly(desc="Downloading annotations", total=total_items_cnt)
            try:
                coro = api.annotation.download_bulk_async(dataset_id, ids, ann_progress)
                loop = sly.utils.get_or_create_event_loop()
                if loop.is_running():
                    future = asyncio.run_coroutine_threadsafe(coro, loop)
                    anns = future.result()
                else:
                    anns = loop.run_until_complete(coro)
                ann_jsons = [ann_info.annotation for ann_info in anns]
            except Exception as e:
                sly.logger.warning(
                    f"Can not download {total_items_cnt} annotations from dataset {dataset_info.name}: {repr(e)}. Skipping."
                )
                continue

            (
                ann_jsons_filtered,
                item_ids_filtered,
                item_names_filtered,
                not_labeled_items_cnt,
                _,
            ) = filter_unlabeled_items(
                "image",
                meta,
                ann_jsons,
                ids,
                img_names,
                not_labeled_items_cnt,
            )
            if DOWNLOAD_ITEMS:
                image_progress = sly.tqdm_sly(
                    desc="Downloading images", total=len(ann_jsons_filtered)
                )
                try:
                    coro = api.image.download_bytes_many_async(
                        item_ids_filtered, progress_cb=image_progress
                    )
                    loop = sly.utils.get_or_create_event_loop()
                    if loop.is_running():
                        future = asyncio.run_coroutine_threadsafe(coro, loop)
                        img_bytes_many = future.result()
                    else:
                        img_bytes_many = loop.run_until_complete(coro)

                    ds_progress = sly.tqdm_sly(
                        desc=f"Processing dataset items",
                        total=len(img_bytes_many),
                    )
                    for name, img_bytes, ann_json in zip(
                        item_names_filtered, img_bytes_many, ann_jsons_filtered
                    ):
                        dataset_fs.add_item_raw_bytes(name, img_bytes, ann_json)
                        ds_progress(1)
                except:
                    sly.logger.warning(
                        f"Can not download {total_items_cnt} images from dataset {dataset_info.name}: {repr(e)}. Skipping."
                    )
                    continue
            else:
                ds_progress = sly.tqdm_sly(
                    desc=f"Processing dataset items", total=len(item_names_filtered)
                )
                ann_dir = os.path.join(RESULT_DIR, dataset_info.name, "ann")
                sly.fs.mkdir(ann_dir)
                for image_name, ann_json in zip(item_names_filtered, ann_jsons_filtered):
                    sly.json.dump_json_file(ann_json, os.path.join(ann_dir, image_name + ".json"))
                    ds_progress(1)

            if total_items_cnt == not_labeled_items_cnt:
                sly.logger.warning(
                    "There are no labeled items in dataset {}".format(dataset_info.name)
                )
            else:
                sly.logger.info(
                    f"Dataset {dataset_info.name} has {total_items_cnt-not_labeled_items_cnt}/{total_items_cnt} items labeled"
                )

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
                video_ann_progress = sly.tqdm_sly(
                    desc="Downloading video annotations", total=total_items_cnt
                )

                coro = api.video.annotation.download_bulk_async(
                    video_ids, progress_cb=video_ann_progress
                )
                loop = sly.utils.get_or_create_event_loop()
                if loop.is_running():
                    future = asyncio.run_coroutine_threadsafe(coro, loop)
                    ann_jsons = future.result()
                else:
                    ann_jsons = loop.run_until_complete(coro)
            except Exception as e:
                sly.logger.warning(
                    f"Can not download {len(video_ids)} annotations: {repr(e)}. Skipping."
                )
                continue

            (
                ann_jsons_filtered,
                video_ids_filtered,
                video_names_filtered,
                not_labeled_items_cnt,
                video_anns,
            ) = filter_unlabeled_items(
                "video",
                meta,
                ann_jsons,
                video_ids,
                video_names,
                not_labeled_items_cnt,
                key_id_map,
            )
            video_paths = [dataset_fs.generate_item_path(name) for name in video_names_filtered]

            if DOWNLOAD_ITEMS:
                progress = sly.tqdm_sly(desc="Downloading videos", total=len(video_ids_filtered))
                try:
                    coro = api.video.download_paths_async(
                        video_ids_filtered, video_paths, progress_cb=progress
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
            ds_progress = sly.tqdm_sly(desc=f"Processing dataset items", total=len(video_anns))
            for video_name, video_path, video_ann in zip(
                video_names_filtered, video_paths, video_anns
            ):
                dataset_fs.add_item_file(
                    video_name, video_path, ann=video_ann, _validate_item=False
                )
                ds_progress(1)
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
            dataset_fs: PointcloudDataset = project_fs.create_dataset(dataset_info.name)
            pointclouds = api.pointcloud.get_list(dataset_info.id)
            not_labeled_items_cnt = 0
            total_items_cnt = len(pointclouds)

            pointcloud_ids = [pointcloud_info.id for pointcloud_info in pointclouds]
            pointcloud_names = [pointcloud_info.name for pointcloud_info in pointclouds]

            anns_json = []
            try:
                ann_progress = sly.tqdm_sly(desc="Downloading annotations", total=total_items_cnt)
                coro = api.pointcloud.annotation.download_bulk_async(
                    pointcloud_ids, progress_cb=ann_progress
                )
                loop = sly.utils.get_or_create_event_loop()
                if loop.is_running():
                    future = asyncio.run_coroutine_threadsafe(coro, loop)
                    anns_json.extend(future.result())
                else:
                    anns_json.extend(loop.run_until_complete(coro))
            except Exception as e:
                sly.logger.warning(
                    f"Can not download {total_items_cnt} annotations from dataset {dataset_info.name}: {repr(e)}. Skipping."
                )
                continue

            (
                _,
                pcd_ids_filtered,
                pcd_names_filtered,
                not_labeled_items_cnt,
                ann_objects,
            ) = filter_unlabeled_items(
                "pointcloud",
                meta,
                anns_json,
                pointcloud_ids,
                pointcloud_names,
                not_labeled_items_cnt,
                key_id_map,
            )

            pcd_file_paths = [dataset_fs.generate_item_path(name) for name in pcd_names_filtered]
            if DOWNLOAD_ITEMS:
                pcd_progress = sly.tqdm_sly(
                    desc="Downloading point clouds", total=len(pcd_ids_filtered)
                )
                try:
                    coro = api.pointcloud.download_paths_async(
                        pcd_ids_filtered, pcd_file_paths, progress_cb=pcd_progress
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
                dri_progress = sly.tqdm_sly(
                    desc="Dumping related images infos", total=len(pcd_ids_filtered)
                )
                for pcd_id, pcd_name in zip(pcd_ids_filtered, pcd_names_filtered):
                    rimage_path = dataset_fs.get_related_images_path(pcd_name)
                    # only one related image for each pointcloud
                    rimage_info = api.pointcloud.get_list_related_images(pcd_id)[0]
                    name = rimage_info[ApiField.NAME]
                    rimage_ids.append(rimage_info[ApiField.ID])
                    rimage_paths.append(os.path.join(rimage_path, name))
                    path_json = os.path.join(rimage_path, name + ".json")
                    sly.fs.mkdir(rimage_path)
                    dump_json_file(rimage_info, path_json)
                    dri_progress(1)
                try:
                    ri_progress = sly.tqdm_sly(
                        desc="Downloading related images", total=len(rimage_ids)
                    )
                    coro = api.pointcloud.download_related_images_async(
                        rimage_ids, rimage_paths, progress_cb=ri_progress
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

            ds_progress = sly.tqdm_sly(
                desc=f"Processing dataset items", total=len(pcd_names_filtered)
            )
            for pcd_path, pointcloud_name, pc_ann in zip(
                pcd_file_paths, pcd_names_filtered, ann_objects
            ):
                dataset_fs.add_item_file(
                    pointcloud_name,
                    pcd_path,
                    ann=pc_ann,
                    _validate_item=False,
                )
                ds_progress(1)
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

    def _print_progress(monitor, upload_progress: List):
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
