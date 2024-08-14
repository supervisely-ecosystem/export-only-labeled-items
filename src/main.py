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

if sly.is_development():
    print("go")
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
SIZE_LIMIT_BYTES = SIZE_LIMIT * 1024 * 1024 * 1024
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
        "The option to download project is not selected, project will be download with items"
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

            ds_progress = Progress(
                "Downloading dataset: {}".format(dataset_info.name),
                total_cnt=len(images),
            )
            labeled_items_cnt = 0
            not_labeled_items_cnt = 0
            for batch in sly.batched(images, batch_size=10):
                image_ids = [image_info.id for image_info in batch]
                image_names = [image_info.name for image_info in batch]

                try:
                    ann_infos = api.annotation.download_batch(dataset_id, image_ids)
                    ann_jsons = [ann_info.annotation for ann_info in ann_infos]
                except Exception as e:
                    sly.logger.warn(
                        f"Can not download {len(image_ids)} annotations from dataset {dataset_info.name}: {repr(e)}. Skip batch."
                    )
                    continue

                if DOWNLOAD_ITEMS:
                    try:
                        batch_imgs_bytes = api.image.download_bytes(dataset_id, image_ids)
                    except Exception as e:
                        sly.logger.warn(
                            f"Can not download {len(image_ids)} images from dataset {dataset_info.name}: {repr(e)}. Skip batch."
                        )
                        continue
                    for name, img_bytes, ann_json in zip(image_names, batch_imgs_bytes, ann_jsons):
                        ann = sly.Annotation.from_json(ann_json, meta)
                        if ann.is_empty():
                            not_labeled_items_cnt += 1
                            continue
                        dataset_fs.add_item_raw_bytes(name, img_bytes, ann_json)
                        labeled_items_cnt += 1
                else:
                    ann_dir = os.path.join(RESULT_DIR, dataset_info.name, "ann")
                    sly.fs.mkdir(ann_dir)
                    for image_name, ann_json in zip(image_names, ann_jsons):
                        ann = sly.Annotation.from_json(ann_json, meta)
                        if ann.is_empty():
                            not_labeled_items_cnt += 1
                            continue
                        sly.io.json.dump_json_file(
                            ann_json, os.path.join(ann_dir, image_name + ".json")
                        )
                        labeled_items_cnt += 1

                ds_progress.iters_done_report(len(batch))
            sly.logger.info(
                "In dataset {} {} items labeled, {} items not labeled".format(
                    dataset_info.name, labeled_items_cnt, not_labeled_items_cnt
                )
            )
            if len(images) == not_labeled_items_cnt:
                sly.logger.warn(
                    "There are no labeled items in dataset {}".format(dataset_info.name)
                )

    elif project.type == str(sly.ProjectType.VIDEOS):
        key_id_map = KeyIdMap()
        project_fs = VideoProject(RESULT_DIR, OpenMode.CREATE)
        project_fs.set_meta(meta)
        for dataset_info in api.dataset.get_list(project_id):
            dataset_fs = project_fs.create_dataset(dataset_info.name)
            videos = api.video.get_list(dataset_info.id)
            labeled_items_cnt = 0
            not_labeled_items_cnt = 0
            ds_progress = Progress(
                "Downloading dataset: {}".format(dataset_info.name),
                total_cnt=len(videos),
            )
            for batch in sly.batched(videos, batch_size=10):
                video_ids = [video_info.id for video_info in batch]
                video_names = [video_info.name for video_info in batch]
                try:
                    ann_jsons = api.video.annotation.download_bulk(dataset_info.id, video_ids)
                except Exception as e:
                    sly.logger.warn(
                        f"Can not download {len(video_ids)} annotations: {repr(e)}. Skip batch."
                    )
                    continue
                for video_id, video_name, ann_json in zip(video_ids, video_names, ann_jsons):
                    video_ann = sly.VideoAnnotation.from_json(ann_json, meta, key_id_map)
                    if video_ann.is_empty():
                        not_labeled_items_cnt += 1
                        continue
                    video_file_path = None
                    labeled_items_cnt += 1
                    if DOWNLOAD_ITEMS:
                        try:
                            video_file_path = dataset_fs.generate_item_path(video_name)
                            api.video.download_path(video_id, video_file_path)
                        except Exception as e:
                            sly.logger.warn(
                                f"Can not download video {video_name}: {repr(e)}. Skip video."
                            )
                            continue
                    dataset_fs.add_item_file(
                        video_name, video_file_path, ann=video_ann, _validate_item=False
                    )

                ds_progress.iters_done_report(len(batch))
            sly.logger.info(
                "In dataset {} {} items labeled, {} items not labeled".format(
                    dataset_info.name, labeled_items_cnt, not_labeled_items_cnt
                )
            )
            if len(videos) == not_labeled_items_cnt:
                sly.logger.warn(
                    "There are no labeled items in dataset {}".format(dataset_info.name)
                )

        project_fs.set_key_id_map(key_id_map)

    elif project.type == str(sly.ProjectType.POINT_CLOUDS):
        key_id_map = KeyIdMap()
        project_fs = PointcloudProject(RESULT_DIR, OpenMode.CREATE)
        project_fs.set_meta(meta)
        for dataset_info in api.dataset.get_list(project_id):
            dataset_fs = project_fs.create_dataset(dataset_info.name)
            pointclouds = api.pointcloud.get_list(dataset_info.id)
            labeled_items_cnt = 0
            not_labeled_items_cnt = 0
            ds_progress = Progress(
                "Downloading dataset: {!r}".format(dataset_info.name),
                total_cnt=len(pointclouds),
            )
            for batch in sly.batched(pointclouds, batch_size=1):
                pointcloud_ids = [pointcloud_info.id for pointcloud_info in batch]
                pointcloud_names = [pointcloud_info.name for pointcloud_info in batch]

                ann_jsons = api.pointcloud.annotation.download_bulk(dataset_info.id, pointcloud_ids)

                for pointcloud_id, pointcloud_name, ann_json in zip(
                    pointcloud_ids, pointcloud_names, ann_jsons
                ):
                    pc_ann = sly.PointcloudAnnotation.from_json(ann_json, meta, key_id_map)
                    if pc_ann.is_empty():
                        not_labeled_items_cnt += 1
                        continue
                    pointcloud_file_path = dataset_fs.generate_item_path(pointcloud_name)
                    labeled_items_cnt += 1
                    if DOWNLOAD_ITEMS:
                        api.pointcloud.download_path(pointcloud_id, pointcloud_file_path)
                        related_images_path = dataset_fs.get_related_images_path(pointcloud_name)
                        related_images = api.pointcloud.get_list_related_images(pointcloud_id)
                        for rimage_info in related_images:
                            name = rimage_info[ApiField.NAME]
                            rimage_id = rimage_info[ApiField.ID]
                            path_img = os.path.join(related_images_path, name)
                            path_json = os.path.join(related_images_path, name + ".json")
                            api.pointcloud.download_related_image(rimage_id, path_img)
                            dump_json_file(rimage_info, path_json)

                    dataset_fs.add_item_file(
                        pointcloud_name,
                        pointcloud_file_path,
                        ann=pc_ann,
                        _validate_item=False,
                    )

                ds_progress.iters_done_report(len(batch))
            sly.logger.info(
                "In dataset {} {} items labeled, {} items not labeled".format(
                    dataset_info.name, labeled_items_cnt, not_labeled_items_cnt
                )
            )
            if len(pointclouds) == not_labeled_items_cnt:
                sly.logger.warn(
                    "There are no labeled items in dataset {}".format(dataset_info.name)
                )

        project_fs.set_key_id_map(key_id_map)

    dir_size = sly.fs.get_directory_size(RESULT_PROJECT_DIR)
    dir_size_gb = round(dir_size / (1024 * 1024 * 1024), 2)

    if dir_size < SIZE_LIMIT_BYTES:
        sly.logger.debug(f"Result archive size ({dir_size_gb} GB) less than limit {SIZE_LIMIT} GB")
        sly.output.set_download(RESULT_PROJECT_DIR)
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
