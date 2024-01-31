import os
import supervisely as sly
from supervisely.io.json import dump_json_file
from supervisely.project.project import Project, OpenMode, Progress
from supervisely.video_annotation.key_id_map import KeyIdMap
from supervisely.project.video_project import VideoProject
from supervisely.project.pointcloud_project import PointcloudProject
from supervisely.api.module_api import ApiField
from supervisely.app.v1.app_service import AppService
from distutils import util
from dotenv import load_dotenv


if sly.is_development():
    load_dotenv("local.env")
    load_dotenv(os.path.expanduser("~/supervisely.env"))

my_app = AppService()

TEAM_ID = sly.env.team_id()
WORKSPACE_ID = sly.env.workspace_id()
PROJECT_ID = sly.env.project_id()
TASK_ID = sly.env.task_id()
RESULT_DIR_NAME = 'export only labeled items'

# SIZE_LIMIT = 10 if sly.is_community() else 100
# SIZE_LIMIT_BYTES = SIZE_LIMIT * 1024 * 1024 * 1024
# SPLIT_MODE = "MB"
# SPLIT_SIZE = 500 # do not increase this value (memory issues)

SIZE_LIMIT = 10 # ! for tests if sly.is_community() else 100
SIZE_LIMIT_BYTES = SIZE_LIMIT * 1024 * 1024 # ! for tests * 1024
SPLIT_MODE = "KB"
SPLIT_SIZE = 50 # ! for tests 00 # do not increase this value (memory issues)

logger = sly.logger

try:
    os.environ['modal.state.items']
except KeyError:
    logger.warn('The option to download project is not selected, project will be download with items')
    DOWNLOAD_ITEMS = True
else:
    DOWNLOAD_ITEMS = bool(util.strtobool(os.environ['modal.state.items']))


@my_app.callback("export_only_labeled_items")
@sly.timeit
def export_only_labeled_items(api: sly.Api, task_id, context, state, app_logger):

    project = api.project.get_info_by_id(PROJECT_ID)
    if project is None:
        raise RuntimeError(f"Project with the given ID {PROJECT_ID} not found")
    project_name = project.name
    meta_json = api.project.get_meta(PROJECT_ID)
    meta = sly.ProjectMeta.from_json(meta_json)

    if len(meta.obj_classes) == 0 and len(meta.tag_metas) == 0:
        logger.warn('Project {} have no labeled items'.format(project_name))
        my_app.stop()

    RESULT_DIR = os.path.join(my_app.data_dir, RESULT_DIR_NAME, project_name)
    RESULT_PROJECT_DIR = os.path.join(my_app.data_dir, RESULT_DIR_NAME)
    ARCHIVE_NAME = f"{PROJECT_ID}_{project_name}.tar.gz"
    RESULT_ARCHIVE_DIR = os.path.join(my_app.data_dir, f"{TASK_ID}")
    sly.fs.mkdir(RESULT_ARCHIVE_DIR, remove_content_if_exists=True)
    RESULT_ARCHIVE = os.path.join(RESULT_ARCHIVE_DIR, ARCHIVE_NAME)
    remote_path = os.path.join(
        sly.team_files.RECOMMENDED_EXPORT_PATH, "export-only-labeled-items", f"{task_id}"
    )

    sly.fs.mkdir(RESULT_DIR, True)
    app_logger.info("Export folder has been created")

    if project.type == str(sly.ProjectType.IMAGES):
        project_fs = Project(RESULT_DIR, OpenMode.CREATE)
        project_fs.set_meta(meta)
        for dataset_info in api.dataset.get_list(PROJECT_ID):
            dataset_id = dataset_info.id
            dataset_fs = project_fs.create_dataset(dataset_info.name)
            images = api.image.get_list(dataset_id)

            ds_progress = Progress('Downloading dataset: {}'.format(dataset_info.name), total_cnt=len(images))
            labeled_items_cnt = 0
            not_labeled_items_cnt = 0
            for batch in sly.batched(images, batch_size=10):
                image_ids = [image_info.id for image_info in batch]
                image_names = [image_info.name for image_info in batch]

                try:
                    ann_infos = api.annotation.download_batch(dataset_id, image_ids)
                    ann_jsons = [ann_info.annotation for ann_info in ann_infos]
                except Exception as e:
                    logger.warn(f"Can not download {len(image_ids)} annotations from dataset {dataset_info.name}: {repr(e)}. Skip batch.")
                    continue

                if DOWNLOAD_ITEMS:
                    try:
                        batch_imgs_bytes = api.image.download_bytes(dataset_id, image_ids)
                    except Exception as e:
                        logger.warn(f"Can not download {len(image_ids)} images from dataset {dataset_info.name}: {repr(e)}. Skip batch.")
                        continue
                    for name, img_bytes, ann_json in zip(image_names, batch_imgs_bytes, ann_jsons):
                        ann = sly.Annotation.from_json(ann_json, meta)
                        if ann.is_empty():
                            not_labeled_items_cnt += 1
                            continue
                        dataset_fs.add_item_raw_bytes(name, img_bytes, ann_json)
                        labeled_items_cnt += 1
                else:
                    ann_dir = os.path.join(RESULT_DIR, dataset_info.name, 'ann')
                    sly.fs.mkdir(ann_dir)
                    for image_name, ann_json in zip(image_names, ann_jsons):
                        ann = sly.Annotation.from_json(ann_json, meta)
                        if ann.is_empty():
                            not_labeled_items_cnt += 1
                            continue
                        sly.io.json.dump_json_file(ann_json, os.path.join(ann_dir, image_name + '.json'))
                        labeled_items_cnt += 1

                ds_progress.iters_done_report(len(batch))
            logger.info('In dataset {} {} items labeled, {} items not labeled'.format(dataset_info.name, labeled_items_cnt, not_labeled_items_cnt))
            if len(images) == not_labeled_items_cnt:
                logger.warn('There are no labeled items in dataset {}'.format(dataset_info.name))

    elif project.type == str(sly.ProjectType.VIDEOS):
        key_id_map = KeyIdMap()
        project_fs = VideoProject(RESULT_DIR, OpenMode.CREATE)
        project_fs.set_meta(meta)
        for dataset_info in api.dataset.get_list(PROJECT_ID):
            dataset_fs = project_fs.create_dataset(dataset_info.name)
            videos = api.video.get_list(dataset_info.id)
            labeled_items_cnt = 0
            not_labeled_items_cnt = 0
            ds_progress = Progress('Downloading dataset: {}'.format(dataset_info.name), total_cnt=len(videos))
            for batch in sly.batched(videos, batch_size=10):
                video_ids = [video_info.id for video_info in batch]
                video_names = [video_info.name for video_info in batch]
                try:
                    ann_jsons = api.video.annotation.download_bulk(dataset_info.id, video_ids)
                except Exception as e:
                    logger.warn(f"Can not download {len(video_ids)} annotations: {repr(e)}. Skip batch.")
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
                            logger.warn(f"Can not download video {video_name}: {repr(e)}. Skip video.")
                            continue
                    dataset_fs.add_item_file(video_name, video_file_path, ann=video_ann, _validate_item=False)


                ds_progress.iters_done_report(len(batch))
            logger.info(
                'In dataset {} {} items labeled, {} items not labeled'.format(dataset_info.name, labeled_items_cnt,
                                                                              not_labeled_items_cnt))
            if len(videos) == not_labeled_items_cnt:
                logger.warn('There are no labeled items in dataset {}'.format(dataset_info.name))

        project_fs.set_key_id_map(key_id_map)

    elif project.type == str(sly.ProjectType.POINT_CLOUDS):
        key_id_map = KeyIdMap()
        project_fs = PointcloudProject(RESULT_DIR, OpenMode.CREATE)
        project_fs.set_meta(meta)
        for dataset_info in api.dataset.get_list(PROJECT_ID):
            dataset_fs = project_fs.create_dataset(dataset_info.name)
            pointclouds = api.pointcloud.get_list(dataset_info.id)
            labeled_items_cnt = 0
            not_labeled_items_cnt = 0
            ds_progress = Progress('Downloading dataset: {!r}'.format(dataset_info.name), total_cnt=len(pointclouds))
            for batch in sly.batched(pointclouds, batch_size=1):
                pointcloud_ids = [pointcloud_info.id for pointcloud_info in batch]
                pointcloud_names = [pointcloud_info.name for pointcloud_info in batch]

                ann_jsons = api.pointcloud.annotation.download_bulk(dataset_info.id, pointcloud_ids)

                for pointcloud_id, pointcloud_name, ann_json in zip(pointcloud_ids, pointcloud_names, ann_jsons):
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

                    dataset_fs.add_item_file(pointcloud_name, pointcloud_file_path, ann=pc_ann, _validate_item=False)

                ds_progress.iters_done_report(len(batch))
            logger.info(
                'In dataset {} {} items labeled, {} items not labeled'.format(dataset_info.name, labeled_items_cnt,
                                                                              not_labeled_items_cnt))
            if len(pointclouds) == not_labeled_items_cnt:
                logger.warn('There are no labeled items in dataset {}'.format(dataset_info.name))

        project_fs.set_key_id_map(key_id_map)

    dir_size = sly.fs.get_directory_size(RESULT_PROJECT_DIR)
    dir_size_gb = round(dir_size / (1024 * 1024 * 1024), 2)

    split = None
    if dir_size > SIZE_LIMIT_BYTES:
        app_logger.info(f"Result archive size ({dir_size_gb} GB) more than {SIZE_LIMIT} GB")
        split = f"{SPLIT_SIZE}{SPLIT_MODE}"
        app_logger.info(f"It will be uploaded with splitting by {split}") 
    splits = sly.fs.archive_directory(RESULT_PROJECT_DIR, RESULT_ARCHIVE, split=split)
    app_logger.info(f"Result directory is archived {'with splitting' if splits else ''}")

    sly.fs.remove_dir(RESULT_PROJECT_DIR) # remove dir 
    if splits is None:
        remote_path = os.path.join(remote_path, ARCHIVE_NAME)

    upload_progress = []
    upload_msg = f"Uploading{' splitted' if splits else ''} archive {ARCHIVE_NAME} to Team Files"

    def _print_progress(monitor, upload_progress):
        if len(upload_progress) == 0:
            upload_progress.append(
                sly.Progress(
                    message=upload_msg,
                    total_cnt=monitor.len,
                    ext_logger=app_logger,
                    is_size=True,
                )
            )
        upload_progress[0].set_current_value(monitor.bytes_read)

    if splits:
        res_remote_dir = api.file.upload_directory(
            TEAM_ID,
            RESULT_ARCHIVE_DIR,
            remote_path,
            progress_size_cb=lambda m: _print_progress(m, upload_progress),
        )
        main_part_name = os.path.basename(splits[0])
        main_part = os.path.join(res_remote_dir, main_part_name)
        file_info = api.file.get_info_by_path(TEAM_ID, main_part)
        api.task.set_output_directory(
            task_id=task_id,
            file_id=file_info.id,
            directory_path=res_remote_dir
        )
        app_logger.info(f"Uploaded to Team-Files: {res_remote_dir}")
    else:
        file_info = api.file.upload(
            TEAM_ID,
            RESULT_ARCHIVE,
            remote_path,
            lambda m: _print_progress(m, upload_progress),
        )
        api.task.set_output_archive(
            task_id=task_id,
            file_id=file_info.id,
            file_name=ARCHIVE_NAME,
            file_url=file_info.storage_path,
        )
        app_logger.info(f"Uploaded to Team-Files: {file_info.path}")

    my_app.stop()


def main():
    sly.logger.info("Script arguments", extra={
        "TEAM_ID": TEAM_ID,
        "WORKSPACE_ID": WORKSPACE_ID,
        "modal.state.slyProjectId": PROJECT_ID
    })
    my_app.run(initial_events=[{"command": "export_only_labeled_items"}])


if __name__ == '__main__':
    sly.main_wrapper("main", main)

