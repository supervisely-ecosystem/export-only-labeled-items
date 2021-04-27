import os
import supervisely_lib as sly
from supervisely_lib.io.json import dump_json_file
from supervisely_lib.project.project import Project, OpenMode, Progress
from supervisely_lib.video_annotation.key_id_map import KeyIdMap
from supervisely_lib.project.video_project import VideoProject, VideoAnnotation
from supervisely_lib.project.pointcloud_project import PointcloudProject
from supervisely_lib.api.module_api import ApiField
from supervisely_lib.pointcloud_annotation.pointcloud_annotation import PointcloudAnnotation


my_app = sly.AppService()

TEAM_ID = int(os.environ['context.teamId'])
WORKSPACE_ID = int(os.environ['context.workspaceId'])
PROJECT_ID = int(os.environ['modal.state.slyProjectId'])
RESULT_DIR_NAME = 'export'
DOWNLOAD_ITEMS = True
logger = sly.logger


@my_app.callback("export_only_labeled_items")
@sly.timeit
def export_only_labeled_items(api: sly.Api, task_id, context, state, app_logger):

    project = api.project.get_info_by_id(PROJECT_ID)
    project_name = project.name
    meta_json = api.project.get_meta(PROJECT_ID)
    meta = sly.ProjectMeta.from_json(meta_json)

    if len(meta.obj_classes) == 0 and len(meta.tag_metas) == 0:
        logger.warn('Project {} have no labeled items'.format(project_name))
        my_app.stop()

    RESULT_DIR = os.path.join(my_app.data_dir, RESULT_DIR_NAME, project_name)
    RESULT_ARCHIVE_PATH = os.path.join(my_app.data_dir, RESULT_DIR_NAME)
    ARCHIVE_NAME = '{}_{}.tar.gz'.format(PROJECT_ID, project_name)
    RESULT_ARCHIVE = os.path.join(my_app.data_dir, ARCHIVE_NAME)

    sly.fs.mkdir(RESULT_DIR)
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
            for batch in sly.batched(images):
                image_ids = [image_info.id for image_info in batch]
                image_names = [image_info.name for image_info in batch]

                ann_infos = api.annotation.download_batch(dataset_id, image_ids)
                ann_jsons = [ann_info.annotation for ann_info in ann_infos]

                if DOWNLOAD_ITEMS:
                    batch_imgs_bytes = api.image.download_bytes(dataset_id, image_ids)
                    for name, img_bytes, ann_json in zip(image_names, batch_imgs_bytes, ann_jsons):
                        if len(ann_json['objects']) == 0 and len(ann_json['tags']) == 0:
                            not_labeled_items_cnt += 1
                            continue
                        dataset_fs.add_item_raw_bytes(name, img_bytes, ann_json)
                        labeled_items_cnt += 1
                else:
                    ann_dir = os.path.join(RESULT_DIR, dataset_info.name, 'ann')
                    sly.fs.mkdir(ann_dir)
                    for image_name, ann_json in zip(image_names, ann_jsons):
                        if len(ann_json['objects']) == 0 and len(ann_json['tags']) == 0:
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
            for batch in sly.batched(videos):
                video_ids = [video_info.id for video_info in batch]
                video_names = [video_info.name for video_info in batch]
                ann_jsons = api.video.annotation.download_bulk(dataset_info.id, video_ids)
                for video_id, video_name, ann_json in zip(video_ids, video_names, ann_jsons):
                    if len(ann_json['objects']) == 0 and len(ann_json['tags']) == 0 and len(ann_json['frames']) == 0:
                        not_labeled_items_cnt += 1
                        continue
                    video_file_path = dataset_fs.generate_item_path(video_name)
                    labeled_items_cnt += 1
                    if DOWNLOAD_ITEMS:
                        api.video.download_path(video_id, video_file_path)
                    dataset_fs.add_item_file(video_name, video_file_path, ann=VideoAnnotation.from_json(ann_json, project_fs.meta, key_id_map), _validate_item=False)

                ds_progress.iters_done_report(len(batch))
            logger.info(
                'In dataset {} {} items labeled, {} items not labeled'.format(dataset_info.name, labeled_items_cnt,
                                                                              not_labeled_items_cnt))
            if len(videos) == not_labeled_items_cnt:
                logger.warn('There are no labeled items in dataset {}'.format(dataset_info.name))

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
            for batch in sly.batched(pointclouds):
                pointcloud_ids = [pointcloud_info.id for pointcloud_info in batch]
                pointcloud_names = [pointcloud_info.name for pointcloud_info in batch]

                ann_jsons = api.pointcloud.annotation.download_bulk(dataset_info.id, pointcloud_ids)

                for pointcloud_id, pointcloud_name, ann_json in zip(pointcloud_ids, pointcloud_names, ann_jsons):
                    if len(ann_json['objects']) == 0 and len(ann_json['tags']) == 0 and len(ann_json['figures']) == 0:
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

                    dataset_fs.add_item_file(pointcloud_name, pointcloud_file_path, ann=PointcloudAnnotation.from_json(ann_json, project_fs.meta, key_id_map), _validate_item=False)

                ds_progress.iters_done_report(len(batch))
            logger.info(
                'In dataset {} {} items labeled, {} items not labeled'.format(dataset_info.name, labeled_items_cnt,
                                                                              not_labeled_items_cnt))
            if len(pointclouds) == not_labeled_items_cnt:
                logger.warn('There are no labeled items in dataset {}'.format(dataset_info.name))


    sly.fs.archive_directory(RESULT_ARCHIVE_PATH, RESULT_ARCHIVE)
    app_logger.info("Result directory is archived")

    upload_progress = []
    remote_archive_path = "/{}/{}".format(RESULT_DIR_NAME, ARCHIVE_NAME)

    def _print_progress(monitor, upload_progress):
        if len(upload_progress) == 0:
            upload_progress.append(sly.Progress(message="Upload {!r}".format(ARCHIVE_NAME),
                                                total_cnt=monitor.len,
                                                ext_logger=app_logger,
                                                is_size=True))
        upload_progress[0].set_current_value(monitor.bytes_read)

    file_info = api.file.upload(TEAM_ID, RESULT_ARCHIVE, remote_archive_path, lambda m: _print_progress(m, upload_progress))
    app_logger.info("Uploaded to Team-Files: {!r}".format(file_info.full_storage_url))
    api.task.set_output_archive(task_id, file_info.id, ARCHIVE_NAME, file_url=file_info.full_storage_url)

    my_app.stop()


def main():
    sly.logger.info("Script arguments", extra={
        "TEAM_ID": TEAM_ID,
        "WORKSPACE_ID": WORKSPACE_ID,
        "modal.state.slyProjectId": PROJECT_ID
    })

    # Run application service
    my_app.run(initial_events=[{"command": "export_only_labeled_items"}])


if __name__ == '__main__':
        sly.main_wrapper("main", main)

