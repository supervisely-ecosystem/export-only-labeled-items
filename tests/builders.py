"""Tiny in-memory Supervisely objects for the fake project."""
import supervisely as sly
from supervisely.geometry.cuboid_3d import Cuboid3d, Vector3d
from supervisely.pointcloud_annotation.pointcloud_figure import PointcloudFigure
from supervisely.pointcloud_annotation.pointcloud_object_collection import (
    PointcloudObjectCollection,
)
from supervisely.video_annotation.key_id_map import KeyIdMap

BOX = sly.ObjClass("box", sly.Rectangle)
CUBE = sly.ObjClass("cube", Cuboid3d)
FLAG = sly.TagMeta("flag", sly.TagValueType.NONE)
META = sly.ProjectMeta(obj_classes=[BOX, CUBE], tag_metas=[FLAG])
META_JSON = META.to_json()


_next_id = [1000]


def _with_ids(ann_json):
    """The server gives every object, figure and tag an id; KeyIdMap needs them unique."""

    def new_id():
        _next_id[0] += 1
        return _next_id[0]

    for obj in ann_json.get("objects", []):
        obj["id"] = new_id()
    for fig in ann_json.get("figures", []):
        fig["id"] = new_id()
    for frame in ann_json.get("frames", []):
        for fig in frame.get("figures", []):
            fig["id"] = new_id()
    for tag in ann_json.get("tags", []):
        tag["id"] = new_id()
    return ann_json


# images
def image_ann(kind):
    """kind: 'empty' | 'label' | 'tag'."""
    ann = sly.Annotation((4, 6))
    if kind == "label":
        ann = ann.add_label(sly.Label(sly.Rectangle(0, 0, 2, 2), BOX))
    elif kind == "tag":
        ann = ann.add_tag(sly.Tag(FLAG))
    return ann.to_json()


# videos
def video_ann(kind):
    """kind: 'empty' | 'figure' | 'tag'."""
    objects, frames, tags = [], [], []
    if kind == "figure":
        obj = sly.VideoObject(BOX)
        objects = [obj]
        frames = [sly.Frame(0, [sly.VideoFigure(obj, sly.Rectangle(0, 0, 2, 2), 0)])]
    elif kind == "tag":
        tags = [sly.VideoTag(FLAG, frame_range=[0, 1])]
    ann = sly.VideoAnnotation(
        (4, 6),
        3,
        objects=sly.VideoObjectCollection(objects),
        frames=sly.FrameCollection(frames),
        tags=sly.VideoTagCollection(tags),
    )
    return _with_ids(ann.to_json(KeyIdMap()))


# point clouds
def pcd_ann(kind):
    """kind: 'empty' | 'figure'."""
    objects, figures = [], []
    if kind == "figure":
        obj = sly.PointcloudObject(CUBE)
        objects = [obj]
        cube = Cuboid3d(Vector3d(0, 0, 0), Vector3d(0, 0, 0), Vector3d(1, 1, 1))
        figures = [PointcloudFigure(obj, cube)]
    ann = sly.PointcloudAnnotation(
        objects=PointcloudObjectCollection(objects), figures=figures
    )
    return _with_ids(ann.to_json(KeyIdMap()))


def rimage(id, name):
    return {"id": id, "name": name, "entityId": 0, "meta": {"deviceId": "cam"}}


def ds(id, name, items, parent_id=None):
    return {"id": id, "name": name, "parent_id": parent_id, "items": items}


def item(id, name, ann, **extra):
    return {"id": id, "name": name, "ann": ann, **extra}
