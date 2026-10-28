"""Nested datasets: every dataset of the tree is exported at its nested path."""
import pytest
from builders import META_JSON, ds, image_ann, item, pcd_ann, rimage, video_ann

ANN = {"images": image_ann, "videos": video_ann, "point_clouds": pcd_ann}
LABELED = {"images": "label", "videos": "figure", "point_clouds": "figure"}
EXT = {"images": "png", "videos": "mp4", "point_clouds": "pcd"}
ITEM_DIR = {"images": "img", "videos": "video", "point_clouds": "pointcloud"}


def nested_project(ptype):
    ann, ext = ANN[ptype], EXT[ptype]

    def it(id, name):
        extra = {"rimages": [rimage(100 + id, f"{name}.png")]} if ptype == "point_clouds" else {}
        return item(id, f"{name}.{ext}", ann(LABELED[ptype]), **extra)

    return [
        ds(10, "parent", [it(1, "top")]),
        ds(11, "child", [it(2, "inner")], parent_id=10),
        ds(12, "grandchild", [it(3, "deep")], parent_id=11),
    ]


@pytest.mark.parametrize("ptype", ["images", "videos", "point_clouds"])
def test_nested_datasets_are_exported(run_app, ptype):
    res = run_app(ptype, nested_project(ptype), META_JSON)
    ext, d = EXT[ptype], ITEM_DIR[ptype]
    for path in (
        f"My project/parent/{d}/top.{ext}",
        f"My project/parent/datasets/child/{d}/inner.{ext}",
        f"My project/parent/datasets/child/datasets/grandchild/{d}/deep.{ext}",
        f"My project/parent/datasets/child/ann/inner.{ext}.json",
        f"My project/parent/datasets/child/datasets/grandchild/ann/deep.{ext}.json",
    ):
        assert path in res.files, path


@pytest.mark.parametrize("ptype", ["images", "videos", "point_clouds"])
def test_archive_reads_back_as_supervisely_project(run_app, ptype, tmp_path):
    """The layout is what the SDK project readers (used by Supervisely import) understand."""
    import tarfile

    import supervisely as sly

    res = run_app(ptype, nested_project(ptype), META_JSON)
    out = tmp_path / "extracted"
    with tarfile.open(res.archive) as tar:
        tar.extractall(out, filter="data")
    cls = {
        "images": sly.Project,
        "videos": sly.VideoProject,
        "point_clouds": sly.PointcloudProject,
    }[ptype]
    project = cls(str(out / "My project"), sly.OpenMode.READ)
    by_path = {d.name: d for d in project.datasets}
    assert sorted(by_path) == ["parent", "parent/child", "parent/child/grandchild"]
    assert sum(len(d) for d in project.datasets) == 3
    for d in project.datasets:
        for name in d:
            d.get_ann(name, project.meta) if ptype == "images" else d.get_ann(
                name, project.meta, sly.KeyIdMap()
            )
