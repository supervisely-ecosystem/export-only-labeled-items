"""Point cloud photo context: zero, one or many related images per cloud."""
from builders import META_JSON, ds, item, pcd_ann, rimage


def test_pointcloud_without_related_images(run_app):
    datasets = [ds(10, "ds1", [item(1, "a.pcd", pcd_ann("figure"))])]
    res = run_app("point_clouds", datasets, META_JSON)
    assert "My project/ds1/pointcloud/a.pcd" in res.files
    assert "My project/ds1/ann/a.pcd.json" in res.files
    assert not any("/related_images/" in f and not f.endswith("/") for f in res.files)


def test_pointcloud_with_several_related_images(run_app):
    rimgs = [rimage(101, "front.png"), rimage(102, "left.jpg"), rimage(103, "right.png")]
    datasets = [
        ds(10, "ds1", [
            item(1, "a.pcd", pcd_ann("figure"), rimages=rimgs),
            item(2, "b.pcd", pcd_ann("figure")),
        ])
    ]
    res = run_app("point_clouds", datasets, META_JSON)
    rel = sorted(f for f in res.files if "/related_images/" in f and not f.endswith("/"))
    assert rel == [
        "My project/ds1/related_images/a_pcd/front.png",
        "My project/ds1/related_images/a_pcd/front.png.json",
        "My project/ds1/related_images/a_pcd/left.jpg",
        "My project/ds1/related_images/a_pcd/left.jpg.json",
        "My project/ds1/related_images/a_pcd/right.png",
        "My project/ds1/related_images/a_pcd/right.png.json",
    ]
    assert "My project/ds1/pointcloud/b.pcd" in res.files


def test_pointcloud_related_images_read_back(run_app, tmp_path):
    import tarfile

    import supervisely as sly

    rimgs = [rimage(101, "front.png"), rimage(102, "left.jpg")]
    datasets = [ds(10, "ds1", [item(1, "a.pcd", pcd_ann("figure"), rimages=rimgs)])]
    res = run_app("point_clouds", datasets, META_JSON)
    out = tmp_path / "x"
    with tarfile.open(res.archive) as tar:
        tar.extractall(out, filter="data")
    project = sly.PointcloudProject(str(out / "My project"), sly.OpenMode.READ)
    dataset = project.datasets.get("ds1")
    assert len(dataset.get_related_images("a.pcd")) == 2
