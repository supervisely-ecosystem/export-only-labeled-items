"""Behaviour that already works on the published version: kept as a regression net."""
from builders import META_JSON, ds, image_ann, item, pcd_ann, rimage, video_ann


def test_images_only_labeled_items_are_exported(run_app):
    datasets = [
        ds(10, "ds1", [
            item(1, "a.png", image_ann("label")),
            item(2, "b.png", image_ann("tag")),
            item(3, "c.png", image_ann("empty")),
        ]),
    ]
    res = run_app("images", datasets, META_JSON)
    assert "My project/meta.json" in res.files
    assert [f for f in res.files if f.startswith("My project/ds1/img/") and not f.endswith("/")] == [
        "My project/ds1/img/a.png",
        "My project/ds1/img/b.png",
    ]
    assert [f for f in res.files if f.startswith("My project/ds1/ann/") and not f.endswith("/")] == [
        "My project/ds1/ann/a.png.json",
        "My project/ds1/ann/b.png.json",
    ]


def test_images_annotations_only_flat(run_app):
    datasets = [ds(10, "ds1", [item(1, "a.png", image_ann("label")), item(2, "c.png", image_ann("empty"))])]
    res = run_app("images", datasets, META_JSON, items=False)
    assert "My project/ds1/ann/a.png.json" in res.files
    assert not any(f.startswith("My project/ds1/img/") and not f.endswith("/") for f in res.files)
    assert not any("c.png" in f for f in res.files)


def test_videos_flat(run_app):
    datasets = [
        ds(10, "ds1", [
            item(1, "a.mp4", video_ann("figure")),
            item(2, "b.mp4", video_ann("tag")),
            item(3, "c.mp4", video_ann("empty")),
        ]),
    ]
    res = run_app("videos", datasets, META_JSON)
    assert "My project/key_id_map.json" in res.files
    assert "My project/ds1/video/a.mp4" in res.files
    assert "My project/ds1/video/b.mp4" in res.files
    assert "My project/ds1/ann/a.mp4.json" in res.files
    assert not any("c.mp4" in f for f in res.files)


def test_pointclouds_flat_with_one_related_image(run_app):
    datasets = [
        ds(10, "ds1", [
            item(1, "a.pcd", pcd_ann("figure"), rimages=[rimage(101, "a.png")]),
            item(2, "b.pcd", pcd_ann("empty"), rimages=[rimage(102, "b.png")]),
        ]),
    ]
    res = run_app("point_clouds", datasets, META_JSON)
    assert "My project/ds1/pointcloud/a.pcd" in res.files
    assert "My project/ds1/ann/a.pcd.json" in res.files
    assert "My project/ds1/related_images/a_pcd/a.png" in res.files
    assert "My project/ds1/related_images/a_pcd/a.png.json" in res.files
    assert not any("b.pcd" in f or "b.png" in f for f in res.files)
