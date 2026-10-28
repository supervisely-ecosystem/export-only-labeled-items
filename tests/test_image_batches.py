"""Images are downloaded and written in batches, not a whole dataset in memory."""
from builders import META_JSON, ds, image_ann, item


def test_images_are_downloaded_in_batches(run_app):
    items = [item(i, f"{i}.png", image_ann("label" if i != 4 else "empty")) for i in range(1, 9)]
    res = run_app("images", [ds(10, "ds1", items)], META_JSON, module_attrs={"IMAGE_BATCH_SIZE": 3})
    batches = [ids for op, ids in res.api.calls if op == "items"]
    assert batches == [[1, 2, 3], [5, 6, 7], [8]]
    imgs = sorted(f for f in res.files if f.startswith("My project/ds1/img/") and f.endswith(".png"))
    assert len(imgs) == 7
    assert "My project/ds1/img/4.png" not in imgs
