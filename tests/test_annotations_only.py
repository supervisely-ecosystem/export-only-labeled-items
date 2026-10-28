"""'Download only annotations' mode keeps every dataset at its own path."""
from builders import META_JSON, ds, image_ann, item


def test_images_annotations_only_nested(run_app):
    datasets = [
        ds(10, "a", [item(1, "x.png", image_ann("label"))]),
        ds(11, "same", [item(2, "y.png", image_ann("label"))], parent_id=10),
        ds(20, "b", [item(3, "z.png", image_ann("label"))]),
        ds(21, "same", [item(4, "y.png", image_ann("tag"))], parent_id=20),
    ]
    res = run_app("images", datasets, META_JSON, items=False)
    anns = [f for f in res.files if f.endswith(".json") and "/ann/" in f]
    assert anns == [
        "My project/a/ann/x.png.json",
        "My project/a/datasets/same/ann/y.png.json",
        "My project/b/ann/z.png.json",
        "My project/b/datasets/same/ann/y.png.json",
    ]
    # nothing is written outside the dataset tree
    assert not any(f.startswith("My project/same/") for f in res.files)
