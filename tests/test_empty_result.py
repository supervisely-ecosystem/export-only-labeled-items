"""A project without labeled items is an error, not an empty archive."""
import pytest
from builders import META_JSON, ds, image_ann, item, pcd_ann, video_ann

PROJECTS = {
    "images": lambda kind: [
        ds(10, "ds1", [item(1, "a.png", image_ann(kind))]),
        ds(11, "empty", []),
    ],
    "videos": lambda kind: [ds(10, "ds1", [item(1, "a.mp4", video_ann(kind))])],
    "point_clouds": lambda kind: [ds(10, "ds1", [item(1, "a.pcd", pcd_ann(kind))])],
}
LABELED = {"images": "label", "videos": "figure", "point_clouds": "figure"}


@pytest.mark.parametrize("ptype", list(PROJECTS))
def test_no_labeled_items_fails(run_app, ptype):
    with pytest.raises(RuntimeError, match="no labeled items"):
        run_app(ptype, PROJECTS[ptype]("empty"), META_JSON)


@pytest.mark.parametrize("ptype", list(PROJECTS))
def test_empty_dataset_next_to_labeled_one_is_fine(run_app, ptype):
    datasets = PROJECTS[ptype](LABELED[ptype]) + [ds(99, "nothing", [])]
    res = run_app(ptype, datasets, META_JSON)
    assert any(f.startswith("My project/ds1/ann/") and f.endswith(".json") for f in res.files)
