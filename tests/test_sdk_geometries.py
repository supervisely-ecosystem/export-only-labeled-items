"""Projects whose meta has classes of newer geometry types (SDK >= 6.74.10)."""
import copy

from builders import META_JSON, ds, image_ann, item


def test_project_with_multipolygon_class(run_app):
    meta = copy.deepcopy(META_JSON)
    meta["classes"].append(
        {"title": "region", "shape": "multipolygon", "color": "#FF0000", "geometry_config": {}}
    )
    datasets = [ds(10, "ds1", [item(1, "a.png", image_ann("label"))])]
    res = run_app("images", datasets, meta)
    assert "My project/ds1/img/a.png" in res.files
