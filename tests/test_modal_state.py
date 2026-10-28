"""modal.state.items parsing, without distutils (removed from the stdlib in Python 3.12)."""
import sys

import pytest
from builders import META_JSON, ds, image_ann, item


@pytest.mark.parametrize(
    "value,expect_images",
    [("True", True), ("true", True), ("1", True), ("False", False), ("false", False), (None, True)],
)
def test_items_option_without_distutils(run_app, monkeypatch, value, expect_images):
    # the hardened image only has distutils through setuptools' shim; do not depend on it
    monkeypatch.setitem(sys.modules, "distutils", None)
    datasets = [ds(10, "ds1", [item(1, "a.png", image_ann("label"))])]
    res = run_app("images", datasets, META_JSON, items=value)
    assert ("My project/ds1/img/a.png" in res.files) is expect_images
    assert "My project/ds1/ann/a.png.json" in res.files


def test_invalid_items_option_fails(run_app):
    datasets = [ds(10, "ds1", [item(1, "a.png", image_ann("label"))])]
    with pytest.raises(ValueError):
        run_app("images", datasets, META_JSON, items="maybe")
