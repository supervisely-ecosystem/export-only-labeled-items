"""A failed download fails the task; no partial archive is reported as a success."""
import pytest
from builders import META_JSON, ds, image_ann, item, pcd_ann, rimage, video_ann

PROJECTS = {
    "images": lambda: [ds(10, "ds1", [item(1, "a.png", image_ann("label"))])],
    "videos": lambda: [ds(10, "ds1", [item(1, "a.mp4", video_ann("figure"))])],
    "point_clouds": lambda: [
        ds(10, "ds1", [item(1, "a.pcd", pcd_ann("figure"), rimages=[rimage(101, "a.png")])])
    ],
}
CASES = [(p, op) for p in PROJECTS for op in ("ann", "items")] + [
    ("point_clouds", "rimages_download")
]


@pytest.mark.parametrize("ptype,op", CASES)
def test_download_failure_fails_the_task(run_app, ptype, op):
    # the error names the dataset and keeps the original cause; set_download is never reached
    with pytest.raises(RuntimeError, match="ds1") as exc:
        run_app(ptype, PROJECTS[ptype](), META_JSON, fail={op})
    assert "simulated failure" in repr(exc.value.__cause__)
