"""Offline test harness: runs src/main.py against a small in-memory fake of sly.Api.

No Supervisely server is contacted. Run from the repo root:
    python -m pytest tests
with the SDK version from dev_requirements.txt installed (plus pytest).

A fake project is a list of datasets: {"id", "name", "parent_id", "items": [...]}, where each item is
{"id", "name", "ann": <annotation json>} and, for point clouds, optionally "rimages": [related image infos].
"""
import importlib
import os
import sys
import tarfile
from types import SimpleNamespace

import cv2
import numpy as np
import pytest

SRC_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src")

PNG_BYTES = cv2.imencode(".png", np.zeros((4, 6, 3), dtype=np.uint8))[1].tobytes()


class Calls(list):
    """Records (method, args) of interesting API calls."""


class FakeApi:
    def __init__(self, project_type, datasets, meta_json, fail=None):
        self.project_type = project_type
        self.datasets = datasets
        self.meta_json = meta_json
        self.fail = set(fail or ())  # names of operations that raise
        self.calls = Calls()
        self.items = {it["id"]: it for ds in datasets for it in ds["items"]}
        self.rimages = {
            r["id"]: r for ds in datasets for it in ds["items"] for r in it.get("rimages", [])
        }
        self.output_files = []

        self.project = SimpleNamespace(
            get_info_by_id=lambda id: SimpleNamespace(id=id, name="My project", type=project_type),
            get_meta=lambda id: meta_json,
        )
        self.dataset = SimpleNamespace(get_list=self._ds_get_list, tree=self._ds_tree)
        self.annotation = SimpleNamespace(download_bulk_async=self._img_ann_bulk)
        self.image = SimpleNamespace(
            get_list=self._items_of, download_bytes_many_async=self._img_bytes_many
        )
        self.video = SimpleNamespace(
            get_list=self._items_of,
            annotation=SimpleNamespace(download_bulk_async=self._entity_ann_bulk),
            download_paths_async=self._download_paths,
        )
        self.pointcloud = SimpleNamespace(
            get_list=self._items_of,
            annotation=SimpleNamespace(download_bulk_async=self._entity_ann_bulk),
            download_paths_async=self._download_paths,
            get_list_related_images=self._related_images,
            download_related_images_async=self._download_rimages,
        )
        self.app = SimpleNamespace(
            workflow=SimpleNamespace(
                add_input_project=lambda *a, **k: None, add_output_file=lambda *a, **k: None
            )
        )
        self.file = SimpleNamespace(get_info_by_id=lambda id: None)

    def _check(self, op):
        if op in self.fail:
            raise RuntimeError(f"simulated failure of {op}")

    # datasets
    def _ds_get_list(self, project_id, recursive=False, **kwargs):
        dss = self.datasets if recursive else [d for d in self.datasets if d["parent_id"] is None]
        return [self._ds_info(d) for d in dss]

    def _ds_info(self, d):
        return SimpleNamespace(id=d["id"], name=d["name"], parent_id=d["parent_id"])

    def _ds_tree(self, project_id, dataset_id=None):
        by_parent = {}
        for d in self.datasets:
            by_parent.setdefault(d["parent_id"], []).append(d)

        def walk(parent_id, parents):
            for d in by_parent.get(parent_id, []):
                yield list(parents), self._ds_info(d)
                yield from walk(d["id"], parents + [d["name"]])

        yield from walk(None, [])

    def _items_of(self, dataset_id, **kwargs):
        ds = next(d for d in self.datasets if d["id"] == dataset_id)
        return [SimpleNamespace(id=it["id"], name=it["name"]) for it in ds["items"]]

    # annotations
    async def _img_ann_bulk(self, dataset_id, ids, progress_cb=None, **kwargs):
        self._check("ann")
        self.calls.append(("ann", list(ids)))
        return [SimpleNamespace(annotation=self.items[i]["ann"]) for i in ids]

    async def _entity_ann_bulk(self, ids, progress_cb=None, **kwargs):
        self._check("ann")
        self.calls.append(("ann", list(ids)))
        id_key = "videoId" if self.project_type == "videos" else "pointCloudId"
        return [{**self.items[i]["ann"], id_key: i} for i in ids]

    # item data
    async def _img_bytes_many(self, ids, progress_cb=None, **kwargs):
        self._check("items")
        self.calls.append(("items", list(ids)))
        return [PNG_BYTES for _ in ids]

    async def _download_paths(self, ids, paths, progress_cb=None, **kwargs):
        self._check("items")
        self.calls.append(("items", list(ids)))
        for p in paths:
            os.makedirs(os.path.dirname(p), exist_ok=True)
            with open(p, "wb") as f:
                f.write(b"data")

    def _related_images(self, pcd_id):
        self._check("rimages")
        return [dict(r) for r in self.items[pcd_id].get("rimages", [])]

    async def _download_rimages(self, ids, paths, progress_cb=None, **kwargs):
        self._check("rimages_download")
        self.calls.append(("rimages", list(ids)))
        for p in paths:
            os.makedirs(os.path.dirname(p), exist_ok=True)
            with open(p, "wb") as f:
                f.write(PNG_BYTES)


class Result(SimpleNamespace):
    """files: sorted relative paths inside the produced archive (directories end with '/')."""


@pytest.fixture
def run_app(tmp_path, monkeypatch):
    """Import src/main.py with the given modal state and run it on a fake project."""
    import supervisely as sly

    def _run(project_type, datasets, meta_json, items=True, fail=None, module_attrs=None):
        workdir = tmp_path / "work"
        workdir.mkdir(exist_ok=True)
        monkeypatch.chdir(workdir)
        monkeypatch.setattr(sly, "is_development", lambda: False)
        monkeypatch.setenv("SERVER_ADDRESS", "https://app.supervisely.com")
        monkeypatch.setenv("TEAM_ID", "1")
        monkeypatch.setenv("WORKSPACE_ID", "2")
        monkeypatch.setenv("PROJECT_ID", "3")
        monkeypatch.delenv("TASK_ID", raising=False)
        if items is None:
            monkeypatch.delenv("modal.state.items", raising=False)
        else:
            monkeypatch.setenv("modal.state.items", str(items))

        result = Result(archive=None, files=None)

        def fake_set_download(local_path):
            archive = f"{local_path}.tar"
            sly.fs.archive_directory(local_path, archive)
            with tarfile.open(archive) as tar:
                # archive_directory roots the tar at the directory's content
                result.files = sorted(
                    m.name + ("/" if m.isdir() else "") for m in tar.getmembers() if m.name
                )
            result.archive = archive
            return SimpleNamespace(id=1, name=os.path.basename(archive), team_id=1)

        monkeypatch.setattr(sly.output, "set_download", fake_set_download)
        monkeypatch.syspath_prepend(SRC_DIR)
        for mod in ("main", "workflow"):
            sys.modules.pop(mod, None)
        main = importlib.import_module("main")
        for name, value in (module_attrs or {}).items():
            monkeypatch.setattr(main, name, value)

        api = FakeApi(project_type, datasets, meta_json, fail=fail)
        result.api = api
        main.export_only_labeled_items(api)
        return result

    return _run
