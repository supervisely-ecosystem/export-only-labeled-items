# export-only-labeled-items: fixes on `ecosystem-fixes-2026-10`

Lifecycle `maintained`, so this branch has targeted fixes only. The code layout (`src/main.py`,
`main_script`, module-level env reads) is unchanged.

## Behaviour contract

- **Started from** the "Download as" menu of an images, videos or point cloud project
  (`context_menu.target`: `images_project`, `videos_project`, `point_cloud_project`). Headless.
- **Option**: `modal.state.items` is `"True"` (download items and annotations, the default; also used when
  the key is missing) or `"False"` (annotations only). It accepts the values `distutils.strtobool` accepted.
- **An item is labeled** when the SDK annotation is not `is_empty()`:
  - images: at least one label or one image tag;
  - videos and point clouds: at least one object or one tag. This includes frame-range tags and
    objects that have no figures.
- **Output**: a tar in Supervisely format, uploaded through `sly.output.set_download` to
  `/tmp/supervisely/export/Export only labeled items/<task_id>/export_only_labeled_items.tar`, with the
  download link set on the task. The tar contains `<project name>/meta.json`, plus `key_id_map.json` for
  videos and point clouds. Each dataset is at its nested path (`parent/datasets/child/`) and has `img|video|pointcloud/`,
  `ann/` and, for point clouds, `related_images/<cloud>_<ext>/<image>` with `<image>.json`. In
  annotations-only mode the item folders are empty. Datasets without labeled items are kept as empty
  folders. When the result is ≥10 GB (Community) or ≥100 GB, it is uploaded as split `.tar.gz` parts to
  `/tmp/supervisely/export/export-only-labeled-items/<timestamp>/` (that code is unchanged).
- **Platform side effects**: workflow lineage (the project as input, the archive as output). Nothing in
  the project is modified.

## Fixed (one commit each; offline tests in `tests/`, fake `sly.Api`)

| # | Commit | Problem | Tests, old code → new code |
|---|---|---|---|
| 0 | 7112b84 | No tests. Added a harness and baseline tests | 4 passed |
| 1 | d21892d | Videos and point clouds: only top-level datasets were exported, and nested ones were skipped without a message. Now uses `api.dataset.tree` | 4 failed / 6 passed → 10 passed |
| 2 | 2d57f2b | Images in annotations-only mode: annotations of nested datasets went to `<project>/<name>/ann`, so datasets with the same name overwrote each other | 1 failed / 10 → 11 passed |
| 3 | fed2921 | Point clouds: `get_list_related_images(id)[0]` raised IndexError on a cloud without photo context, and only the first camera was exported | 3 failed / 11 → 14 passed |
| 4 | d1a8cde | Download errors were logged as warnings and the dataset was skipped (or, for videos, annotations were kept without videos), so a partial archive was reported as a success. The images handler crashed with `NameError: e` | 7 failed / 14 → 21 passed |
| 5 | 4af77b9 | A project with no labeled items produced an empty archive and a successful task | 3 failed / 24 → 27 passed |
| 6 | 75260e0 | Images: the bytes of a whole dataset were held in memory at once (OOM on large datasets). Now batches of 200 | 1 failed / 27 → 28 passed |
| 7 | 914ff79 | `from distutils import util` works on Python 3.14 only through the setuptools shim. Replaced with a local `str_to_bool` | 6 failed / 29 → 35 passed |
| 8 | b199835 | SDK 6.73.577 cannot parse a `multipolygon` class, so any project with one failed at `ProjectMeta.from_json` (dev project 9390). Moved to SDK 6.74.34 | 1 failed / 35 → 36 passed (6.73.577 vs 6.74.34) |

Read-only local runs against dev used `~/supervisely.env` with uploads and workflow calls stubbed. On
SDK 6.74.34:
- project 9390 (every shape type, nested): both datasets exported. All shapes and the tag read back.
- project 1317 (Fruits Nested): 22/22 items at their nested paths.
- project 143 (videos): 2/2 videos.
- project 1751 (point clouds): 12 clouds with their photo context.

## Intentional behaviour changes

1. A download error fails the task with `Can not download N <what> from dataset '<name>'`, chained to the
   original error. Before, the dataset was skipped (or the videos were missing) and the task finished green.
2. A project with no labeled items fails with `Project '<name>' has no labeled items: nothing to export`
   instead of uploading an empty archive.
3. An invalid `modal.state.items` value still raises `ValueError`, now with the key name in the message.
4. `instance_version` 6.16.19 → 6.17.22, because SDK 6.74.34 requires it. Instances older than 6.17.22
   keep the current release v2.2.5.

## Image

`supervisely/base-py-sdk-hardened:6.73.577` → `supervisely/base-py-sdk-hardened:6.74.34`. The tag
exists on Docker Hub and is the same image that export-to-coco uses. `dev_requirements.txt` pins
`supervisely==6.74.34`. The app is still on the bare hardened image because it needs nothing beyond the
SDK, so there is no build job and the release workflows are unchanged. Python 3.14 in both images, and
the tests were run on 3.14.

## Found, not fixed

- **2D figures on photo context images are not exported.** The SDK's `download_pointcloud_project`
  writes `<image>.figures.json` and `upload_pointcloud_project` reads it back. This app only writes the
  image and its `.json`, so labels drawn on camera images are lost. The fix is about 20 lines, mirroring
  `pointcloud_project.py` (`api.image.figure.download` plus object keys from `key_id_map`). It was left
  out to keep this branch targeted.
- **Related image names are not normalized.** The SDK renames `x.png.json`-style names; this app does
  not. Rare.
- **Video and point cloud objects without any figure count as "labeled".** This is the SDK
  `is_empty()` semantics, kept as is.
- **Large-archive branch** (≥10/100 GB): it uploads `.tar.gz` parts with its own code instead of
  `set_download`, and a failure of `set_output_directory` is only logged at debug level. It was not
  changed and has no tests.
- `point_cloud_episodes` projects are not in the context menu. If such a project reached the app,
  it would now fail with "no labeled items" instead of producing an empty archive.

## Dev-check plan (after the coordinator pushes; the branch builds as a dev release)

Each case runs the branch and the published version (`--version v2.2.5`, or the latest release
without `--version`), then compares the outputs.

| Case | Command (branch) | Expected on branch | Published version |
|---|---|---|---|
| Every shape type, nested | `uv run ecosystem-fleet/fleet.py run export-only-labeled-items --workspace 733 --project 9390 --version ecosystem-fixes-2026-10 --branch` | success. The tar has `ccs-synthetic-source/root/{img,ann}/a.png*` and `root/datasets/child/{img,ann}/b.png*` | fails: `Unknown shape: 'multipolygon'` |
| Same, annotations only | same + `--state items=False` | success. Only the `ann/*.json` files, at the nested paths | fails (multipolygon) |
| Nested images (Fruits) | `... --workspace 3 --project 1317 --version ecosystem-fixes-2026-10 --branch` (the project is in workspace 3) | 22 images and 22 annotations under `Temperate/datasets/*` and `Tropical/datasets/*` | same content |
| Nested images, annotations only | same + `--state items=False` | 22 annotation files at the nested paths | annotations in flat `<project>/Apple/ann/…`, outside the tree |
| Videos | `... --workspace 1 --project 143 --version ecosystem-fixes-2026-10 --branch` | 2 videos, `ann/`, `key_id_map.json` | same |
| Point clouds | `... --workspace 1 --project 1751 --version ecosystem-fixes-2026-10 --branch` | 12 clouds, one related image each | same |
| Nested videos | make a copy of 143 with a nested dataset, then run on it | the nested dataset is in the tar at `ds0/datasets/<child>/` | the nested dataset is missing |
| Unlabeled project | any images project without labels or tags | task fails with "has no labeled items: nothing to export" | success with an empty tar |

To check the round trip, import the tar from Team Files with Auto Import (main-import) and compare the
item and label counts with the source project.

## Tests

```
uv venv --python 3.14 .venv && VIRTUAL_ENV=.venv uv pip install supervisely==6.74.34 pytest
.venv/bin/python -m pytest
```
