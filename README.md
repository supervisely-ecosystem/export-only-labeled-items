<div align="center" markdown>
<img src="https://user-images.githubusercontent.com/48245050/182848469-81045a80-d01d-4314-b4bb-9996a6c4edf0.png"/>


# Export only labeled items

<p align="center">
  <a href="#Overview">Overview</a> •
  <a href="#How-To-Run">How To Run</a> •
  <a href="#How-To-Use">How To Use</a>
</p>

[![](https://img.shields.io/badge/slack-chat-green.svg?logo=slack)](https://supervise.ly/slack)
![GitHub release (latest SemVer)](https://img.shields.io/github/v/release/supervisely-ecosystem/export-only-labeled-items)
[![views](https://app.supervise.ly/img/badges/views/supervisely-ecosystem/export-only-labeled-items.png)](https://supervise.ly)
[![runs](https://app.supervise.ly/img/badges/runs/supervisely-ecosystem/export-only-labeled-items.png)](https://supervise.ly)

</div>

## Overview

App exports only labeled items from project and prepares downloadable `tar` archive. Annotations will be in [Supervisely format](https://docs.supervise.ly/data-organization/00_ann_format_navi). App works with all types of projects: `Images`, `Videos` , `Point Clouds`. Unlabeled items will be skipped. Also there is the additional option to export only annotations without actual data.

🏋️ Starting from version v1.2.13 application supports split archives. If the archive file size is too big, it will be split into several parts. Learn more below in the "How to extract split archives" section.


## How To Run 
**Step 1**: Add app to your team from [Ecosystem](https://app.supervise.ly/apps/ecosystem/export-only-labeled-items) if it is not there.

**Step 2**: Open context menu of project -> `Download via App` -> `Export only labeled items` 

<img src="https://i.imgur.com/cFSJIpi.png"/>


**Step 3**: Select project export mode.

<img src="https://i.imgur.com/WU9yOJK.png" width="500px"/>



## How to use

After running the application, you will be redirected to the `Tasks` page. Once application processing has finished, the download link will be available. Click on it.

<img src="https://i.imgur.com/4rdr2Pk.png"/>

**Note:** Result archive (or archive parts) will be available for download:

- single archive: in the **Tasks list** (image below) or from **Team Files**
  - `Team Files`->`tmp`->`supervisely`->`export`->`export-only-labeled-items`->`task_id`->`<projectId>_<projectName>.tar`
- split archive: all parts will be stored in the **Team Files** directory
  - `Team Files`->`tmp`->`supervisely`->`export`->`export-only-labeled-items`->`<task_id>`

<img src="https://i.imgur.com/B75bSh1.png"/>

### How to extract split archives

In the case of a split archive:

1. download all parts from `Team Files` directory (`Team Files`->`tmp`->`supervisely`->`export`->`export-only-labeled-items`->`<task_id>`)
2. After downloading all archive parts, you can extract them:

- for Windows:
  use the following freeware to unpack Multi-Tar files: [7-zip](https://www.7-zip.org/) and click on the first file (with extension `.tar.001`)

- for MacOS:
  replace `<path_to_folder_with_archive_parts>`, `<projectId>` and `<projectName>` with your values and run the following commands in the terminal:

```bash
cd <path_to_folder_with_archive_parts>
cat <projectId>_<projectName>.tar* | tar --options read_concatenated_archives -xvf -
```

- for Linux (Ubuntu):
  replace `<path_to_folder_with_archive_parts>`, `<projectId>` and `<projectName>` with your values and run the following commands in the terminal:

```bash
cd <path_to_folder_with_archive_parts>
cat '<projectId>_<projectName>.tar'* > result_archive.tar | tar -xvf result_archive.tar
```
