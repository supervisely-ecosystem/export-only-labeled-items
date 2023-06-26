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


## How To Run 
**Step 1**: Add app to your team from [Ecosystem](https://app.supervise.ly/apps/ecosystem/export-only-labeled-items) if it is not there.

**Step 2**: Open context menu of project -> `Download via App` -> `Export only labeled items` 

<img src="media/htr2.png"/>


**Step 3**: Select project export mode.

<img src="media/htr3.png" width="500px"/>



## How to use

After running the application, you will be redirected to the `Tasks` page. Once application processing has finished, the download link will be available. Click on it to download archive.

<img src="media/htu.png"/>

**Note:** You can also find your converted project in: `Team Files`->`Export_only_labeled_items`->`<taskId>_<projectId>_<projectName>.tar`

<img src="media/htua.png"/>
