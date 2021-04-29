<div align="center" markdown>
<img src="https://i.imgur.com/gUEuSc7.png"/>


# Export only labeled items

<p align="center">
  <a href="#Overview">Overview</a> •
  <a href="#How-To-Run">How To Run</a> •
  <a href="#How-To-Use">How To Use</a>
</p>

[![](https://img.shields.io/badge/slack-chat-green.svg?logo=slack)](https://supervise.ly/slack)
![GitHub release (latest SemVer)](https://img.shields.io/github/v/release/supervisely-ecosystem/export-only-labeled-items)
[![views](https://app.supervise.ly/public/api/v3/ecosystem.counters?repo=supervisely-ecosystem/export-only-labeled-items&counter=views&label=views)](https://supervise.ly)
[![used by teams](https://app.supervise.ly/public/api/v3/ecosystem.counters?repo=supervisely-ecosystem/export-only-labeled-items&counter=downloads&label=used%20by%20teams)](https://supervise.ly)
[![runs](https://app.supervise.ly/public/api/v3/ecosystem.counters?repo=supervisely-ecosystem/export-only-labeled-items&counter=runs&label=runs&123)](https://supervise.ly)

</div>

## Overview

Export [Supervisely](https://docs.supervise.ly/data-organization/00_ann_format_navi) project and prepares downloadable `tar` archive. Images, videos and point clouds projects can be export. If items in project not labeled they will be skipped on export. You can choose to load project both data and annotations or only annotations.



## How To Run 
**Step 1**: Add app to your team from [Ecosystem](https://ecosystem.supervise.ly/apps/convert-supervisely-to-cityscapes-format) if it is not there.

**Step 2**: Open context menu of project -> `Download as` -> `Export to Cityscapes format` 

<img src="https://i.imgur.com/kxVGztq.png" width="600px"/>


## How to use
After running the application, you will be redirected to the Tasks page. Once application processing has finished, your link for downloading will become available. Click on the file name to download it.

<img src="https://i.imgur.com/5WEh12e.png" width="800px"/>

**Note** You can also find your converted project in: Team Files->cityscapes_format->app_id->`projectId_projectName_formatName.tar`

<img src="https://i.imgur.com/5WEh12e.png" width="800px"/>
