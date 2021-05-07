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

Export labeled items from [Supervisely](https://app.supervise.ly) project and prepares downloadable `tar` archive. Images, videos and point clouds projects can be exported. Unlabeled items will be skipped. You can choose to export both data and annotations or only annotations.



## How To Run 
**Step 1**: Add app to your team from [Ecosystem](https://app.supervise.ly/apps/ecosystem/export-only-labeled-items) if it is not there.

**Step 2**: Open context menu of project -> `Download via App` -> `Export only labeled items` 

<img src="https://i.imgur.com/EEQSPB3.png"/>





**Step 3**: Select the project export mode.

<img src="https://i.imgur.com/tVHGW95.png"/>



## How to use

After running the application, you will be redirected to the `Tasks` page. Once application processing has finished, your link for downloading will be available. Click on the `file name` to download it.



<img src="https://i.imgur.com/0kbPDBr.png"/>

**Note** You can also find your converted project in: `Team Files`->`ApplicationsData`->`Export_only_labeled_items`->`TaskID`->`projectID_projectName.tar`

<img src="https://i.imgur.com/YO7AABZ.png"/>