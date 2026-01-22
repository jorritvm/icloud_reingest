# icloud-reingest
Reingest curated and archived media into iCloud

## Introduction
The workflow below has a critical flaw:
1. Capture photos and videos on your iPhone or iPad
2. Sync them to iCloud
3. Periodically off-load them to your pc for curating and archival

Curated media are not available on your iPhone or iPad. Instead these devices are stuck with the raw unfiltered media.

This tool creates a way to reingest curated and archived media back into iCloud so they are available on your devices again.
As a result the following steps are added to the workflow:
4. Define a transition date (earlier than the latest offloading cutoff date)
5. Remove all media from iCloud that is older than the transition date
6. Reingest curated and archived media back into iCloud up to the transition date

This creates an iCloud media collection that has curated images before the transition date and unfiltered ones after.
Visually that looks like this:

```
|<-- Curated and archived media -->|<-- Raw unfiltered media -->|
|<------- Reingested into iCloud ------->|<-- Still in iCloud -->|
```

## Critical Notes
Media are reingested with their original creation dates so they appear in the correct chronological order in iCloud.
If the correct date can't be found in the metadata, or there are reasons to believe the file modified date is wrong, the media file is skipped.

## How to install this tool
```python
poetry install --no-root
```
## How to use this tool
1. Generate a report of the picture media
2. Process the report so images to reingest are stored in a new flat location
3. Generate a report of the video media
4. Process the report so videos to reingest are stored in a new flat location
5. Reingest the curated media into iCloud via an intermediary device using PhotoSync

## Author
Jorrit Vander Mynsbrugge