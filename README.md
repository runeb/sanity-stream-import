# sanity-stream-import

Experimental streaming import client.

## Usage

Obtain an export of a Sanity project. This will be a file named datasetName.tar.gz. In this folder, run `npm install` and then the following:

```
PROJECT=your-project-id DATASET=target-dataset-name TOKEN=a-write-token node import datasetName.tar.gz
```

Substitute for your actual file name, of course.

## Disclaimer

This is an experimental importer, written to avoid writing to the disk while importing. This should theoretically reduce the time spend importing by quite a bit, if you are dealing with a large export and/or a slow disk, like HDD.

Please only use this importer for testing purposes, in development environments and not for production, at this time.
