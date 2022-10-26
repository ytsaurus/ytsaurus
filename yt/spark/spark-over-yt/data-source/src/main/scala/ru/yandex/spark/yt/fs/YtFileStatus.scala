package ru.yandex.spark.yt.fs

import org.apache.hadoop.fs.{FileStatus, LocatedFileStatus}

class YtFileStatus(path: YtPath,
                   approximateRowSize: Long,
                   modificationTime: Long)
  extends LocatedFileStatus(
    new FileStatus(approximateRowSize * path.rowCount, false, 1, approximateRowSize, modificationTime, path),
    Array.empty
  ) {
}
