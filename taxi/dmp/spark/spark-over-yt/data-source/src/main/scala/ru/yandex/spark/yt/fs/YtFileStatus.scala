package ru.yandex.spark.yt.fs

import org.apache.hadoop.fs.{FileStatus, LocatedFileStatus}

class YtFileStatus(val path: YtPath,
                   val totalRowCount: Long,
                   val totalChunksCount: Long)
  extends LocatedFileStatus(new FileStatus(path.rowCount, false, 1, path.rowCount, 0, path), Array.empty) {

  val avgChunkSize: Long = if (totalChunksCount > 0) totalRowCount / totalChunksCount + 1 else 1
}
