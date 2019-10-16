package ru.yandex.spark.yt.format

import org.apache.hadoop.fs.Path

class YtPath(path: Path,
             val startRow: Long,
             val rowCount: Long) extends Path(path, s"${startRow}_$rowCount") {
  lazy val stringPath: String = path.toUri.getPath
}

object YtPath {
  def decode(path: Path): YtPath = {
    new YtPath(path.getParent, path.getName.split("_").head.toLong, path.getName.split("_").last.toLong)
  }

  def decode(path: String): YtPath = decode(new Path(path))
}
