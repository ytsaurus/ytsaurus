package ru.yandex.spark.yt.fs

import org.apache.hadoop.fs.Path
import ru.yandex.spark.yt.serializers.PivotKeysConverter
import ru.yandex.spark.yt.wrapper.YtWrapper.PivotKey

sealed abstract class YtPath(path: Path, name: String) extends Path(path, name) {
  lazy val stringPath: String = path.toUri.getPath

  def rowCount: Long
}

case class YtStaticPath(path: Path,
                        beginRow: Long,
                        rowCount: Long) extends YtPath(path, s"${beginRow}_${beginRow + rowCount}") {
}

case class YtDynamicPath(path: Path,
                         beginKey: PivotKey,
                         endKey: PivotKey,
                         id: String,
                         keyColumns: Seq[String]) extends YtPath(path, id) {
  override def rowCount: Long = 1
}

object YtPath {
  def basePath(path: Path): String = path.getParent.toUri.toString
}
