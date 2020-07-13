package ru.yandex.spark.yt.fs

import org.apache.hadoop.fs.Path
import ru.yandex.spark.yt.wrapper.YtWrapper.PivotKey
import ru.yandex.spark.yt.wrapper.table.OptimizeMode

import scala.util.Try

sealed abstract class YtPath(path: Path, name: String) extends Path(path, name) {
  lazy val stringPath: String = path.toUri.getPath

  def rowCount: Long
}

case class YtStaticPath(path: Path,
                        optimizeMode: OptimizeMode,
                        beginRow: Long,
                        rowCount: Long) extends YtPath(path, s"${optimizeMode.name}_${beginRow}_${beginRow + rowCount}") {
  def optimizedForScan: Boolean = optimizeMode == OptimizeMode.Scan
}

object YtStaticPath {
  def fromPath(path: Path): Option[YtStaticPath] = {
    Try {
      val optimizeMode :: beginRowStr :: endRowStr :: Nil = path.getName.trim.split("_", 3).toList
      val beginRow = beginRowStr.trim.toLong
      val endRow = endRowStr.trim.toLong
      YtStaticPath(path.getParent, OptimizeMode.fromName(optimizeMode), beginRow, endRow - beginRow)
    }.toOption
  }
}

case class YtDynamicPath(path: Path,
                         beginKey: PivotKey,
                         endKey: PivotKey,
                         id: String,
                         keyColumns: Seq[String]) extends YtPath(path, id) {
  override def rowCount: Long = 1
}

object YtPath {
  def basePath(path: Path): String = path.getParent.toUri.getPath

  def fromPath(path: Path): Path = {
    path match {
      case yp: YtDynamicPath => yp
      case yp: YtStaticPath => yp
      case p =>
        YtStaticPath.fromPath(p) match {
          case Some(yp) => yp
          case None => p
        }
    }
  }
}
