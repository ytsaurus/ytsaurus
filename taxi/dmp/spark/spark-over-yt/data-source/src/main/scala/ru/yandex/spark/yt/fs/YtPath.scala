package ru.yandex.spark.yt.fs

import org.apache.hadoop.fs.Path
import ru.yandex.spark.yt.fs.YPathEnriched.ypath
import ru.yandex.spark.yt.fs.YtStaticPath.toFileName
import ru.yandex.spark.yt.wrapper.YtWrapper.PivotKey
import ru.yandex.spark.yt.wrapper.table.OptimizeMode

import scala.util.Try

sealed abstract class YtPath(val ypath: YPathEnriched, name: String) extends Path(ypath.toPath, name) {
  def rowCount: Long

  def toStringPath: String = ypath.toStringPath
}

case class YtStaticPath(override val ypath: YPathEnriched,
                        attrs: YtStaticPathAttributes) extends YtPath(ypath, toFileName(attrs)) {
  def optimizedForScan: Boolean = attrs.optimizeMode == OptimizeMode.Scan

  override def rowCount: Long = attrs.rowCount
}

case class YtStaticPathAttributes(optimizeMode: OptimizeMode,
                                  beginRow: Long,
                                  rowCount: Long)

object YtStaticPath {
  def toFileName(attrs: YtStaticPathAttributes): String = {
    import attrs._
    s"${optimizeMode.name}_${beginRow}_${rowCount}"
  }

  def fromPath(path: Path): Option[YtStaticPath] = {
    Try {
      val optimizeModeStr :: beginRowStr :: rowCountStr :: Nil = path.getName.trim.split("_", 3).toList
      val optimizeMode = OptimizeMode.fromName(optimizeModeStr)
      val beginRow = beginRowStr.trim.toLong
      val rowCount = rowCountStr.trim.toLong
      YtStaticPath(ypath(path.getParent), YtStaticPathAttributes(optimizeMode, beginRow, rowCount))
    }.toOption
  }
}

case class YtDynamicPath(override val ypath: YPathEnriched,
                         beginKey: PivotKey,
                         endKey: PivotKey,
                         id: String,
                         keyColumns: Seq[String]) extends YtPath(ypath, id) {
  override def rowCount: Long = 1
}

object YtPath {
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
