package tech.ytsaurus.spyt.fs

import org.apache.hadoop.fs.Path
import tech.ytsaurus.core.cypress.YPath
import tech.ytsaurus.spyt.fs.YtStaticPath.toFileName
import tech.ytsaurus.spyt.fs.path.YPathEnriched
import tech.ytsaurus.spyt.fs.path.YPathEnriched.ypath
import tech.ytsaurus.spyt.serializers.PivotKeysConverter
import tech.ytsaurus.spyt.wrapper.YtWrapper.PivotKey
import tech.ytsaurus.spyt.wrapper.table.OptimizeMode

import java.util.Base64
import scala.util.Try

sealed abstract class YtPath(val ypath: YPathEnriched, name: String) extends Path(ypath.toPath, name) {
  def rowCount: Long

  def toStringPath: String = ypath.toStringPath

  def isDynamic: Boolean

  def toYPath: YPath
}

case class YtStaticPath(override val ypath: YPathEnriched,
                        attrs: YtStaticPathAttributes) extends YtPath(ypath, YtStaticPath.toFileName(attrs)) {
  def optimizedForScan: Boolean = attrs.optimizeMode == OptimizeMode.Scan

  override def rowCount: Long = attrs.rowCount

  def isDynamic: Boolean = false

  def toYPath: YPath = {
    val res = ypath.toYPath.withRange(attrs.beginRow, attrs.beginRow + attrs.rowCount)
    res
  }
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
                         attrs: YtDynamicPathAttributes) extends YtPath(ypath, YtDynamicPath.toFileName(attrs)) {
  override def rowCount: Long = 1

  def isDynamic: Boolean = true

  def toYPath: YPath = {
    val res = ypath.toYPath.withRange(
      PivotKeysConverter.toRangeLimit(attrs.beginKey),
      PivotKeysConverter.toRangeLimit(attrs.endKey)
    )
    res
  }
}

case class YtDynamicPathAttributes(beginKey: PivotKey,
                                   endKey: PivotKey,
                                   keyColumns: Seq[String])

object YtDynamicPath {
  private def encodeKey: Array[Byte] => String = Base64.getEncoder.encodeToString

  private def decodeKey: String => Array[Byte] = Base64.getDecoder.decode

  def toFileName(attrs: YtDynamicPathAttributes): String = {
    import attrs._
    f"${encodeKey(beginKey)}_${encodeKey(endKey)}_${keyColumns.mkString(",")}"
  }

  def fromPath(path: Path): Option[YtDynamicPath] = {
    Try {
      val beginKey :: endKey :: keyColumns :: Nil = path.getName.trim.split("_", 3).toList
      YtDynamicPath(ypath(path.getParent),
        YtDynamicPathAttributes(decodeKey(beginKey), decodeKey(endKey), keyColumns.split(",")))
    }.toOption
  }
}

object YtPath {
  def fromPath(path: Path): Path = {
    path match {
      case yp: YtDynamicPath => yp
      case yp: YtStaticPath => yp
      case p =>
        YtStaticPath.fromPath(p).orElse { YtDynamicPath.fromPath(p) } .getOrElse(p)
    }
  }
}
