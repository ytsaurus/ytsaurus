package tech.ytsaurus.spyt.fs

import org.apache.hadoop.fs.Path
import tech.ytsaurus.core.cypress.{RangeLimit, YPath}
import tech.ytsaurus.spyt.fs.path.YPathEnriched
import tech.ytsaurus.spyt.fs.path.YPathEnriched.ypath
import tech.ytsaurus.spyt.serializers.PivotKeysConverter
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.wrapper.YtWrapper.PivotKey
import tech.ytsaurus.spyt.wrapper.table.OptimizeMode
import tech.ytsaurus.ysontree.{YTreeBinarySerializer, YTreeBuilder, YTreeNode}

import java.io.ByteArrayInputStream
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
    ypath.toYPath.withRange(attrs.lowerRangeLimit, attrs.upperRangeLimit)
  }
}

case class YtDynamicPathAttributes(lowerRangeLimit: RangeLimit = YtDynamicPath.emptyRangeLimit,
                                   upperRangeLimit: RangeLimit = YtDynamicPath.emptyRangeLimit)

object YtDynamicPath {
  val emptyRangeLimit: RangeLimit = RangeLimit.builder().build()

  private def encodeBytes: Array[Byte] => String = Base64.getEncoder.encodeToString

  private def encodeLimit(limit: RangeLimit): String = encodeBytes(limit.toTree(new YTreeBuilder()).build().toBinary)

  private def decodeBytes: String => Array[Byte] = Base64.getDecoder.decode

  private def decodeLimit(limit: String): RangeLimit = {
    RangeLimit.fromTree(YTreeBinarySerializer.deserialize(new ByteArrayInputStream(decodeBytes(limit))))
  }

  def toFileName(attrs: YtDynamicPathAttributes): String = {
    f"${encodeLimit(attrs.lowerRangeLimit)}_${encodeLimit(attrs.upperRangeLimit)}"
  }

  def fromPath(path: Path): Option[YtDynamicPath] = {
    Try {
      val lowerLimit :: upperLimit :: Nil = path.getName.trim.split("_", 2).toList
      YtDynamicPath(ypath(path.getParent), YtDynamicPathAttributes(decodeLimit(lowerLimit), decodeLimit(upperLimit)))
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
