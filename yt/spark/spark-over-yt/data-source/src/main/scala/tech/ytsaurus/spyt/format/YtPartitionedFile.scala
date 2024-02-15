package tech.ytsaurus.spyt.format

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.execution.datasources.PartitionedFile
import tech.ytsaurus.spyt.common.utils.{TuplePoint, TupleSegment}
import YtPartitionedFile._
import tech.ytsaurus.spyt.serializers.PivotKeysConverter
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.core.cypress.{Range, RangeCriteria, RangeLimit, YPath}
import tech.ytsaurus.ysontree.{YTreeBinarySerializer, YTreeNode}

import java.io.ByteArrayInputStream

// At most one range supported inside ypath.
class YtPartitionedFile(val serializedYPath: Array[Byte],
                        val byteLength: Long,
                        val isDynamic: Boolean,
                        val modificationTs: Long,
                        override val partitionValues: InternalRow)
  extends PartitionedFile(
    partitionValues = partitionValues,
    filePath = getPath(serializedYPath),
    start = getNormalizedStart(serializedYPath),
    length = byteLength
  ) {
  def path: String = filePath

  def copy(newEndRow: Long): YtPartitionedFile = {
    withNewRangeCriteria(
      new Range(
        RangeLimit.row(beginRow),
        RangeLimit.row(newEndRow)
      )
    )
  }

  def copy(newBeginKey: Array[Byte], newEndKey: Array[Byte]): YtPartitionedFile = {
    import scala.collection.JavaConverters._
    withNewRangeCriteria(
      new Range(
        RangeLimit.key(PivotKeysConverter.toList(newBeginKey).asJava),
        RangeLimit.key(PivotKeysConverter.toList(newEndKey).asJava)
      )
    )
  }

  def isReadByKeys: Boolean = beginKeyOption.nonEmpty || endKeyOption.nonEmpty

  def ypath: YPath = deserializeYPath(serializedYPath)

  def getAttributeFromSelf[T](attributeGetter: YPath => T): T = {
    attributeGetter(ypath)
  }

  def rangeOption: Option[Range] = getAttributeFromSelf(YPathUtils.rangeOption)

  def beginKey: Seq[YTreeNode] = getAttributeFromSelf(YPathUtils.beginKey)

  def beginPoint: Option[TuplePoint] = {
    if (beginKey.isEmpty) {
      Some(TupleSegment.mInfinity)
    } else {
      PivotKeysConverter.toPoint(beginKey)
    }
  }

  def endKey: Seq[YTreeNode] = getAttributeFromSelf(YPathUtils.endKey)

  def endPoint: Option[TuplePoint] = {
    if (endKey.isEmpty) {
      Some(TupleSegment.pInfinity)
    } else {
      PivotKeysConverter.toPoint(endKey)
    }
  }

  def beginKeyOption: Option[Seq[YTreeNode]] = getAttributeFromSelf(YPathUtils.beginKeyOption)

  def endKeyOption: Option[Seq[YTreeNode]] = getAttributeFromSelf(YPathUtils.endKeyOption)

  def beginRow: Long = getAttributeFromSelf(getStart)

  def endRow: Long = getAttributeFromSelf(getEnd)

  def withNewRangeCriteria(rangeCriteria: RangeCriteria): YtPartitionedFile = {
    new YtPartitionedFile(
      serializedYPath = serializeYPath(ypath.ranges(rangeCriteria)),
      byteLength = byteLength,
      isDynamic = isDynamic,
      modificationTs = modificationTs,
      partitionValues = partitionValues
    )
  }
}

object YtPartitionedFile {
  val emptyInternalRow = new GenericInternalRow(new Array[Any](0))

  val fullRange: RangeCriteria = new Range(RangeLimit.key(), RangeLimit.key())

  private def toSimpleYPath(path: String): YPath = {
    YPath.simple(YtWrapper.formatPath(path))
  }

  def static(path: String, beginRow: Long, endRow: Long, byteLength: Long, modificationTs: Long = 0L,
             emptyInternalRow: InternalRow = YtPartitionedFile.emptyInternalRow): YtPartitionedFile = {
    static(toSimpleYPath(path), beginRow, endRow, byteLength, modificationTs, emptyInternalRow)
  }

  def static(path: YPath, beginRow: Long, endRow: Long, byteLength: Long, modificationTs: Long,
             emptyInternalRow: InternalRow): YtPartitionedFile = {
    val ypath = path.ranges(new Range(RangeLimit.row(beginRow), RangeLimit.row(endRow)))
    YtPartitionedFile(ypath, byteLength, isDynamic = false, modificationTs, emptyInternalRow)
  }

  def dynamic(path: String, range: RangeCriteria, byteLength: Long, modificationTs: Long = 0L,
              emptyInternalRow: InternalRow = YtPartitionedFile.emptyInternalRow): YtPartitionedFile = {
    dynamic(toSimpleYPath(path), range, byteLength, modificationTs, emptyInternalRow)
  }

  def dynamic(path: YPath, range: RangeCriteria,
              byteLength: Long, modificationTs: Long, emptyInternalRow: InternalRow): YtPartitionedFile = {
    val ypath = path.ranges(range)
    YtPartitionedFile(ypath, byteLength = byteLength, isDynamic = true, modificationTs = modificationTs, partitionValues = emptyInternalRow)
  }

  def apply(yPath: YPath, byteLength: Long, isDynamic: Boolean, modificationTs: Long, partitionValues: InternalRow): YtPartitionedFile = {
    new YtPartitionedFile(serializeYPath(yPath), byteLength, isDynamic, modificationTs, partitionValues)
  }

  def getAttributeFromYPath[T](attributeGetter: YPath => T)(serializedPath: Array[Byte]): T = {
    attributeGetter(deserializeYPath(serializedPath))
  }

  private def serializeYPath(ypath: YPath): Array[Byte] = {
    ypath.toTree.toBinary
  }

  private def deserializeYPath(serializedYPath: Array[Byte]): YPath = {
    val input = new ByteArrayInputStream(serializedYPath)
    val treeNode = YTreeBinarySerializer.deserialize(input)
    YPath.fromTree(treeNode)
  }

  private def getStartOption(ypath: YPath): Option[Long] = {
    YPathUtils.beginRowOption(ypath)
  }

  private def getStart(ypath: YPath): Long = {
    getStartOption(ypath).getOrElse(0)
  }

  private def getEnd(ypath: YPath): Long = {
    YPathUtils.endRowOption(ypath).getOrElse(0)
  }

  private def getPath: Array[Byte] => String = {
    getAttributeFromYPath(YPathUtils.getPath)
  }

  private def getStart: Array[Byte] => Long = {
    getAttributeFromYPath(getStart)
  }

  private def getNormalizedStart(serializedYPath: Array[Byte]): Long = {
    val start = getStart(serializedYPath)
    if (start == -1) {
      0
    } else {
      start
    }
  }
}
