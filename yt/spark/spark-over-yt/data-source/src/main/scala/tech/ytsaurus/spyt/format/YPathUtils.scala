package tech.ytsaurus.spyt.format

import tech.ytsaurus.core.cypress.{Exact, Range, RangeCriteria, RangeLimit, YPath}
import tech.ytsaurus.ysontree.{YTree, YTreeNode}

object YPathUtils {
  private def getOnlyKey(limit: RangeLimit): Option[java.util.List[YTreeNode]] = {
    if (!limit.key.isEmpty && limit.rowIndex == -1 && limit.offset == -1) {
      Some(limit.key)
    } else {
      None
    }
  }

  private def getOnlyIndex(limit: RangeLimit): Option[Long] = {
    if (limit.key.isEmpty && limit.rowIndex != -1 && limit.offset == -1) {
      Some(limit.rowIndex)
    } else {
      None
    }
  }

  implicit class RichExact(e: Exact) {
    def toRange: Range = {
      val onlyKey = getOnlyKey(e.exact)
      val onlyIndex = getOnlyIndex(e.exact)
      if (onlyKey.isDefined) {
        // Copied from tech.ytsaurus.core.cypress.Exact.forRetry.
        val lastPart = YTree.builder.beginAttributes.key("type").value("max").endAttributes.entity.build
        val upperKey = new java.util.ArrayList(onlyKey.get)
        upperKey.add(lastPart)
        new Range(RangeLimit.builder.setKey(onlyKey.get).build, RangeLimit.key(upperKey))
      } else if (onlyIndex.isDefined) {
        new Range(RangeLimit.row(onlyIndex.get), RangeLimit.row(onlyIndex.get + 1))
      } else {
        throw new IllegalArgumentException("Cannot process exact file interval")
      }
    }
  }

  implicit class RichRangeCriteria(rangeCriteria: RangeCriteria) {
    def toRange: Range = rangeCriteria match {
      case e: Exact => e.toRange
      case r: Range => r
    }
  }

  def getPath(ypath: YPath): String = ypath.justPath().toString

  def rangeCriteriaOption(ypath: YPath): Option[RangeCriteria] = {
    val ranges = ypath.getRanges
    if (ranges.isEmpty) {
      None
    } else {
      Some(ranges.get(0))
    }
  }

  def rangeOption(ypath: YPath): Option[Range] = rangeCriteriaOption(ypath).map(_.toRange)

  def beginKey(ypath: YPath): Seq[YTreeNode] = beginKeyOption(ypath).getOrElse(Nil)

  def endKey(ypath: YPath): Seq[YTreeNode] = endKeyOption(ypath).getOrElse(Nil)

  def beginKeyOption(ypath: YPath): Option[Seq[YTreeNode]] = {
    import scala.collection.JavaConverters._
    rangeOption(ypath).map(_.lower.key.asScala)
  }

  def endKeyOption(ypath: YPath): Option[Seq[YTreeNode]] = {
    import scala.collection.JavaConverters._
    rangeOption(ypath).map(_.upper.key.asScala)
  }

  def beginRowOption(ypath: YPath): Option[Long] = rangeOption(ypath).map(_.lower.rowIndex)

  def endRowOption(ypath: YPath): Option[Long] = rangeOption(ypath).map(_.upper.rowIndex)

  def rowCount(ypath: YPath): Option[Long] = {
    rangeOption(ypath).flatMap(range =>
      getOnlyIndex(range.upper).flatMap(endRow =>
        getOnlyIndex(range.lower).map(beginRow => endRow - beginRow))
    )
  }
}
