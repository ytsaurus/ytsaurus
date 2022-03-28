package ru.yandex.spark.yt.common.utils

import org.apache.spark.sql.sources._
import ru.yandex.inside.yt.kosher.ytree.{YTreeDoubleNode, YTreeIntegerNode, YTreeNode, YTreeStringNode}
import ru.yandex.spark.yt.common.utils.Segment.Segment
import ru.yandex.spark.yt.logger.YtLogger

object ExpressionTransformer {

  def filtersToSegmentSet(dataFilters: Seq[Filter])
                         (implicit ytLog: YtLogger = YtLogger.noop): SegmentSet = {
    val set = dataFilters.map(expressionToSegmentSet)
    val res = SegmentSet.intercept(set: _*)
    if (set.length > 2) {
      ytLog.warn(s"ExpressionTransformer intercepts more than 2 filters",
        Map("filters" -> dataFilters.mkString(", "), "interception" -> res.toString))
    }
    res
  }

  private def parseOrderedLiteral(exp: Any): Option[RealValue[_]] = {
    exp match {
      case x: Long => Some(RealValue(x))
      case x: Int => Some(RealValue(x.longValue()))
      case x: Short => Some(RealValue(x.longValue()))
      case x: Byte => Some(RealValue(x.longValue()))
      case x: Double => Some(RealValue(x))
      case x: Float => Some(RealValue(x.doubleValue()))
      case x: String => Some(RealValue(x))
      case _ => None
    }
  }

  private def sortedRealValues(seq: Seq[RealValue[_]], head: RealValue[_]): Option[Seq[RealValue[_]]] = {
    head match {
      case x: RealValue[_] if x.value.isInstanceOf[Double] => Some(seq.map(_.asInstanceOf[RealValue[Double]]).sorted)
      case x: RealValue[_] if x.value.isInstanceOf[Long] => Some(seq.map(_.asInstanceOf[RealValue[Long]]).sorted)
      case _: RealValue[String] => Some(seq.map(_.asInstanceOf[RealValue[String]]).sorted)
      case _ => None
    }
  }

  private def parseOrderedLiteralList(exp: List[Any]): Option[Seq[RealValue[_]]] = {
    import cats.implicits._
    for {
      valuesList <- exp.map(parseOrderedLiteral).traverse(identity)
      valuesHead <- valuesList.headOption
      sortedValues <- sortedRealValues(valuesList, valuesHead)
    } yield sortedValues
  }

  private def processComparison(left: String, right: Any)(f: RealValue[_] => Segment): SegmentSet = {
    parseOrderedLiteral(right)
      .map(p => SegmentSet(left, f(p)))
      .getOrElse(SegmentSet())
  }

  def expressionToSegmentSet(expression: Filter): SegmentSet = {
    expression match {
      case LessThan(left, right) =>
        processComparison(left, right)(point => new Segment(MInfinity(), point))
      case LessThanOrEqual(left, right) =>
        processComparison(left, right)(point => new Segment(MInfinity(), point))
      case GreaterThan(left, right) =>
        processComparison(left, right)(point => new Segment(point, PInfinity()))
      case GreaterThanOrEqual(left, right) =>
        processComparison(left, right)(point => new Segment(point, PInfinity()))
      case EqualTo(left, right) =>
        processComparison(left, right)(Segment(_))
      case Or(left, right) =>
        val l = expressionToSegmentSet(left)
        val r = expressionToSegmentSet(right)
        val lKeys = l.map.keys
        val rKeys = r.map.keys
        if (lKeys.size == 1 && lKeys == rKeys) {
          SegmentSet.union(l, r)
        } else {
          SegmentSet()
        }
      case And(left, right) =>
        SegmentSet.intercept(
          expressionToSegmentSet(left),
          expressionToSegmentSet(right))
      case In(varName, right) =>
        val valuesList = parseOrderedLiteralList(right.toList)
        valuesList.map { vL => SegmentSet(Map(varName -> vL.map(Segment(_)))) }.getOrElse(SegmentSet())
      case _ => SegmentSet()
    }
  }

  def isSupportedNodeType(node: YTreeNode): Boolean = node match {
    case _: YTreeDoubleNode => true
    case _: YTreeStringNode => true
    case _: YTreeIntegerNode => true
    case _ => false
  }

  def nodeToPoint(node: YTreeNode): RealValue[_] = node match {
    case s: YTreeStringNode => RealValue(s.getValue)
    case d: YTreeDoubleNode => RealValue(d.getValue)
    case l: YTreeIntegerNode => RealValue(l.getLong)
  }
}

