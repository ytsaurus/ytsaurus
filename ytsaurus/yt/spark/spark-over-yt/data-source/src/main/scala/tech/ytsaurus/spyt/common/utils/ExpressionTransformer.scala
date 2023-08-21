package tech.ytsaurus.spyt.common.utils

import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{BooleanType, ByteType, DataType, DoubleType, FloatType, IntegerType, LongType, ShortType, StringType}
import org.apache.spark.unsafe.types.UTF8String
import Segment.Segment
import tech.ytsaurus.spyt.logger.YtLogger
import tech.ytsaurus.spyt.common.utils
import tech.ytsaurus.ysontree._

object ExpressionTransformer {

  def filtersToSegmentSet(dataFilters: Seq[Filter])
                         (implicit ytLog: YtLogger = YtLogger.noop): SegmentSet = {
    val set = dataFilters.map(expressionToSegmentSet)
    val res = SegmentSet.intercept(set: _*)
    if (dataFilters.count(!_.isInstanceOf[IsNotNull]) > 2) {
      ytLog.warn(s"ExpressionTransformer intercepts more than 2 filters",
        Map("filters" -> dataFilters.mkString(", "), "interception" -> res.toString))
    }
    res
  }

  def parseOrderedLiteral(exp: Any): Option[Point] = {
    exp match {
      case null => Some(RealValue(null))
      case x: Boolean => Some(RealValue(x))
      case x: Long => Some(RealValue(x))
      case x: Int => Some(RealValue(x.longValue()))
      case x: Short => Some(RealValue(x.longValue()))
      case x: Byte => Some(RealValue(x.longValue()))
      case x: Double => Some(RealValue(x))
      case x: Float => Some(RealValue(x.doubleValue()))
      case x: String => Some(RealValue(x))
      case x: UTF8String => Some(RealValue(x.toString))
      case _ => None
    }
  }

  def parseLiteralList(exp: List[Any]): Option[Seq[Point]] = {
    import cats.implicits._
    exp.map(parseOrderedLiteral).traverse(identity)
  }

  private def parseOrderedLiteralList(exp: List[Any]): Option[Seq[Point]] = {
    parseLiteralList(exp).map(_.sorted)
  }

  private def processComparison(left: String, right: Any)(f: Point => Segment): SegmentSet = {
    parseOrderedLiteral(right)
      .map(p => utils.SegmentSet(left, f(p)))
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
      case IsNull(varName) =>
        expressionToSegmentSet(EqualTo(varName, null))
      case _ => SegmentSet()
    }
  }

  def isSupportedNodeType(node: YTreeNode): Boolean = node match {
    case _: YTreeBooleanNode => true
    case _: YTreeDoubleNode => true
    case _: YTreeStringNode => true
    case _: YTreeIntegerNode => true
    case _: YTreeEntityNode => true
    case _ => false
  }

  def isSupportedDataType(dataType: DataType): Boolean = dataType match {
    case _: BooleanType => true
    case _: ByteType => true
    case _: ShortType => true
    case _: IntegerType => true
    case _: LongType => true
    case _: FloatType => true
    case _: DoubleType => true
    case _: StringType => true
    case _ => false
  }

  def nodeToPoint(node: YTreeNode): Point = node match {
    case b: YTreeBooleanNode => RealValue(b.getValue)
    case s: YTreeStringNode => RealValue(s.getValue)
    case d: YTreeDoubleNode => RealValue(d.getValue)
    case l: YTreeIntegerNode => RealValue(l.getLong)
    case e: YTreeEntityNode =>
      val t = e.getAttribute("type")
      if (t.isPresent) {
        t.get().stringValue() match {
          case "max" => PInfinity()
          case "min" => MInfinity()
        }
      } else {
        RealValue(null)
      }
  }
}

