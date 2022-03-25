package ru.yandex.spark.yt.common.utils

import org.apache.spark.sql.sources.{And, EqualTo, Filter, GreaterThan, GreaterThanOrEqual, In, LessThan, LessThanOrEqual, Or}
import ru.yandex.spark.yt.logger.YtLogger

object ExpressionTransformer {
  def filtersToSegmentSet(dataFilters: Seq[Filter])
                         (implicit ytLog: YtLogger = YtLogger.noop): SegmentSet = {
    val set = dataFilters.flatMap(expressionToSegmentSet)
    val res = SegmentSet.intercept(set:_*)
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

  private def processComparison(left: String, right: Any): Option[(String, RealValue[_])] = {
    for {
      segment <- parseOrderedLiteral(right)
    } yield left -> segment
  }

  def expressionToSegmentSet(expression: Filter): Option[SegmentSet] = {
    expression match {
      case LessThan(left, right) =>
        processComparison(left, right).map {
          case (col, segment) => SegmentSet(col, Segment(MInfinity(), segment))}
      case LessThanOrEqual(left, right) =>
        processComparison(left, right).map {
          case (col, segment) => SegmentSet(col, Segment(MInfinity(), segment))}
      case GreaterThan(left, right) =>
        processComparison(left, right).map {
          case (col, segment) => SegmentSet(col, Segment(segment, PInfinity()))}
      case GreaterThanOrEqual(left, right) =>
        processComparison(left, right).map {
          case (col, segment) => SegmentSet(col, Segment(segment, PInfinity()))}
      case EqualTo(left, right) =>
        processComparison(left, right).map {
          case (col, segment) => SegmentSet(col, Segment(segment))}
      case Or(left, right) =>
        expressionToSegmentSet(left).flatMap {
          l => expressionToSegmentSet(right).flatMap {
            r =>
              val lKeys = l.map.keys
              val rKeys = r.map.keys
              if (lKeys.size == 1 && lKeys == rKeys) {
                Some(SegmentSet.union(l, r))
              } else {
                None
              }
          }
        }
      case And(left, right) =>
        for {
          l <- expressionToSegmentSet(left)
          r <- expressionToSegmentSet(right)
        } yield {
          SegmentSet.intercept(l, r)
        }
      case In(varName, right) =>
        for {
          valuesList <- parseOrderedLiteralList(right.toList)
        } yield {
          SegmentSet(Map(varName -> valuesList.map(Segment(_))))
        }
      case _ => None
    }
  }
}

