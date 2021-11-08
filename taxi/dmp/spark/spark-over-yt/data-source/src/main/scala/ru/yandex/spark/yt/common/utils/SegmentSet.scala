package ru.yandex.spark.yt.common.utils

import org.apache.spark.sql.sources.{And, Filter, GreaterThanOrEqual, In, LessThanOrEqual, Or}

trait Point extends Ordered[Point]

case class MInfinity() extends Point {
  override def compare(that: Point): Int = that match {
    case MInfinity() => 0
    case _ => -1
  }
}

case class RealValue[T](value: T)(implicit ord: Ordering[T]) extends Point {
  override def compare(that: Point): Int = that match {
    case MInfinity() => 1
    case a: RealValue[T] => ord.compare(value, a.value)
    case PInfinity() => -1
    case _ => throw new IllegalArgumentException("Comparing different types is not allowed")
  }
}

object RealValue {
  implicit def ordering[T](implicit ord: Ordering[T]): Ordering[RealValue[T]] = {
    ord.on[RealValue[T]](_.value)
  }
}

case class PInfinity() extends Point {
  override def compare(that: Point): Int = that match {
    case PInfinity() => 0
    case _ => 1
  }
}

case class Segment(left: Point, right: Point)

object Segment {
  def apply(point: Point): Segment = Segment(point, point)

  def toFilters(varName: String, segmentsAndPoints: Seq[Segment]): List[Filter] = {
    val (points, segments) = segmentsAndPoints.partition { segment => segment.left == segment.right }
    val pointFilters =
      if (points.nonEmpty) List(In(varName, points.map { case Segment(_, RealValue(value)) => value }.toArray))
      else Nil
    val segmentFilters = segments.flatMap(segmentToFilter(varName, _))
    pointFilters ++ segmentFilters
  }

  def segmentToFilter(varName: String, segment: Segment): Option[Filter] = segment match {
    case Segment(MInfinity(), PInfinity()) => None
    case Segment(MInfinity(), RealValue(value)) => Some(LessThanOrEqual(varName, value))
    case Segment(RealValue(value), PInfinity()) => Some(GreaterThanOrEqual(varName, value))
    case Segment(RealValue(left), RealValue(right)) =>
      Some(And(GreaterThanOrEqual(varName, left), LessThanOrEqual(varName, right)))
  }

  sealed trait SegmentSide

  object SegmentSide {
    case object Begin extends SegmentSide
    case object End extends SegmentSide
  }

  def getAllPoints(array: Seq[Seq[Segment]]): Map[Point, Map[SegmentSide, Int]] = {
    val duplicatedPoints = array.flatten.foldLeft(List.empty[(Point, SegmentSide)]) {
      case (points, segment) =>
        (segment.left, SegmentSide.Begin) +: (segment.right, SegmentSide.End) +: points
    }
    duplicatedPoints.groupBy { case (point, _) => point }
      .mapValues(singlePointList => singlePointList.groupBy { case (_, kind) => kind }.mapValues(_.length))
  }

  def calculateCoverage(array: Seq[Seq[Segment]]): Seq[(Segment, Int)] = {
    val importantPointsMap = getAllPoints(array)
    val importantPoints = importantPointsMap.keys.toList.sorted
    if (importantPoints.isEmpty) {
      Nil
    } else {
      val (_, _, reversedResult) = importantPoints.foldLeft((Option.empty[Point], 0, List.empty[(Segment, Int)])) {
        case ((previousPoint, beforePointCoverage, result), currentPoint) =>
          val inPointCoverage = beforePointCoverage + importantPointsMap(currentPoint)
            .getOrElse(SegmentSide.Begin, 0)
          val afterPointCoverage = inPointCoverage - importantPointsMap(currentPoint)
            .getOrElse(SegmentSide.End, 0)
          val lastResult = previousPoint match {
            case Some(value) => (Segment(value, currentPoint), beforePointCoverage) +: result
            case None => result
          }
          (Some(currentPoint), afterPointCoverage,
            (Segment(currentPoint, currentPoint), inPointCoverage) +: lastResult)
      }
      reversedResult.reverse
    }
  }

  def unionNeighbourSegments(array: Seq[Segment]): Seq[Segment] = {
    array.foldLeft(List.empty[Segment]) {
      case (result, current) =>
        result match {
          case Nil => List(current)
          case head :: tail =>
            if (head.right == current.left) {
              Segment(head.left, current.right) :: tail
            } else {
              current :: result
            }
        }
    }.reverse
  }

  def getCoveredSegments(array: Seq[Seq[Segment]], limit: Int): Seq[Segment] = {
    val coverage = calculateCoverage(array)
    unionNeighbourSegments(
      coverage
        .collect {
          case (segment, coverage) if coverage >= limit => segment
        }
    )
  }

  def union(array: Seq[Seq[Segment]]): Seq[Segment] = getCoveredSegments(array, 1)

  def intercept(array: Seq[Seq[Segment]]): Seq[Segment] = getCoveredSegments(array, array.length)
}

case class SegmentSet(map: Map[String, Seq[Segment]]) {
  def simplifySegments: SegmentSet = {
    SegmentSet(map.mapValues(segments => List(Segment(segments.head.left, segments.last.right)))
      .filter { case (_, segments) => segments != List(MInfinity(), PInfinity()) })
  }

  def toFilters: Array[Filter] = {
    map.flatMap {
      case (varName, segments) =>
        Segment.toFilters(varName, segments).reduceOption(Or)
    }.toArray
  }
}

object SegmentSet {
  def apply(): SegmentSet = new SegmentSet(Map.empty[String, Seq[Segment]])

  def apply(columnName: String, segments: Segment*): SegmentSet = new SegmentSet(Map((columnName, segments)))

  private def mergeByColumn(array: Seq[SegmentSet]): Map[String, Seq[Seq[Segment]]] = {
    val arrayKeys = array.map(x => x.map.keySet)
    val keys = arrayKeys.reduceOption(_.union(_)).getOrElse(Set.empty[String])
    keys.map(x => x -> array.flatMap(a => a.map.get(x))).toMap
  }

  def union(array: SegmentSet*): SegmentSet = {
    SegmentSet(mergeByColumn(array).mapValues(Segment.union))
  }

  def intercept(array: SegmentSet*): SegmentSet = {
    SegmentSet(mergeByColumn(array).mapValues(Segment.intercept).filter { case (_, segments) => segments.nonEmpty })
  }
}

