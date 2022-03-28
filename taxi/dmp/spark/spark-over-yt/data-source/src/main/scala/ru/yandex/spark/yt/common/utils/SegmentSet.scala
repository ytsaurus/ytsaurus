package ru.yandex.spark.yt.common.utils

import org.apache.spark.sql.sources.{And, Filter, GreaterThanOrEqual, In, LessThanOrEqual, Or}
import ru.yandex.spark.yt.common.utils.Segment.Segment

object Segment {
  type Segment = AbstractSegment[Point]

  val full: Segment = Segment(MInfinity(), PInfinity())

  def unapply(segment: Segment): Option[(Point, Point)] = Some((segment.left, segment.right))

  def apply(left: Point, right: Point): Segment = new Segment(left, right)

  def apply(point: Point): Segment = Segment(point, point)

  private [utils]def toFilters(varName: String, segmentsAndPoints: Seq[Segment]): List[Filter] = {
    val (points, segments) = segmentsAndPoints.partition { segment => segment.left == segment.right }
    val pointFilters =
      if (points.nonEmpty) List(In(varName, points.map { case Segment(_, RealValue(value)) => value }.toArray))
      else Nil
    val segmentFilters = segments.flatMap(segmentToFilter(varName, _))
    pointFilters ++ segmentFilters
  }

  private [utils]def segmentToFilter(varName: String, segment: Segment): Option[Filter] = segment match {
    case Segment(MInfinity(), PInfinity()) => None
    case Segment(MInfinity(), RealValue(value)) => Some(LessThanOrEqual(varName, value))
    case Segment(RealValue(value), PInfinity()) => Some(GreaterThanOrEqual(varName, value))
    case Segment(RealValue(left), RealValue(right)) =>
      Some(And(GreaterThanOrEqual(varName, left), LessThanOrEqual(varName, right)))
  }
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

  // .map(identity) https://stackoverflow.com/questions/32900862/map-can-not-be-serializable-in-scala
  def union(array: SegmentSet*): SegmentSet = {
    SegmentSet(mergeByColumn(array).mapValues(AbstractSegment.union).map(identity))
  }

  def intercept(array: SegmentSet*): SegmentSet = {
    SegmentSet(mergeByColumn(array).mapValues(AbstractSegment.intercept).map(identity))
  }

  private def mergeByColumn(array: Seq[SegmentSet]): Map[String, Seq[Seq[Segment]]] = {
    val arrayKeys = array.map(x => x.map.keySet)
    val keys = arrayKeys.reduceOption(_.union(_)).getOrElse(Set.empty[String])
    keys.map(x => x -> array.flatMap(a => a.map.get(x))).toMap
  }
}
