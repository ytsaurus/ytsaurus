package ru.yandex.spark.yt.common.utils

case class AbstractSegment[T <: Ordered[T]](left: T, right: T)

object AbstractSegment {
  sealed trait SegmentSide

  object SegmentSide {
    case object Begin extends SegmentSide
    case object End extends SegmentSide
  }

  def union[T <: Ordered[T]](array: Seq[Seq[AbstractSegment[T]]]): Seq[AbstractSegment[T]] =
    getCovered(array, 1)

  def intercept[T <: Ordered[T]](array: Seq[Seq[AbstractSegment[T]]]): Seq[AbstractSegment[T]] =
    getCovered(array, array.length)

  def getCovered[T <: Ordered[T]](array: Seq[Seq[AbstractSegment[T]]], limit: Int): Seq[AbstractSegment[T]] = {
    val coverage = calculateCoverage(array)
    unionNeighbourSegments(
      coverage
        .collect {
          case (segment, coverage) if coverage >= limit => segment
        }
    )
  }

  private[utils] def getAllPoints[T <: Ordered[T]](array: Seq[Seq[AbstractSegment[T]]]): Map[T, Map[SegmentSide, Int]] = {
    val duplicatedPoints = array.flatten.foldLeft(List.empty[(T, SegmentSide)]) {
      case (points, segment) =>
        (segment.left, SegmentSide.Begin) +: (segment.right, SegmentSide.End) +: points
    }
    duplicatedPoints.groupBy { case (point, _) => point }
      .mapValues(singlePointList => singlePointList.groupBy { case (_, kind) => kind }.mapValues(_.length))
  }

  private[utils] def calculateCoverage[T <: Ordered[T]](array: Seq[Seq[AbstractSegment[T]]]): Seq[(AbstractSegment[T], Int)] = {
    val importantPointsMap = getAllPoints(array)
    val importantPoints = importantPointsMap.keys.toList.sorted
    if (importantPoints.isEmpty) {
      Nil
    } else {
      val (_, _, reversedResult) = importantPoints.foldLeft((Option.empty[T], 0, List.empty[(AbstractSegment[T], Int)])) {
        case ((previousPoint, beforePointCoverage, result), currentPoint) =>
          val inPointCoverage = beforePointCoverage + importantPointsMap(currentPoint)
            .getOrElse(SegmentSide.Begin, 0)
          val afterPointCoverage = inPointCoverage - importantPointsMap(currentPoint)
            .getOrElse(SegmentSide.End, 0)
          val lastResult = previousPoint match {
            case Some(value) => (AbstractSegment(value, currentPoint), beforePointCoverage) +: result
            case None => result
          }
          (Some(currentPoint), afterPointCoverage,
            (AbstractSegment(currentPoint, currentPoint), inPointCoverage) +: lastResult)
      }
      reversedResult.reverse
    }
  }

  private[utils] def unionNeighbourSegments[T <: Ordered[T]](array: Seq[AbstractSegment[T]]): Seq[AbstractSegment[T]] = {
    array.foldLeft(List.empty[AbstractSegment[T]]) {
      case (result, segment) =>
        result match {
          case Nil => List(segment)
          case head :: tail =>
            if (head.right == segment.left) {
              AbstractSegment(head.left, segment.right) :: tail
            } else {
              segment :: result
            }
        }
    }.reverse
  }
}
