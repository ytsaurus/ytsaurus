package ru.yandex.spark.yt.common.utils

case class TuplePoint(points: Seq[Point]) extends Ordered[TuplePoint] {
  override def compare(that: TuplePoint): Int = {
    val diff0 = points.zip(that.points).map { case (a, b) => a.compare(b) }.find(_ != 0)
    diff0 match {
      case Some(value) => value
      case None => points.length.compare(that.points.length)
    }
  }
}

object TupleSegment {
  type TupleSegment = AbstractSegment[TuplePoint]

  def apply(left: TuplePoint, right: TuplePoint): TupleSegment = new TupleSegment(left, right)
}
