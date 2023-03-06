package tech.ytsaurus.spyt.common.utils

sealed trait Point extends Ordered[Point]

case class MInfinity() extends Point {
  override def compare(that: Point): Int = that match {
    case MInfinity() => 0
    case _ => -1
  }
}

case class RealValue[T](value: T)(implicit ord: Ordering[T]) extends Point {
  override def compare(that: Point): Int = that match {
    case MInfinity() => 1
    case a: RealValue[T] =>
      val another = a.value
      if (value == null && another == null) {
        0
      } else if (value == null) {
        -1
      } else if (another == null) {
        1
      } else {
        ord.compare(value, another)
      }
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
