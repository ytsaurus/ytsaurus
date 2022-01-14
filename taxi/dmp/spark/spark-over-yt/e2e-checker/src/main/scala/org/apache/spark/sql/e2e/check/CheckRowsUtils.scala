package org.apache.spark.sql.e2e.check

import org.apache.spark.sql.types._

object CheckRowsUtils {
  def checkRows(actual: Seq[Any], expected: Seq[Any], schema: Seq[StructField]): Boolean = {
    actual.zip(expected).zip(schema).forall {
      case ((a, e), f) => check(a, e, f)
    }
  }

  def check(actual: Any, expected: Any, f: StructField): Boolean = {
    if (actual == null && expected == null) {
      true
    } else if (actual == null || expected == null) {
      false
    } else {
      f.dataType match {
        case _: DoubleType =>
          val va = actual.asInstanceOf[Double]
          val ve = expected.asInstanceOf[Double]
          Math.abs(va - ve) < 0.0001
        case _: FloatType =>
          val va = actual.asInstanceOf[Float]
          val ve = expected.asInstanceOf[Float]
          Math.abs(va - ve) < 0.0001
        case _: AtomicType => actual == expected
        case ArrayType(StringType, _) => actual.asInstanceOf[Seq[String]].sorted == expected.asInstanceOf[Seq[String]].sorted
        case MapType(StringType, LongType, _) => actual.asInstanceOf[Map[String, Long]] == expected.asInstanceOf[Map[String, Long]]
        case MapType(StringType, DoubleType, _) =>
          val ma = actual.asInstanceOf[Map[String, Double]]
          val me = expected.asInstanceOf[Map[String, Double]]
          ma.forall { case (k, va) =>
            me.get(k).exists(ve => Math.abs(va - ve) < 0.0001)
          }
      }
    }
  }
}
