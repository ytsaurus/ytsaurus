package ru.yandex.spark.yt.wrapper

import scala.concurrent.duration._

object Utils {
  def parseDuration(s: String): Duration = {
    val regex = """(\d+)(.*)""".r
    s match {
      case regex(amount, "") => amount.toInt.seconds
      case regex(amount, "s") => amount.toInt.seconds
      case regex(amount, "m") => amount.toInt.minutes
      case regex(amount, "min") => amount.toInt.minutes
      case regex(amount, "h") => amount.toInt.hours
      case regex(_, unit) => throw new IllegalArgumentException(s"Unknown time unit: $unit")
      case _ => throw new IllegalArgumentException(s"Illegal time format: $s")
    }
  }
}
