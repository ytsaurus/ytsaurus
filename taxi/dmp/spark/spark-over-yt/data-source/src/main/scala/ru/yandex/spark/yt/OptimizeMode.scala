package ru.yandex.spark.yt

sealed abstract class OptimizeMode(val name: String)

object OptimizeMode {
  case object Scan extends OptimizeMode("scan")
  case object Lookup extends OptimizeMode("lookup")
}
