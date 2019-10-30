package ru.yandex.spark.yt.conf

object SparkYtInternalConfiguration {
  private val prefix = "internal"

  case object Transaction extends StringConfigEntry(s"$prefix.transaction")
  case object GlobalTransaction extends StringConfigEntry(s"$prefix.globalTransaction")
}
