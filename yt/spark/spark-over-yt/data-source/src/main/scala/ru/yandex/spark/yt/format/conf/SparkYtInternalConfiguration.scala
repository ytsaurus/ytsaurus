package ru.yandex.spark.yt.format.conf

import ru.yandex.spark.yt.fs.conf.StringConfigEntry

object SparkYtInternalConfiguration {
  private val prefix = "internal"

  case object Transaction extends StringConfigEntry(s"$prefix.transaction")

  case object GlobalTransaction extends StringConfigEntry(s"$prefix.globalTransaction")

}
