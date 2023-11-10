package tech.ytsaurus.spyt.format.conf

import tech.ytsaurus.spyt.fs.conf.ConfigEntry

object SparkYtInternalConfiguration {
  import ConfigEntry.implicits._
  private val prefix = "internal"

  case object Transaction extends ConfigEntry[String](s"$prefix.transaction")

  case object GlobalTransaction extends ConfigEntry[String](s"$prefix.globalTransaction")

}
