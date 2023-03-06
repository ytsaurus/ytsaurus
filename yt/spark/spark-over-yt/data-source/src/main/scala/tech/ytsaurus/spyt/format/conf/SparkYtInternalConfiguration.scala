package tech.ytsaurus.spyt.format.conf

import tech.ytsaurus.spyt.fs.conf.StringConfigEntry

object SparkYtInternalConfiguration {
  private val prefix = "internal"

  case object Transaction extends StringConfigEntry(s"$prefix.transaction")

  case object GlobalTransaction extends StringConfigEntry(s"$prefix.globalTransaction")

}
