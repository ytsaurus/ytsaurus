package ru.yandex.spark.yt.format.conf

import org.apache.spark.sql.SparkSession

case class KeyPartitioningConfig(enabled: Boolean,
                                 unionLimit: Int)

object KeyPartitioningConfig {
  def apply(spark: SparkSession): KeyPartitioningConfig = {
    import ru.yandex.spark.yt.fs.conf._
    KeyPartitioningConfig(
      enabled = spark.ytConf(SparkYtConfiguration.Read.KeyPartitioning.Enabled),
      unionLimit = spark.ytConf(SparkYtConfiguration.Read.KeyPartitioning.UnionLimit)
    )
  }
}
