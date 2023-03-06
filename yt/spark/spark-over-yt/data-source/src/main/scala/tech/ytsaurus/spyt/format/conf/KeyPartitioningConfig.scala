package tech.ytsaurus.spyt.format.conf

import org.apache.spark.sql.SparkSession

case class KeyPartitioningConfig(enabled: Boolean,
                                 unionLimit: Int)

object KeyPartitioningConfig {
  def apply(spark: SparkSession): KeyPartitioningConfig = {
    import tech.ytsaurus.spyt.fs.conf._
    KeyPartitioningConfig(
      enabled = spark.ytConf(SparkYtConfiguration.Read.KeyPartitioning.Enabled),
      unionLimit = spark.ytConf(SparkYtConfiguration.Read.KeyPartitioning.UnionLimit)
    )
  }
}
