package ru.yandex.spark.yt.format.conf

import org.apache.spark.sql.SparkSession

case class FilterPushdownConfig(enabled: Boolean,
                                unionEnabled: Boolean,
                                ytPathCountLimit: Int)

object FilterPushdownConfig {
  def apply(spark: SparkSession): FilterPushdownConfig = {
    import ru.yandex.spark.yt.format.conf.{SparkYtConfiguration => SparkSettings}
    import ru.yandex.spark.yt.fs.conf._
    FilterPushdownConfig(
      enabled = spark.ytConf(SparkSettings.Read.KeyColumnsFilterPushdown.Enabled),
      unionEnabled = spark.ytConf(SparkSettings.Read.KeyColumnsFilterPushdown.UnionEnabled),
      ytPathCountLimit = spark.ytConf(SparkSettings.Read.KeyColumnsFilterPushdown.YtPathCountLimit)
    )
  }
}
