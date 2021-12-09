package ru.yandex.spark.yt.serializers

import org.apache.spark.sql.SparkSession

case class SchemaConverterConfig(parsingTypeV3: Boolean)

object SchemaConverterConfig {
  def apply(spark: SparkSession): SchemaConverterConfig = {
    import ru.yandex.spark.yt.format.conf.{SparkYtConfiguration => SparkSettings}
    import ru.yandex.spark.yt.fs.conf._
    SchemaConverterConfig(
      parsingTypeV3 = spark.ytConf(SparkSettings.Read.ParsingTypeV3)
    )
  }
}

