package tech.ytsaurus.spyt.serializers

import org.apache.spark.sql.SparkSession

case class SchemaConverterConfig(parsingTypeV3: Boolean)

object SchemaConverterConfig {
  def apply(spark: SparkSession): SchemaConverterConfig = {
    import tech.ytsaurus.spyt.format.conf.{SparkYtConfiguration => SparkSettings}
    import tech.ytsaurus.spyt.fs.conf._
    SchemaConverterConfig(
      parsingTypeV3 = spark.ytConf(SparkSettings.Read.ParsingTypeV3)
    )
  }
}

