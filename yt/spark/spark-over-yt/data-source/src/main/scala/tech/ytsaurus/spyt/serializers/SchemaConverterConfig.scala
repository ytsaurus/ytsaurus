package tech.ytsaurus.spyt.serializers

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import tech.ytsaurus.spyt.format.conf.SparkYtConfiguration
import tech.ytsaurus.spyt.fs.conf.{SparkYtSparkConf, SparkYtSparkSession}

case class SchemaConverterConfig(parsingTypeV3: Boolean)

object SchemaConverterConfig {
  def apply(spark: SparkSession): SchemaConverterConfig = {
    SchemaConverterConfig(parsingTypeV3 = spark.ytConf(SparkYtConfiguration.Read.TypeV3))
  }

  def apply(conf: SparkConf): SchemaConverterConfig = {
    SchemaConverterConfig(parsingTypeV3 = conf.ytConf(SparkYtConfiguration.Read.TypeV3))
  }
}

