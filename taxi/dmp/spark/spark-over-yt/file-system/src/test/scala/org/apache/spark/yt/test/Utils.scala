package org.apache.spark.yt.test

import org.apache.spark.internal.config.ConfigEntry
import org.apache.spark.sql.SparkSession

object Utils {
  type SparkConfigEntry[T] = ConfigEntry[T]

  def defaultParallelism(spark: SparkSession): Int = spark.sparkContext.defaultParallelism
}
