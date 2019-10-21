package ru.yandex.spark.yt

import org.apache.spark.sql.{RuntimeConfig, SparkSession}
import ru.yandex.spark.yt
import ru.yandex.spark.yt.format.GlobalTableOptions

object PythonUtils {
  def restartSparkWithExtensions(spark: SparkSession): SparkSession = yt.restartSparkWithExtensions(spark)

  def setPathFilesCount(path: String, filesCount: Int): Unit = {
    GlobalTableOptions.setFilesCount(path, filesCount)
  }
}
