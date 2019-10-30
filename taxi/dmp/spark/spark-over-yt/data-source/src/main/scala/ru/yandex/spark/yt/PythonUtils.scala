package ru.yandex.spark.yt

import org.apache.spark.sql.SparkSession
import ru.yandex.spark.yt.format.GlobalTableSettings

object PythonUtils {
  def restartSparkWithExtensions(spark: SparkSession): SparkSession =
    ru.yandex.spark.yt.restartSparkWithExtensions(spark)

  def setPathFilesCount(path: String, filesCount: Int): Unit = {
    GlobalTableSettings.setFilesCount(path, filesCount)
  }
}
