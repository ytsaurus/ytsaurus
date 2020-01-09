package ru.yandex.spark.yt.format

import org.apache.spark.sql.SparkSessionExtensions

class YtSparkExtensions extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectPlannerStrategy(_ => new YtSourceStrategy())
  }
}
