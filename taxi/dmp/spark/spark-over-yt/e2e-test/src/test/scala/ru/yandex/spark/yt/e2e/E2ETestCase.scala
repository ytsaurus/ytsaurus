package ru.yandex.spark.yt.e2e

case class E2ETestCase(name: String,
                       keyColumns: Seq[String],
                       uniqueKeys: Boolean = false,
                       conf: Map[String, String] = Map(
                         "spark.pyspark.python" -> "/opt/python3.7/bin/python3.7",
//                         "spark.eventLog.enabled" -> "true",
                         "spark.yt.read.keyColumnsFilterPushdown.enabled" -> "true"
                       )) {
  private val basePath = "//home/spark/e2e"

  def jobPath: String = s"yt:/$basePath/$name/job.py"

  def outputPath: String = s"$basePath/$name/output"

  def expectedPath: String = s"$basePath/$name/expected"

  def checkResultPath: String = s"${basePath.drop(1)}/$name/check_result"

  def withConf(key: String, value: String): E2ETestCase = {
    copy(conf = conf + (key -> value))
  }
}
