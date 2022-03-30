package ru.yandex.spark.yt.e2e

import scala.concurrent.duration.Duration

case class E2ETestCase(name: String,
                       executionTime: Duration,
                       keyColumns: Seq[String],
                       customInputPath: Option[String] = None,
                       uniqueKeys: Boolean = false,
                       conf: Map[String, String] = Map(
                         "spark.pyspark.python" -> "/opt/python3.7/bin/python3.7",
//                         "spark.eventLog.enabled" -> "true",
                         "spark.yt.read.keyColumnsFilterPushdown.enabled" -> "true"
                       )) {
  val userDirPath: String = s"${SubmitTest.userDirPath}/$name"
  val basePath: String = s"${SubmitTest.basePath}/$name"

  def jobPath: String = s"yt:/$userDirPath/job.py"

  def inputPath: String = customInputPath.getOrElse(s"$basePath/input")

  def outputPath: String = s"$userDirPath/output"

  def expectedPath: String = s"$basePath/expected"

  def checkResultPath: String = s"${SubmitTest.basePath.drop(1)}/$name/check_result"

  def withConf(key: String, value: String): E2ETestCase = {
    copy(conf = conf + (key -> value))
  }
}
