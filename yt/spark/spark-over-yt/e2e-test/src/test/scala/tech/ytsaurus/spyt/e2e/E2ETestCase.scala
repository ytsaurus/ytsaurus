package tech.ytsaurus.spyt.e2e

import scala.concurrent.duration.Duration

case class E2ETestCase(name: String,
                       executionTime: Duration,
                       keyColumns: Seq[String],
                       customInputPath: Option[String] = None,
                       uniqueKeys: Boolean = false,
                       conf: Map[String, String] = Map(
                         "spark.yt.read.keyPartitioningSortedTables.enabled" -> "true",
                         "spark.yt.read.keyPartitioningSortedTables.unionLimit" -> "2",
                         "spark.yt.read.planOptimization.enabled" -> "true",
                         "spark.pyspark.python" -> "/opt/python3.7/bin/python3.7",
                         "spark.eventLog.enabled" -> "true",
                         "spark.yt.read.keyColumnsFilterPushdown.enabled" -> "true",
                         "spark.context.listeners" ->
                           "tech.ytsaurus.spyt.format.GlobalTransactionSparkListener,tech.ytsaurus.spyt.format.ExtraOptimizationsSparkListener"
                       )) {
  val userDirPath: String = s"${SubmitTest.userDirPath}/scala/$name"
  val basePath: String = s"${SubmitTest.basePath}/$name"

  def jobPath: String = s"yt:/${SubmitTest.userDirPath}/scripts/$name.py"

  def inputPath: String = customInputPath.getOrElse(s"$basePath/input")

  def outputPath: String = s"$userDirPath/output"

  def expectedPath: String = s"$basePath/expected"

  def checkResultPath: String = s"${userDirPath.drop(1)}/check_result"

  def withConf(key: String, value: String): E2ETestCase = {
    copy(conf = conf + (key -> value))
  }
}
