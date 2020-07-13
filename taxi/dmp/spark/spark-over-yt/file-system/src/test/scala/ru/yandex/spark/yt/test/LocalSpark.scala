package ru.yandex.spark.yt.test

import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.SparkStrategy
import org.apache.spark.sql.{SparkSession, Strategy}
import org.scalatest.TestSuite
import ru.yandex.spark.yt.fs.YtClientConfigurationConverter._
import ru.yandex.spark.yt.wrapper.YtWrapper
import ru.yandex.spark.yt.wrapper.client.YtRpcClient

trait LocalSpark extends LocalYtClient {
  self: TestSuite =>

  def sparkConf: SparkConf = new SparkConf()
    .set("fs.ytTable.impl", "ru.yandex.spark.yt.fs.YtTableFileSystem")
    .set("fs.defaultFS", "ytTable:///")
    .set("spark.hadoop.yt.proxy", "localhost:8000")
    .set("spark.hadoop.yt.user", "root")
    .set("spark.hadoop.yt.token", "")
    .set("spark.hadoop.yt.timeout", "300")
    .set("spark.yt.write.batchSize", "10")
    .set("spark.sql.sources.commitProtocolClass", "ru.yandex.spark.yt.format.YtOutputCommitter")
    .set("spark.ui.enabled", "false")
    .set("spark.hadoop.yt.read.arrow.enabled", "true")

  def plannerStrategy: SparkStrategy = {
    val plannerStrategyClass = "ru.yandex.spark.yt.format.YtSourceStrategy"
    val loader = getClass.getClassLoader
    val cls = loader.loadClass(plannerStrategyClass)
    cls.getConstructor().newInstance().asInstanceOf[Strategy]
  }

  lazy val spark: SparkSession = SparkSession.builder()
    .master("local[4]")
    .config(sparkConf)
    .withExtensions(_.injectPlannerStrategy(_ => plannerStrategy))
    .getOrCreate()

  override lazy val ytClient: YtRpcClient = YtWrapper.createRpcClient(ytClientConfiguration(spark))
}
