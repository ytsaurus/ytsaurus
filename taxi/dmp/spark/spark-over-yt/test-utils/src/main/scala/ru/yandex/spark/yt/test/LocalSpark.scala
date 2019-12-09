package ru.yandex.spark.yt.test

import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.SparkStrategy
import org.apache.spark.sql.{SparkSession, Strategy}
import ru.yandex.spark.yt.fs.YtClientConfigurationConverter
import ru.yandex.spark.yt.utils.YtClientUtils
import ru.yandex.yt.ytclient.proxy.YtClient

trait LocalSpark {
  def sparkConf: SparkConf = new SparkConf()
    .set("fs.yt.impl", "ru.yandex.spark.yt.fs.YtFileSystem")
    .set("fs.defaultFS", "yt:///")
    .set("spark.hadoop.yt.proxy", "localhost:8000")
    .set("spark.hadoop.yt.user", "root")
    .set("spark.hadoop.yt.token", "")
    .set("spark.hadoop.yt.timeout", "300")
    .set("spark.yt.write.batchSize", "10")
    .set("spark.sql.sources.commitProtocolClass", "ru.yandex.spark.yt.format.YtOutputCommitter")
    .set("spark.ui.enabled", "false")

  def plannerStrategy: SparkStrategy = {
    val plannerStrategyClass = "ru.yandex.spark.yt.format.YtSourceStrategy"
    val loader = getClass.getClassLoader
    val cls = loader.loadClass(plannerStrategyClass)
    cls.getConstructor().newInstance().asInstanceOf[Strategy]
  }

  lazy val spark: SparkSession = SparkSession.builder()
    .master("local[2]")
    .config(sparkConf)
    .withExtensions(_.injectPlannerStrategy(_ => plannerStrategy))
    .getOrCreate()

  implicit val ytClient: YtClient = YtClientUtils.createRpcClient(YtClientConfigurationConverter(spark)).yt
}
