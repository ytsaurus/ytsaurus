package ru.yandex.spark.yt.test

import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}
import org.apache.spark.sql.{DataFrame, SparkSession, Strategy}
import org.scalatest.TestSuite
import ru.yandex.spark.yt.fs.YtClientConfigurationConverter._
import ru.yandex.spark.yt.wrapper.YtWrapper
import ru.yandex.spark.yt.wrapper.client.YtRpcClient

import scala.annotation.tailrec
import org.apache.spark.sql.internal.SQLConf.FILE_COMMIT_PROTOCOL_CLASS

trait LocalSpark extends LocalYtClient {
  self: TestSuite =>
  System.setProperty("io.netty.tryReflectionSetAccessible", "true")

  def numExecutors: Int = 4

  def sparkConf: SparkConf = new SparkConf()
    .set("fs.ytTable.impl", "ru.yandex.spark.yt.fs.YtTableFileSystem")
    .set("fs.yt.impl", "ru.yandex.spark.yt.fs.YtFileSystem")
    .set("fs.defaultFS", "ytTable:///")
    .set("spark.hadoop.yt.proxy", "localhost:8000")
    .set("spark.hadoop.yt.user", "root")
    .set("spark.hadoop.yt.token", "")
    .set("spark.hadoop.yt.timeout", "300")
    .set("spark.yt.write.batchSize", "10")
    .set(FILE_COMMIT_PROTOCOL_CLASS.key, "ru.yandex.spark.yt.format.YtOutputCommitter")
    .set("spark.ui.enabled", "false")
    .set("spark.hadoop.yt.read.arrow.enabled", "true")
    .set("spark.sql.adaptive.enabled", "true")

  def plannerStrategy: SparkStrategy = {
    val plannerStrategyClass = "ru.yandex.spark.yt.format.YtSourceStrategy"
    val loader = getClass.getClassLoader
    val cls = loader.loadClass(plannerStrategyClass)
    cls.getConstructor().newInstance().asInstanceOf[Strategy]
  }

  lazy val spark: SparkSession = SparkSession.builder()
    .master(s"local[$numExecutors]")
    .config(sparkConf)
    .withExtensions(_.injectPlannerStrategy(_ => plannerStrategy))
    .getOrCreate()

  override lazy val ytClient: YtRpcClient = YtWrapper.createRpcClient("test", ytClientConfiguration(spark))

  def physicalPlan(df: DataFrame): SparkPlan = {
    spark.sessionState.executePlan(df.queryExecution.logical)
      .executedPlan
  }

  def adaptivePlan(df: DataFrame): SparkPlan = {
    val plan = physicalPlan(df).asInstanceOf[AdaptiveSparkPlanExec]
    plan.execute()
    plan.executedPlan
  }

  def nodes(plan: SparkPlan): Seq[SparkPlan] = {
    @tailrec
    def inner(res: Seq[SparkPlan], queue: Seq[SparkPlan]): Seq[SparkPlan] = {
      queue match {
        case h :: t =>
          val children = h.children
          inner(res ++ children, t ++ children)
        case Nil => res
      }
    }

    inner(Seq(plan), Seq(plan))
  }
}
