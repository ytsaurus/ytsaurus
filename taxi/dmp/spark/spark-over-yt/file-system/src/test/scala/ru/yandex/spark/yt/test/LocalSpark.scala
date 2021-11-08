package ru.yandex.spark.yt.test

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, SubqueryExpression}
import org.apache.spark.sql.catalyst.planning.ScanOperation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Implicits.TableHelper
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, PushDownUtils}
import org.apache.spark.sql.internal.SQLConf.FILE_COMMIT_PROTOCOL_CLASS
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.yt.test.Utils
import org.apache.spark.yt.test.Utils.{SparkConfigEntry, defaultConfValue}
import org.scalatest.TestSuite
import ru.yandex.spark.yt.fs.YtClientConfigurationConverter._
import ru.yandex.spark.yt.test.LocalSpark.defaultSparkConf
import ru.yandex.spark.yt.wrapper.client.{YtClientProvider, YtRpcClient}

import scala.annotation.tailrec

trait LocalSpark extends LocalYtClient {
  self: TestSuite =>
  System.setProperty("io.netty.tryReflectionSetAccessible", "true")

  def numExecutors: Int = 4

  def sparkConf: SparkConf = defaultSparkConf

  lazy val spark: SparkSession = {
    if (LocalSpark.spark != null) {
      LocalSpark.spark
    } else {
      LocalSpark.spark = SparkSession.builder()
        .master(s"local[$numExecutors]")
        .config(sparkConf)
        .getOrCreate()
      LocalSpark.spark
    }
  }

  def getPushedFilters(plan: LogicalPlan): Seq[Filter] = {
    (plan collectFirst {
      case ScanOperation(_, filters, relation: DataSourceV2Relation) =>
        val scanBuilder = relation.table.asReadable.newScanBuilder(relation.options)

        val normalizedFilters = filters.map { e =>
          e transform {
            case a: AttributeReference =>
              a.withName( relation.output.find(_.semanticEquals(a)).getOrElse(a).name)
          }
        }
        val (_, normalizedFiltersWithoutSubquery) = normalizedFilters.partition(SubqueryExpression.hasSubquery)

        val (pushedFilters, _) = PushDownUtils.pushFilters(scanBuilder, normalizedFiltersWithoutSubquery)

        pushedFilters
    }).getOrElse(Nil)
  }

  override protected def ytRpcClient: YtRpcClient = {
    YtClientProvider.ytRpcClient(ytClientConfiguration(spark), "test")
  }

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

  def withConf[T, R](entry: SparkConfigEntry[T], value: String)(f: => R): R = {
    withConf(entry.key, value, defaultConfValue(entry, LocalSpark.defaultSparkConf))(f)
  }

  def withConf[R](key: String, value: String, default: Option[String] = None)(f: => R): R = {
    spark.conf.set(key, value)
    try {
      f
    } finally {
      default match {
        case Some(value) => spark.conf.set(key, value)
        case None => spark.conf.unset(key)
      }
    }
  }

  def defaultParallelism: Int = Utils.defaultParallelism(spark)
}

object LocalSpark {
  private var spark: SparkSession = _

  def stop(): Unit = {
    SparkSession.getDefaultSession.foreach(_.stop())
    SparkSession.clearDefaultSession()
    spark = null
  }

  def maybeSetExtensions(sparkConf: SparkConf): SparkConf = {
    val extensionsClass = "ru.yandex.spark.yt.format.YtSparkExtensions"
    val loader = getClass.getClassLoader
    try {
      loader.loadClass(extensionsClass)
      sparkConf.set("spark.sql.extensions", "ru.yandex.spark.yt.format.YtSparkExtensions")
    } catch {
      case _: ClassNotFoundException => sparkConf
    }
  }

  val defaultSparkConf: SparkConf = maybeSetExtensions(new SparkConf()
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
    .set("spark.sql.adaptive.enabled", "true"))
}
