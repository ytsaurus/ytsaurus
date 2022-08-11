package ru.yandex.spark.yt.format

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.FakeSortedShuffleExchangeExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.mockito.scalatest.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}
import ru.yandex.spark.yt._
import ru.yandex.spark.yt.format.conf.SparkYtConfiguration
import ru.yandex.spark.yt.format.optimizer.YtSortedTableMarkerRule
import ru.yandex.spark.yt.test.{LocalSpark, TmpDir}

import scala.language.postfixOps

class YtSortedTableAggregationTest extends FlatSpec with Matchers with LocalSpark
  with TmpDir with MockitoSugar {
  behavior of "YtInputSplit"

  import spark.implicits._

  // 1Kb ~ 60 rows with 2 long numbers
  private val conf = Map(
    s"spark.yt.${SparkYtConfiguration.Read.KeyPartitioning.Enabled.name}" -> "true",
    s"spark.yt.${SparkYtConfiguration.Read.KeyPartitioning.UnionLimit.name}" -> "2",
    "spark.sql.adaptive.enabled" -> "false",
    "spark.sql.files.maxPartitionBytes" -> "1Kb",
    "spark.yt.minPartitionBytes" -> "1Kb"
  )

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.experimental.extraOptimizations = Seq(new YtSortedTableMarkerRule())
  }

  override def afterAll(): Unit = {
    spark.experimental.extraOptimizations = Nil
    super.afterAll()
  }

  private def findRealShuffles(query: DataFrame): Seq[ShuffleExchangeExec] = {
    val realShuffles = query.queryExecution.executedPlan.collect {
      case s: ShuffleExchangeExec if !s.isInstanceOf[FakeSortedShuffleExchangeExec] => s
    }
    realShuffles
  }

  private def findFakeShuffles(query: DataFrame): Seq[FakeSortedShuffleExchangeExec] = {
    val fakeShuffles = query.queryExecution.executedPlan.collect {
      case s: FakeSortedShuffleExchangeExec => s
    }
    fakeShuffles
  }

  it should "be disabled when key partitioning didn't work" in {
    withConfs(conf) {
      // union limit is 2, but here 4 partitions was merged
      val data = (1L to 2000L).map(x => (x / 200, x / 200))
      val df = data
        .toDF("a", "b")
      df.write.sortedBy("a", "b").yt(tmpPath)

      val res = spark.read.yt(tmpPath).groupBy("a").count()
      res.collect()

      findFakeShuffles(res).length shouldBe 0
      findRealShuffles(res).length shouldBe 1
    }
  }

  it should "work on simple case" in {
    withConfs(conf) {
      val data = (1L to 2000L).map(x => (x / 10, x / 10))

      val df = data
        .toDF("a", "b")
      df.write.sortedBy("a", "b").yt(tmpPath)

      val res = spark.read.yt(tmpPath).groupBy("a").count()
      res.collect()

      findFakeShuffles(res).length shouldBe 1
      findRealShuffles(res).length shouldBe 0
    }
  }

  it should "work on several aggregations" in {
    withConfs(conf) {
      val tmpPath2 = tmpPath + "2"
      val data = (1L to 2000L).map(x => (x / 10, x / 10))

      val df = data
        .toDF("a", "b")
      df.write.sortedBy("a", "b").yt(tmpPath)
      df.write.sortedBy("a", "b").yt(tmpPath2)

      val res = spark.read.yt(tmpPath).groupBy("a").count()
        .unionAll(spark.read.yt(tmpPath2).groupBy("a").count())
      res.collect()

      findFakeShuffles(res).length shouldBe 2
      findRealShuffles(res).length shouldBe 0
    }
  }

  it should "use optimized number of shuffles" in {
    withConfs(conf) {
      val data = (1L to 2000L).map(x => (x / 10, x / 10))

      val df = data
        .toDF("a", "b")
      df.write.sortedBy("a", "b").yt(tmpPath)

      val res = spark.read.yt(tmpPath).groupBy("a").count()
        .unionAll(spark.read.yt(tmpPath).groupBy("a").count())
      res.collect()

      findFakeShuffles(res).length shouldBe 1
      findRealShuffles(res).length shouldBe 0
    }
  }

  it should "be disabled on unsorted data" in {
    withConfs(conf) {
      val data = (1L to 2000L).map(x => (x / 10, x / 10))
      val df = data
        .toDF("a", "b")
      df.write.yt(tmpPath)

      val res = spark.read.yt(tmpPath).groupBy("a").count()
      res.collect()

      findFakeShuffles(res).length shouldBe 0
      findRealShuffles(res).length shouldBe 1
    }
  }

  it should "be disabled on repartitioned data" in {
    withConfs(conf) {
      val data = (1L to 2000L).map(x => (x / 10, x / 10))
      val df = data
        .toDF("a", "b")
      df.write.sortedBy("a", "b").yt(tmpPath)

      val res = spark.read.yt(tmpPath).repartition(5).groupBy("a").count()
      res.collect()

      findFakeShuffles(res).length shouldBe 0
      findRealShuffles(res).length shouldBe 2
    }
  }

  it should "be disabled on indirect aggregation" in {
    withConfs(conf) {
      val data = (1L to 2000L).map(x => (x / 10, x / 10))
      val df = data
        .toDF("a", "b")
      df.write.sortedBy("a", "b").yt(tmpPath)

      val res = spark.read.yt(tmpPath).unionAll(spark.read.yt(tmpPath)).groupBy("a").count()
      res.collect()

      findFakeShuffles(res).length shouldBe 0
      findRealShuffles(res).length shouldBe 1
    }
  }
}
