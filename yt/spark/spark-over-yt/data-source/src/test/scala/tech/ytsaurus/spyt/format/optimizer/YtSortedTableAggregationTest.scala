package tech.ytsaurus.spyt.format.optimizer

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.FakeSortShuffleExchangeExec
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.mockito.scalatest.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}
import tech.ytsaurus.spyt._
import tech.ytsaurus.spyt.format.conf.SparkYtConfiguration
import tech.ytsaurus.spyt.test.{DynTableTestUtils, LocalSpark, TmpDir}

import java.util.UUID
import scala.language.postfixOps

class YtSortedTableAggregationTest extends FlatSpec with Matchers with LocalSpark
  with TmpDir with MockitoSugar with DynTableTestUtils {
  behavior of "YtInputSplit"

  import spark.implicits._

  // 1Kb ~ 200 rows with 2 long numbers
  private val conf = Map(
    s"spark.yt.${SparkYtConfiguration.Read.KeyPartitioning.Enabled.name}" -> "true",
    s"spark.yt.${SparkYtConfiguration.Read.KeyPartitioning.UnionLimit.name}" -> "2",
    s"spark.yt.${SparkYtConfiguration.Read.PlanOptimizationEnabled.name}" -> "true",
    "spark.sql.adaptive.enabled" -> "false",
    "spark.sql.files.maxPartitionBytes" -> "1Kb",
    "spark.yt.minPartitionBytes" -> "1Kb"
  )

  // (1L to 2000L).map(x => (x / 10, x / 10)).toDF("a", "b").write.sortedBy("a", "b")
  private val commonTable = s"$tmpPath-common-${UUID.randomUUID()}"

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.experimental.extraOptimizations = Seq(new YtSortedTableMarkerRule(spark))

    // creating common table for speed boosting
    val data = (1L to 2000L).map(x => (x / 10, x / 10))
    val df = data.toDF("a", "b")
    df.write.sortedBy("a", "b").yt(commonTable)
  }

  override def afterAll(): Unit = {
    spark.experimental.extraOptimizations = Nil
    super.afterAll()
  }

  private def findRealShuffles(query: DataFrame): Seq[ShuffleExchangeExec] = {
    val realShuffles = query.queryExecution.executedPlan.collect {
      case s: ShuffleExchangeExec => s
    }
    realShuffles
  }

  private def findFakeShuffles(query: DataFrame): Seq[FakeSortShuffleExchangeExec] = {
    val fakeShuffles = query.queryExecution.executedPlan.collect {
      case s: FakeSortShuffleExchangeExec => s
    }
    fakeShuffles
  }

  it should "be disabled when key partitioning didn't work" in {
    withConfs(conf) {
      // union limit is 2, but here 4 partitions was merged
      val data = (1L to 2000L).map(x => (x / 1000, x / 200))
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
      val res = spark.read.yt(commonTable).groupBy("a").count()
      res.collect()

      findFakeShuffles(res).length shouldBe 1
      findRealShuffles(res).length shouldBe 0
    }
  }

  it should "be disabled when config is disabled" in {
    withConfs(conf) {
      withConf(SparkYtConfiguration.Read.PlanOptimizationEnabled, false) {
        val res = spark.read.yt(commonTable).groupBy("a").count()
        res.collect()

        findFakeShuffles(res).length shouldBe 0
        findRealShuffles(res).length shouldBe 1
      }
    }
  }

  it should "work on with scan optimized table" in {
    withConfs(conf) {
      val tmpPath2 = tmpPath + "3"
      val data = (1L to 2000L).map(x => (x / 10, x / 10))
      val df = data
        .toDF("a", "b")
      df.write.sortedBy("a", "b").optimizeFor("scan").yt(tmpPath2)

      val res = spark.read.yt(tmpPath2).groupBy("a").count()
      res.collect()

      findFakeShuffles(res).length shouldBe 1
      findRealShuffles(res).length shouldBe 0
    }
  }

  it should "work on dynamic table" in {
    withConfs(conf) {
      val tmpPath2 = tmpPath + "4"
      prepareTestTable(tmpPath2, testData, Seq(Seq(), Seq(3), Seq(6)))

      val res = spark.read.yt(tmpPath2).groupBy("a").count()
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
      df.write.sortedBy("a", "b").yt(tmpPath2)

      val res = spark.read.yt(commonTable).groupBy("a").count()
        .unionAll(spark.read.yt(tmpPath2).groupBy("a").count())
      res.collect()

      findFakeShuffles(res).length shouldBe 2
      findRealShuffles(res).length shouldBe 0
    }
  }

  it should "use optimized number of shuffles" ignore {
    withConfs(conf) {
      val res = spark.read.yt(commonTable).groupBy("a").count()
        .unionAll(spark.read.yt(commonTable).groupBy("a").count())
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
      val res = spark.read.yt(commonTable).repartition(5).groupBy("a").count()
      res.collect()

      findFakeShuffles(res).length shouldBe 0
      findRealShuffles(res).length shouldBe 2
    }
  }

  it should "be disabled on indirect aggregation" in {
    withConfs(conf) {
      val res = spark.read.yt(commonTable).unionAll(spark.read.yt(commonTable)).groupBy("a").count()
      res.collect()

      findFakeShuffles(res).length shouldBe 0
      findRealShuffles(res).length shouldBe 1
    }
  }
}
