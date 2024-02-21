package tech.ytsaurus.spyt.format.optimizer

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.execution.exchange.{ReusedExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.execution.{DependentHashShuffleExchangeExec, FakeHashShuffleExchangeExec, FakeSortShuffleExchangeExec, SortExec, SparkPlan}
import org.apache.spark.sql.functions.{col, floor}
import org.apache.spark.sql.internal.SQLConf.{CODEGEN_FACTORY_MODE, WHOLESTAGE_CODEGEN_ENABLED}
import org.mockito.scalatest.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}
import tech.ytsaurus.spyt._
import tech.ytsaurus.spyt.format.conf.SparkYtConfiguration
import tech.ytsaurus.spyt.test.{DynTableTestUtils, LocalSpark, TmpDir}

import java.util.UUID
import scala.language.postfixOps

class YtSortedTableJoinTest extends FlatSpec with Matchers with LocalSpark with TmpDir
  with MockitoSugar with DynTableTestUtils {
  import spark.implicits._

  // 1Kb ~ 60 rows with 2 long numbers
  private val conf = Map(
    WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
    CODEGEN_FACTORY_MODE.key -> CodegenObjectFactoryMode.NO_CODEGEN.toString,
    s"spark.yt.${SparkYtConfiguration.Read.KeyPartitioning.Enabled.name}" -> "true",
    s"spark.yt.${SparkYtConfiguration.Read.KeyPartitioning.UnionLimit.name}" -> "2",
    s"spark.yt.${SparkYtConfiguration.Read.PlanOptimizationEnabled.name}" -> "true",
    "spark.sql.adaptive.enabled" -> "false",
    "spark.sql.autoBroadcastJoinThreshold" -> "-1",
    "spark.sql.files.maxPartitionBytes" -> "1Kb",
    "spark.yt.minPartitionBytes" -> "1Kb",
  )

  // (1L to 2000L).map(x => (x, x / 10)).toDF("a", "b").write.sortedBy("a", "b")
  private val commonTable = s"$tmpPath-common-${UUID.randomUUID()}"

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.experimental.extraOptimizations = Seq(new YtSortedTableMarkerRule(spark))

    // creating common table for speed boosting
    val data = (1L to 2000L).map(x => (x, x / 10))
    val df = data.toDF("a", "b")
    df.write.sortedBy("a", "b").yt(commonTable)
  }

  override def afterAll(): Unit = {
    spark.experimental.extraOptimizations = Nil
    super.afterAll()
  }

  private def isFakeHashShuffle(shuffle: SparkPlan): Boolean = shuffle match {
    case _: FakeHashShuffleExchangeExec => true
    case _ => false
  }

  private def isDependentHashShuffle(shuffle: SparkPlan): Boolean = shuffle match {
    case _: DependentHashShuffleExchangeExec => true
    case _ => false
  }

  private def isRealShuffle(shuffle: SparkPlan): Boolean = shuffle match {
    case _: DependentHashShuffleExchangeExec => false
    case _: FakeHashShuffleExchangeExec => false
    case _: ShuffleExchangeExec => true
    case _: ReusedExchangeExec => true
    case _ => false
  }

  private def findNotProcessedJoin(query: DataFrame): Seq[SortMergeJoinExec] = {
    val fakeShuffles = query.queryExecution.executedPlan.collect {
      case s@SortMergeJoinExec(_, _, _, _,
      SortExec(_, _, lS, _),
      SortExec(_, _, rS, _), _)
        if isRealShuffle(lS) && isRealShuffle(rS) =>
        s
    }
    fakeShuffles
  }

  private def findProcessedOneSideJoin(query: DataFrame): Seq[SortMergeJoinExec] = {
    val fakeShuffles = query.queryExecution.executedPlan.collect {
      case s@SortMergeJoinExec(_, _, _, _, lS, SortExec(_, _, rS, _), _)
        if isFakeHashShuffle(lS) && isDependentHashShuffle(rS) =>
        s
      case s@SortMergeJoinExec(_, _, _, _, SortExec(_, _, lS, _), rS, _)
        if isDependentHashShuffle(lS) && isFakeHashShuffle(rS) =>
        s
    }
    fakeShuffles
  }

  private def findProcessedBothSideJoin(query: DataFrame): Seq[SortMergeJoinExec] = {
    val fakeShuffles = query.queryExecution.executedPlan.collect {
      case s@SortMergeJoinExec(_, _, _, _, lS, rS, _)
        if isFakeHashShuffle(lS) && isFakeHashShuffle(rS) =>
        s
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

      val res = spark.read.yt(tmpPath).select("a").join(spark.read.yt(tmpPath), "a")
      res.collect()

      findNotProcessedJoin(res).length shouldBe 1
      findProcessedOneSideJoin(res).length shouldBe 0
      findProcessedBothSideJoin(res).length shouldBe 0
    }
  }

  it should "work on simple case" in {
    withConfs(conf) {
      val data = (1L to 2000L).map(x => (x / 10, x / 10, x / 10))

      val df = data
        .toDF("a", "b", "c")
      df.write.sortedBy("a", "b", "c").yt(tmpPath)

      val res = spark.read.yt(tmpPath).join(spark.read.yt(tmpPath), Seq("a", "b"))
      res.collect()

      findNotProcessedJoin(res).length shouldBe 0
      findProcessedOneSideJoin(res).length shouldBe 0
      findProcessedBothSideJoin(res).length shouldBe 1
    }
  }

  it should "work with filter" in {
    withConfs(conf) {
      val tmpPath2 = s"$tmpPath-${UUID.randomUUID()}"

      val data = (0L to 99L).map(x => (x / 10, x / 10, x))

      data.toDF("a", "b", "c").write.sortedBy("a", "b").yt(tmpPath)
      data.toDF("a", "b", "c").write.sortedBy("a", "b").yt(tmpPath2)

      val left = spark.read.yt(tmpPath)
      val right = spark.read.yt(tmpPath2)
      val query = left.join(right, Seq("a", "b")).filter(floor(left("c") / 10) - floor(right("c") / 10) === 0)
        .select(left("a"), left("b"), left("c"))
      val res = query.collect()

      val expected = (0L to 99L).flatMap(x => (0L to 9L).map(x2 => Seq(x / 10, x / 10, x / 10 * 10 + x2)))
      res should contain theSameElementsAs expected.map(Row.fromSeq)

      findNotProcessedJoin(query).length shouldBe 0
      findProcessedOneSideJoin(query).length shouldBe 0
      findProcessedBothSideJoin(query).length shouldBe 1
    }
  }

  it should "be disabled when config is disabled" in {
    withConfs(conf) {
      withConf(SparkYtConfiguration.Read.PlanOptimizationEnabled, false) {
        val data = (1L to 2000L).map(x => (x / 10, x / 10, x / 10))

        val df = data
          .toDF("a", "b", "c")
        df.write.sortedBy("a", "b", "c").yt(tmpPath)

        val res = spark.read.yt(tmpPath).join(spark.read.yt(tmpPath), Seq("a", "b"))
        res.collect()

        findNotProcessedJoin(res).length shouldBe 1
        findProcessedOneSideJoin(res).length shouldBe 0
        findProcessedBothSideJoin(res).length shouldBe 0
      }
    }
  }

  it should "work on several joins" in {
    withConfs(conf) {
      val res = spark.read.yt(commonTable).join(spark.read.yt(commonTable), Seq("a"))
        .unionAll(spark.read.yt(commonTable).join(spark.read.yt(commonTable), Seq("a")))
      res.collect()

      findNotProcessedJoin(res).length shouldBe 0
      findProcessedOneSideJoin(res).length shouldBe 0
      findProcessedBothSideJoin(res).length shouldBe 2
    }
  }

  it should "be disabled on unsorted data" in {
    withConfs(conf) {
      val data = (1L to 2000L).map(x => (x, x / 10))
      val df = data
        .toDF("a", "b")
      df.write.yt(tmpPath)

      val res = spark.read.yt(tmpPath).join(spark.read.yt(tmpPath), Seq("a"))
      res.collect()

      findNotProcessedJoin(res).length shouldBe 1
      findProcessedOneSideJoin(res).length shouldBe 0
      findProcessedBothSideJoin(res).length shouldBe 0
    }
  }

  it should "be disabled on repartitioned data" in {
    withConfs(conf) {
      val res = spark.read.yt(commonTable).repartition(5)
        .join(spark.read.yt(commonTable).repartition(5), Seq("a", "b"))
      res.collect()

      findNotProcessedJoin(res).length shouldBe 1
      findProcessedOneSideJoin(res).length shouldBe 0
      findProcessedBothSideJoin(res).length shouldBe 0
    }
  }

  it should "work when one half is sorted" in {
    withConfs(conf) {
      val tmpPath2 = s"$tmpPath-${UUID.randomUUID()}"

      val data = (1L to 2000L).map(x => (x, x / 10))
      val df = data
        .toDF("a", "b")
      df.write.yt(tmpPath2)

      val in1 = spark.read.yt(commonTable)
      val in2 = spark.read.yt(tmpPath2)
      val res = in1.join(in2, Seq("a"))
      res.collect()

      findNotProcessedJoin(res).length shouldBe 0
      findProcessedOneSideJoin(res).length shouldBe 1
      findProcessedBothSideJoin(res).length shouldBe 0
    }
  }

  it should "work when second table has reordered columns" in {
    withConfs(conf) {
      val tmpPath2 = s"$tmpPath-${UUID.randomUUID()}"

      val data = (1L to 2000L)
      data.map(x => (x, x)).toDF("c", "a").write.sortedBy("c", "a").yt(tmpPath2)

      val in1 = spark.read.yt(commonTable)
      val in2 = spark.read.yt(tmpPath2)
      val query = in1.join(in2, Seq("a")).select(in2("a"))
      val res = query.collect()
      res should contain theSameElementsAs data.map(Row(_))

      findNotProcessedJoin(query).length shouldBe 0
      findProcessedOneSideJoin(query).length shouldBe 1
      findProcessedBothSideJoin(query).length shouldBe 0
    }
  }

  it should "work when tables have renamed columns" in {
    withConfs(conf) {
      val tmpPath2 = s"$tmpPath-${UUID.randomUUID()}"

      (1L to 2000L).map(x => (x, x))
        .toDF("c", "d")
        .write.sortedBy("c", "d").yt(tmpPath2)

      val in1 = spark.read.yt(commonTable)
      val in2 = spark.read.yt(tmpPath2)
      val res = in1.join(in2,
        col("a") === col("c") and col("b") === col("d"))
      res.collect()

      findNotProcessedJoin(res).length shouldBe 0
      findProcessedOneSideJoin(res).length shouldBe 0
      findProcessedBothSideJoin(res).length shouldBe 1
    }
  }

  it should "work on indirect join" in {
    withConfs(conf) {
      val res = spark.read.yt(commonTable).unionAll(spark.read.yt(commonTable))
        .join(spark.read.yt(commonTable), Seq("a"))
      res.collect()

      findNotProcessedJoin(res).length shouldBe 0
      findProcessedOneSideJoin(res).length shouldBe 1
      findProcessedBothSideJoin(res).length shouldBe 0
    }
  }

  it should "work on dynamic table" in {
    withConfs(conf) {
      val tmpPath2 = tmpPath + "2"
      prepareTestTable(tmpPath2, testData, Seq(Seq(), Seq(3), Seq(6)))

      val staticTableReader = spark.read.yt(commonTable)
      val dynTableReader = spark.read.yt(tmpPath2).select("a", "b").toDF("a", "b_new")

      val res = staticTableReader.join(dynTableReader, "a")
      val collectedRes = res.collect()

      val expected = (1 to 10).map(i => Row(i, i / 10, i * 2))
      collectedRes should contain theSameElementsAs expected

      findNotProcessedJoin(res).length shouldBe 0
      findProcessedOneSideJoin(res).length shouldBe 0
      findProcessedBothSideJoin(res).length shouldBe 1
    }
  }
}
