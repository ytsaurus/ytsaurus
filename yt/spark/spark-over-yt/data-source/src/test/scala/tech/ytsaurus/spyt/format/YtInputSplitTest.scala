package tech.ytsaurus.spyt.format

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.sources._
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.mockito.scalatest.MockitoSugar
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}
import tech.ytsaurus.spyt._
import tech.ytsaurus.spyt.common.utils.ExpressionTransformer.expressionToSegmentSet
import tech.ytsaurus.spyt.common.utils._
import tech.ytsaurus.spyt.format.YtInputSplit.{getKeyFilterSegments, pushdownFiltersToYPath}
import tech.ytsaurus.spyt.format.conf.SparkYtConfiguration
import tech.ytsaurus.spyt.fs.path.YPathEnriched.ypath
import tech.ytsaurus.spyt.test._
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.wrapper.table.OptimizeMode
import tech.ytsaurus.core.tables.{ColumnValueType, TableSchema}
import tech.ytsaurus.spyt.common.utils.{MInfinity, PInfinity, RealValue, Segment, SegmentSet}
import tech.ytsaurus.spyt.format.conf.FilterPushdownConfig
import tech.ytsaurus.spyt.test.{DynTableTestUtils, TestRow}

import scala.language.postfixOps
import scala.util.Random

class YtInputSplitTest extends FlatSpec with Matchers with LocalSpark with DynTableTestUtils
  with TmpDir with TestUtils with MockitoSugar with TableDrivenPropertyChecks {
  behavior of "YtInputSplit"

  import spark.implicits._

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(s"spark.yt.${SparkYtConfiguration.Read.KeyColumnsFilterPushdown.Enabled.name}", value = true)
    spark.conf.set(s"spark.yt.${SparkYtConfiguration.Read.KeyColumnsFilterPushdown.YtPathCountLimit.name}", value = 10)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    spark.conf.set(s"spark.yt.${SparkYtConfiguration.Read.KeyColumnsFilterPushdown.Enabled.name}", value = false)
  }

  it should "create SegmentSet from Filter" in {
    val a1 = LessThan("a", 5L)
    val a2 = GreaterThan("a", 3L)
    val b = GreaterThanOrEqual("b", 2L)

    val a1SS = expressionToSegmentSet(a1)
    val a2SS = expressionToSegmentSet(a2)
    val bSS = expressionToSegmentSet(b)
    val union = expressionToSegmentSet(Or(Or(a1, a2), b))
    val intercept = expressionToSegmentSet(And(And(a1, a2), b))
    a1SS shouldBe SegmentSet("a", Segment(MInfinity(), RealValue(5L)))
    a2SS shouldBe SegmentSet("a", Segment(RealValue(3L), PInfinity()))
    bSS shouldBe SegmentSet("b", Segment(RealValue(2L), PInfinity()))
    union shouldBe SegmentSet()
    intercept shouldBe
      SegmentSet(
        Map(
          ("a", List(Segment(RealValue(3L), RealValue(5L)))),
          ("b", List(Segment(RealValue(2L), PInfinity()))),
        )
      )
  }

  it should "push compatible filters" in {
    writeTableFromYson(Seq(
      """{c = 1; a = 1}"""
    ), tmpPath, TableSchema.builder()
      .setUniqueKeys(false)
      .addKey("c", ColumnValueType.INT64)
      .addKey("a", ColumnValueType.INT64)
      .build()
    )
    val df = spark.read.yt(tmpPath)

    val test = Seq(
      (
        df("a").isin(1L, 2L, 3L) && df("c") === 1L,
        Seq(
          In("a", Array(1, 2, 3)),
          In("c", Array(1))
        )
      ), (
        df("a") === 1L || df("a") === 4L || df("a") < 3L,
        Seq(
          Or(In("a", Array(4)), LessThanOrEqual("a", 3))
        )
      ), (
        df("a") === 1L || df("c") === 2L,
        Seq()
      )
    )

    test.foreach {
      case (input, output) =>
        val query = df.filter(input)

        val plan = query.queryExecution.logical

        val res = getPushedFilters(plan)
        res should contain theSameElementsAs output
    }
  }

  it should "read with pushed data filters" in {
    val data = (1L to 1000L).map(x => (x, x % 2, 0L))
    val df = data
      .toDF("a", "b", "c")
      .coalesce(2)

    df.write.sortedBy("a", "b", "c").yt(tmpPath)

    val res = spark.read.yt(tmpPath)
    val test = Seq(
      (
        (res("a") <= 50 && res("a") >= 50 - 1) && res("b") === 1L,
        data.filter { case (a, b, c) => a >= 49 && a <= 50 && b == 1 }
      ), (
        res("a") >= 77L && res("b").isin(0L) && res("c") === 0L,
        data.filter { case (a, b, c) => a >= 77 && b == 0 && c == 0 }
      ), (
        res("a") === 1L || res("b") === 2L,
        data.filter { case (a, b, c) => a == 1 || b == 2 }
      )
    )
    test.foreach {
      case (input, output) =>
        res.filter(input).collect() should contain theSameElementsAs output.map(Row.fromTuple)
    }
  }

  it should "read with pushed random range filters" in {
    val rng = new Random(0)
    val data = List.fill(1000)(rng.nextInt()).sorted
    val df = data
      .toDF("a")
      .coalesce(4)

    df.write.sortedBy("a").yt(tmpPath)

    val res = spark.read.yt(tmpPath)

    (1 to 100).foreach {
      _ =>
        val aL = rng.nextInt()
        val aH = rng.nextInt()
        val filtered = res.filter(res("a") <= aH && res("a") > aL)
        filtered.collect() should contain theSameElementsAs data
          .filter { a => a <= aH && a > aL }
          .map {
            Row(_)
          }
    }
  }

  it should "read with pushed random isin filters" in {
    val rng = new Random(0)
    val data = List.fill(1000)(rng.nextInt(100)).sorted
    val df = data
      .toDF("a")

    df.write.sortedBy("a").yt(tmpPath)

    val res = spark.read.yt(tmpPath)

    (1 to 100).foreach {
      _ =>
        val isin = (1 to 10).map(_ => rng.nextInt(100))
        res.filter(res("a").isin(isin: _*)).collect() should contain theSameElementsAs data
          .filter { a => isin.contains(a) }
          .map {
            Row(_)
          }
    }
  }

  it should "reduce number of read rows" in {
    val data = 1L to 1000L
    val df = data
      .toDF("a")
      .coalesce(2)

    df.write.sortedBy("a").yt(tmpPath)

    val res = spark.read.yt(tmpPath)
    val test = Seq(
      (res("a") <= 50, 60L),
      (res("a") >= 123L && res("a") % 2 === 0, 900L),
      (res("a") === 1L || res("a").isin(5L, 10L, 15L, 20L), 30L),
      (res("a") < 10 && res("a") > 20, 0L)
    )
    test.foreach {
      case (filter, rowLimit) =>
        val numOutputRows = getNumOutputRows(res, filter)
        numOutputRows should be <= rowLimit
    }
  }

  it should "push double filters" in {
    val rng = new Random(0)
    val data = (1L to 1000L).map(x => (x / 100).doubleValue())
      .map(x => (x, x + rng.nextDouble(), x * rng.nextDouble())).sorted
    // [0..10]
    val df = data
      .toDF("a", "b", "c")
      .coalesce(3)

    df.write.sortedBy("a", "b", "c").yt(tmpPath)

    val res = spark.read.yt(tmpPath)
    val test = Seq(
      (
        (res("a") <= 7.0 || res("a") >= 7.0) &&
          (res("b") >= 6.0 && res("b") >= 5.0) &&
          res("c") < 5.0,
        data.filter { case (a, b, c) => b >= 6.0 && c < 5.0 }
      ), (
        res("c") < 5.0,
        data.filter { case (a, b, c) => c < 5.0 }
      )
    )
    test.foreach {
      case (input, output) =>
        res.filter(input).count() shouldBe output.size
        res.filter(input).collect() should contain theSameElementsAs output.map(Row.fromTuple)
    }
  }

  it should "push string filters" in {
    val data = (10L to 98L)
      .map(x => ((x + 1).toString, x.toString, x))
    // [0..10]
    val df = data
      .toDF("a", "b", "c")
      .coalesce(3)

    df.write.sortedBy("a", "b").yt(tmpPath)

    val res = spark.read.disableArrow.yt(tmpPath)
    val test = Seq(
      (
        res("b") <= "20",
        data.filter { case (_, _, c) => c <= 20 }
      ), (
        res("a") > "10" && res("b") <= "20",
        data.filter { case (_, _, c) => c <= 20 && c >= 10 }
      )
    )
    test.foreach {
      case (input, output) =>
        res.filter(input).count() shouldBe output.size
        res.filter(input).collect() should contain theSameElementsAs output.map(Row.fromTuple)
    }
  }

  it should "not duplicate data" in {
    val data = Seq((0, 1), (1, 5), (2, 1))
    val df = data
      .toDF("a", "b")
      .coalesce(3)

    df.write.sortedBy("a", "b").yt(tmpPath)

    val res = spark.read.yt(tmpPath)
    val test = Seq(
      (res("b") === 1 || res("b") === 5, data)
    )
    test.foreach {
      case (input, output) =>
        res.filter(input).collect() should contain theSameElementsAs output.map(Row.fromTuple)
    }
  }

  it should "support null filters" in {
    val data = Seq((null, 1), (null, 7), ("0", 1), ("1", 5), ("2", 1))
    data.toDF("a", "b").write.sortedBy("a", "b").yt(tmpPath)

    val res = spark.read.yt(tmpPath)
    val test = Seq(
      (res("a").isNotNull, data.filter { case (s, _) => s != null }),
      (res("a").isNull || res("a") > "1", data.filter { case (s, _) => s == null || s > "1" }),
      (res("a") < "1", data.filter { case (s, _) => s != null && s < "1" }),
    )
    test.foreach {
      case (input, output) =>
        res.filter(input).collect() should contain theSameElementsAs output.map(Row.fromTuple)
    }
  }

  private val segmentMInfTo5 = Segment(MInfinity(), RealValue(5L))
  private val segment2To20 = Segment(RealValue(2L), RealValue(20L))
  private val segment10To30 = Segment(RealValue(10L), RealValue(30L))
  private val segment15ToPInf = Segment(RealValue(15L), PInfinity())

  private val exampleSet1 = SegmentSet(Map(
    ("a", List(segmentMInfTo5, segment15ToPInf)),
    ("b", List(segment2To20))))
  private val exampleSet2 = SegmentSet(Map(
    ("a", List(segment2To20)),
    ("b", List(segment10To30)),
    ("c", List(segment10To30))))

  it should "get key filter segments" in {
    val res1 = getKeyFilterSegments(exampleSet1, List("a", "b"), 5)
    res1 should contain theSameElementsAs Seq(
      Seq(("a", segmentMInfTo5), ("b", segment2To20)),
      Seq(("a", segment15ToPInf), ("b", segment2To20))
    )

    val res2 = getKeyFilterSegments(exampleSet2, List("c", "a"), 5)
    res2 should contain theSameElementsAs Seq(
      Seq(("c", segment10To30), ("a", segment2To20))
    )

    val res3 = getKeyFilterSegments(exampleSet2, List("z"), 5)
    res3 should contain theSameElementsAs Seq(Seq())

    val res4 = getKeyFilterSegments(exampleSet1, List("b", "a"), 1)
    res4 should contain theSameElementsAs Seq(
      Seq(("b", segment2To20))
    )
  }

  it should "get ypath" in {
    val keyColumns = List("a", "b")
    val file = YtPartitionedFile.static("//dir/path", 2, 5, 10)
    val baseYPath = file.ypath.withColumns(keyColumns: _*)
    val config = FilterPushdownConfig(enabled = true, unionEnabled = true, ytPathCountLimit = 5)
    pushdownFiltersToYPath(single = false, exampleSet1, keyColumns.map(Some(_)), config, baseYPath).toString shouldBe
      """<"ranges"=
        |[{"lower_limit"={"row_index"=2;"key"=[<"type"="min";>#;2;];};
        |"upper_limit"={"row_index"=5;"key"=[5;20;<"type"="max";>#;];};};
        |{"lower_limit"={"row_index"=2;"key"=[15;2;];};
        |"upper_limit"={"row_index"=5;"key"=[<"type"="max";>#;];};};];
        |"columns"=["a";"b";];>//dir/path""".stripMargin.replaceAll("\n", "")

    pushdownFiltersToYPath(single = true, exampleSet1, keyColumns.map(Some(_)), config, baseYPath).toString shouldBe
      """<"ranges"=
        |[{"lower_limit"={"row_index"=2;"key"=[<"type"="min";>#;2;];};
        |"upper_limit"={"row_index"=5;"key"=[<"type"="max";>#;];};};];
        |"columns"=["a";"b";];>//dir/path""".stripMargin.replaceAll("\n", "")
  }

  private val atomicSchema = TableSchema.builder()
    .setUniqueKeys(false)
    .addKey("a", ColumnValueType.INT64)
    .addKey("b", ColumnValueType.STRING)
    .addValue("c", ColumnValueType.DOUBLE)
    .build()

  it should "read directory" in {
    YtWrapper.createDir(tmpPath)
    val table1 = s"$tmpPath/t1"
    val table2 = s"$tmpPath/t2"
    writeTableFromYson(Seq(
      """{a = 1; b = "a"; c = 0.3}""",
      """{a = 2; b = "b"; c = 0.5}"""
    ), table1, atomicSchema)
    writeTableFromYson(Seq(
      """{a = 3; b = "c"; c = 0.1}"""
    ), table2, atomicSchema)

    val df = spark.read.yt(tmpPath)

    val query = df.filter(col("a") >= 2)

    val plan = query.queryExecution.logical

    val res = getPushedFilters(plan)
    res should contain theSameElementsAs Seq(GreaterThanOrEqual("a", 2))

    query.collect() should contain theSameElementsAs Seq(
      Row(2, "b", 0.5),
      Row(3, "c", 0.1)
    )
  }

  it should "support vectorized reader" in {
    val data = 1L to 10000L
    val df = data
      .toDF("a")

    df.write.sortedBy("a").optimizeFor(OptimizeMode.Scan).yt(tmpPath)

    spark.read.enableArrow.yt(tmpPath)
      .filter(col("a") === 0)
      .count() shouldBe 0

    spark.read.enableArrow.yt(tmpPath)
      .filter(col("a") === 2)
      .count() shouldBe 1
  }

  it should "read filtered dynamic tables" in {
    val data = (1L to 1000L).map(x => (x / 10, x % 10, 0.toString))
    prepareTestTable(tmpPath,
      data.map { case (a, b, c) => TestRow(a, b, c) }, Seq(Seq(), Seq(6, 0), Seq(7, 0), Seq(50), Seq(80, 0)))

    val res = spark.read.option("enable_inconsistent_read", "true").yt(tmpPath)
    val test = Seq(
      (
        (res("a") <= 50 && res("a") >= 50 - 1) && res("b") === 1L,
        data.filter { case (a, b, c) => a >= 49 && a <= 50 && b == 1 }
      ), (
        res("a") >= 77L && res("b").isin(0L) && res("c") === "0",
        data.filter { case (a, b, c) => a >= 77 && b == 0 && c == "0" }
      ), (
        res("a") === 1L || res("b") === 2L,
        data.filter { case (a, b, c) => a == 1 || b == 2 }
      ), (
        res("b") === 1L || res("b") === 0,
        data.filter { case (a, b, c) => b == 1 || b == 0 }
      ), (
        res("a") < 10 || res("a") > 20,
        data.filter { case (a, b, c) => a < 10 || a > 20 }
      ), (
        res("a") < 10 && res("a") > 20,
        data.filter { case (a, b, c) => a < 10 && a > 20 }
      ), (
        res("a") < 50 && res("c") < "1",
        data.filter { case (a, b, c) => a < 50 && c < "1" }
      ), (
        res("c") === "0",
        data.filter { case (a, b, c) => c == "0" }
      ), (
        res("a") <= 6,
        data.filter { case (a, b, c) => a <= 6 }
      ), (
        res("a") === 6,
        data.filter { case (a, b, c) => a == 6 }
      )
    )
    test.foreach {
      case (input, output) =>
        res.filter(input).count() shouldBe output.size
        res.filter(input).collect() should contain theSameElementsAs output.map(Row.fromTuple)
    }
  }

  it should "reduce number of read rows in dynamic tables" in {
    val data = (1L to 1000L).map(x => (x / 10, x % 10, 0.toString))
    prepareTestTable(tmpPath,
      data.map { case (a, b, c) => TestRow(a, b, c) }, Seq(Seq(), Seq(6, 0), Seq(7, 0), Seq(50), Seq(80, 0)))

    val res = spark.read.option("enable_inconsistent_read", "true").yt(tmpPath)
    val test = Seq(
      (res("a") <= 50 && res("a") >= 50 - 1 && res("b") === 1L, 20L),
      (res("a") >= 77L && res("b").isin(0L) && res("c") === "0", 300L),
      (res("a") === 6, 20L),
      (res("a") < 10 || res("a") > 20, 950L),
      (res("a") < 10 && res("a") > 20, 0L),
      (res("a") < 50 && res("c") < "1", 550L),
      (res("a").isin(10, 20, 30, 49), 50L)
    )
    test.foreach {
      case (filter, rowLimit) =>
        val numOutputRows = getNumOutputRows(res, filter)
        numOutputRows should be <= rowLimit
    }
  }

  private def getNumOutputRows(res: DataFrame, filter: Column): Long = {
    val query = res.filter(filter)
    query.collect()
    query.queryExecution.executedPlan.collectFirst {
      case b@BatchScanExec(_, _, _) => b.metrics("numOutputRows").value
    }.get
  }
}

