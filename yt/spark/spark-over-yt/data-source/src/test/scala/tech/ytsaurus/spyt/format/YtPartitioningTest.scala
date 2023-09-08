package tech.ytsaurus.spyt.format

import org.apache.spark.sql.Row
import org.apache.spark.sql.v2.Utils.getParsedKeys
import org.scalatest.{FlatSpec, Matchers}
import tech.ytsaurus.core.tables.{ColumnValueType, TableSchema}
import tech.ytsaurus.spyt._
import tech.ytsaurus.spyt.format.conf.SparkYtConfiguration
import tech.ytsaurus.spyt.test._
import tech.ytsaurus.spyt.wrapper.YtWrapper

import scala.language.postfixOps
import scala.util.Random

class YtPartitioningTest extends FlatSpec with Matchers with LocalSpark with TmpDir with TestUtils with DynTableTestUtils {
  import spark.implicits._

  private val atomicSchema = new TableSchema.Builder()
    .setUniqueKeys(false)
    .addValue("a", ColumnValueType.INT64)
    .addValue("b", ColumnValueType.STRING)
    .addValue("c", ColumnValueType.DOUBLE)
    .build()

  private val testBigTablePath = f"${tmpPath}_YtPartitioningTest2"
  private val testDataBigTable = (1 to 5000).map(i => TestRow(i, i * 2, ('a'.toInt + i).toChar.toString))

  // 1Kb ~ 60 rows with 2 long numbers
  private val conf = Map(
    s"spark.yt.${SparkYtConfiguration.Read.YtPartitioningEnabled.name}" -> "true",
    "spark.sql.adaptive.enabled" -> "false",
    "spark.sql.files.maxPartitionBytes" -> "1Kb",
    "spark.yt.minPartitionBytes" -> "1Kb"
  )

  override def beforeAll(): Unit = {
    super.beforeAll()
    prepareTestTable(testBigTablePath, testDataBigTable, Seq(Seq(), Seq(500)))
  }

  override def afterAll(): Unit = {
    YtWrapper.remove(testBigTablePath)
    super.afterAll()
  }

  it should "use chunk partitioning when disabled" in {
    val dfNoOpt = spark.read.yt(testBigTablePath)
    val keys = getParsedKeys(dfNoOpt)
    keys.length shouldBe 2
    dfNoOpt.selectAs[TestRow].collect() should contain theSameElementsAs testDataBigTable
  }

  it should "read static table with no chunk" in {
    createEmptyTable(tmpPath, atomicSchema)

    withConfs(conf) {
      val res = spark.read.yt(tmpPath)

      res.columns should contain theSameElementsAs Seq("a", "b", "c")
      res.select("a", "b", "c").collect() should contain theSameElementsAs Seq()
    }
  }

  it should "read static table with single chunk" in {
    writeTableFromYson(Seq(
      """{a = 1; b = "a"; c = 0.3}""",
      """{a = 2; b = "b"; c = 0.5}"""
    ), tmpPath, atomicSchema)

    withConfs(conf) {
      val res = spark.read.yt(tmpPath)

      res.columns should contain theSameElementsAs Seq("a", "b", "c")
      res.select("a", "b", "c").collect() should contain theSameElementsAs Seq(
        Row(1, "a", 0.3),
        Row(2, "b", 0.5)
      )
    }
  }

  it should "read static table with many chunks" in {
    val rng = new Random(0)
    val data = List.fill(1000)(rng.nextInt(200))

    withConfs(conf) {
      val df = data.toDF("a")
      df.repartition(10)
        .write.yt(tmpPath)

      val res = spark.read.yt(tmpPath)

      val filtered = res.filter(res("a") <= 55 && res("a") > 10)
      filtered.collect() should contain theSameElementsAs data
        .filter { a => a <= 55 && a > 10 }
        .map { Row(_) }
    }
  }

  it should "read dynamic table with no chunk" in {
    withConfs(conf) {
      prepareTestTable(tmpPath, Nil, Nil)

      val df = spark.read.yt(tmpPath)
      df.selectAs[TestRow].collect() should contain theSameElementsAs Seq()
    }
  }

  it should "read dynamic table with single chunk" in {
    withConfs(conf) {
      prepareTestTable(tmpPath, testData, Nil)

      val df = spark.read.yt(tmpPath)
      df.selectAs[TestRow].collect() should contain theSameElementsAs testData
    }
  }

  it should "read heavy dynamic table" in {
    withConfs(conf) {
      val dfWithOpt = spark.read.yt(testBigTablePath)
      val keys = getParsedKeys(dfWithOpt)
      keys.length should be > 2
      dfWithOpt.selectAs[TestRow].collect() should contain theSameElementsAs testDataBigTable
    }
  }

  it should "support complex queries for dynamic tables" in {
    withConfs(conf) {
      val df = spark.read.yt(testBigTablePath).filter("b < 100").select("a")
      val keys = getParsedKeys(df)
      keys.length should be > 2
      val correctResult = testDataBigTable.filter(t => t.b < 100).map(t => t.a)
      df.collect().map(_.getLong(0)) should contain theSameElementsAs correctResult
    }
  }
}
