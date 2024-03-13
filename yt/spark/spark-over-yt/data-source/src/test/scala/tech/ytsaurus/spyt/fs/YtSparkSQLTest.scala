package tech.ytsaurus.spyt.fs

import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.types.{ArrayType, LongType, MapType, StringType}
import org.apache.spark.sql.yson.YsonBinary
import org.apache.spark.test.UtilsWrapper
import org.mockito.scalatest.MockitoSugar
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers, PrivateMethodTester}
import tech.ytsaurus.core.tables.{ColumnValueType, TableSchema}
import tech.ytsaurus.spyt._
import tech.ytsaurus.spyt.serialization.YsonEncoder
import tech.ytsaurus.spyt.test.{DynTableTestUtils, LocalSpark, LocalYt, TestRow, TestUtils, TmpDir}
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.wrapper.table.OptimizeMode

import scala.language.postfixOps

class YtSparkSQLTest extends FlatSpec with Matchers with LocalSpark with TmpDir
  with TestUtils with MockitoSugar with TableDrivenPropertyChecks with PrivateMethodTester  with DynTableTestUtils {
  import spark.implicits._

  private val atomicSchema = TableSchema.builder()
    .setUniqueKeys(false)
    .addValue("a", ColumnValueType.INT64)
    .addValue("b", ColumnValueType.STRING)
    .addValue("c", ColumnValueType.DOUBLE)
    .build()

  private val anotherSchema = TableSchema.builder()
    .addValue("a", ColumnValueType.INT64)
    .addValue("d", ColumnValueType.STRING)
    .build()

  private val complexSchema = TableSchema.builder()
    .addValue("array", ColumnValueType.ANY)
    .addValue("map", ColumnValueType.ANY)
    .build()

  private val testModes = Table(
    "optimizeFor",
    OptimizeMode.Scan,
    OptimizeMode.Lookup
  )

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set("spark.sql.caseSensitive", value = true)
  }

  override def afterEach(): Unit = {
    spark.sessionState.catalog.invalidateAllCachedTables()
    super.afterEach()
  }

  override def afterAll(): Unit = {
    spark.conf.set("spark.sql.caseSensitive", value = false)
    super.afterAll()
  }

  it should "select rows using views" in {
    YtWrapper.createDir(tmpPath)
    forAll(testModes) { optimizeFor =>
      val path = s"$tmpPath/${optimizeFor.name}"
      writeTableFromYson(Seq(
        """{a = 1; b = "a"; c = 0.3}""",
        """{a = 2; b = "b"; c = 0.5}"""
      ), path, atomicSchema, optimizeFor)

      val table = spark.read.yt(path)
      table.createOrReplaceTempView("table")
      val res = spark.sql(s"SELECT * FROM table")

      res.columns should contain theSameElementsAs Seq("a", "b", "c")
      res.select("a", "b", "c").collect() should contain theSameElementsAs Seq(
        Row(1, "a", 0.3),
        Row(2, "b", 0.5)
      )
    }
  }

  it should "select rows" in {
    YtWrapper.createDir(tmpPath)
    forAll(testModes) { optimizeFor =>
      val path = s"$tmpPath/${optimizeFor.name}"
      writeTableFromYson(Seq(
        """{a = 1; b = "a"; c = 0.3}""",
        """{a = 2; b = "b"; c = 0.5}"""
      ), path, atomicSchema, optimizeFor)

      val res = spark.sql(s"SELECT * FROM yt.`ytTable:/$path`")

      res.columns should contain theSameElementsAs Seq("a", "b", "c")
      res.select("a", "b", "c").collect() should contain theSameElementsAs Seq(
        Row(1, "a", 0.3),
        Row(2, "b", 0.5)
      )
    }
  }

  it should "select rows in complex table" in {
    val data = Seq("""{array = [1; 2; 3]; map = {k1 = "a"; k2 = "b"}}""")
    val correctResult = Array(Seq(
      YsonEncoder.encode(List(1L, 2L, 3L), ArrayType(LongType), false).toList,
      YsonEncoder.encode(Map("k1" -> "a", "k2" -> "b"), MapType(StringType, StringType), false).toList
    ))

    YtWrapper.createDir(tmpPath)
    forAll(testModes) { optimizeFor =>
      val path = s"$tmpPath/${optimizeFor.name}"
      writeTableFromYson(data, path, complexSchema, optimizeFor)

      val df = spark.sql(s"SELECT * FROM yt.`ytTable:/$path`")
      df.columns should contain theSameElementsAs Seq("array", "map")

      val res = df.select("array", "map").collect()
        .map(row => row.toSeq.map(value => value.asInstanceOf[YsonBinary].bytes.toList))
      res should contain theSameElementsAs correctResult
    }
  }

  it should "filter rows" in {
    YtWrapper.createDir(tmpPath)
    forAll(testModes) { optimizeFor =>
      val path = s"$tmpPath/${optimizeFor.name}"
      writeTableFromYson(Seq(
        """{a = 1; b = "a"; c = 0.3}""",
        """{a = 2; b = "b"; c = 0.5}""",
        """{a = 3; b = "c"; c = 1.0}"""
      ), path, atomicSchema, optimizeFor)

      val res = spark.sql(s"SELECT * FROM yt.`ytTable:/$path` WHERE a > 1")
      res.collect() should contain theSameElementsAs Seq(
        Row(2, "b", 0.5),
        Row(3, "c", 1.0)
      )
    }
  }

  it should "read without specified scheme" in {
    writeTableFromYson(Seq(
      """{a = 1; b = "a"; c = 0.3}""",
      """{a = 2; b = "b"; c = 0.5}"""
    ), tmpPath, atomicSchema)

    withConf("fs.null.impl", "tech.ytsaurus.spyt.fs.YtTableFileSystem") {
      val res = spark.sql(s"SELECT * FROM yt.`$tmpPath`")
      res.collect() should contain theSameElementsAs Seq(
        Row(1, "a", 0.3),
        Row(2, "b", 0.5)
      )
    }
  }

  it should "sort rows" in {
    YtWrapper.createDir(tmpPath)
    forAll(testModes) { optimizeFor =>
      val path = s"$tmpPath/${optimizeFor.name}"
      writeTableFromYson(Seq(
        """{a = 1; b = "a"; c = 0.3}""",
        """{a = 3; b = "c"; c = 1.0}""",
        """{a = 2; b = "b"; c = 0.5}"""
      ), path, atomicSchema, optimizeFor)

      val res = spark.sql(s"SELECT * FROM yt.`ytTable:/$path` ORDER BY a DESC")
      res.collect() shouldBe Seq(
        Row(3, "c", 1.0),
        Row(2, "b", 0.5),
        Row(1, "a", 0.3)
      )
    }
  }

  it should "group rows" in {
    YtWrapper.createDir(tmpPath)
    forAll(testModes) { optimizeFor =>
      val path = s"$tmpPath/${optimizeFor.name}"
      writeTableFromYson(Seq(
        """{a = 1; b = "a"; c = 0.3}""",
        """{a = 1; b = "b"; c = 0.5}""",
        """{a = 2; b = "c"; c = 0.0}"""
      ), path, atomicSchema, optimizeFor)

      val res = spark.sql(s"SELECT a, COUNT(*) FROM yt.`ytTable:/$path` GROUP BY a")
      res.collect() should contain theSameElementsAs Seq(
        Row(1, 2),
        Row(2, 1)
      )
    }
  }

  it should "join tables" in {
    YtWrapper.createDir(tmpPath)
    forAll(testModes) { optimizeFor =>
      val path1 = s"$tmpPath/${optimizeFor.name}_1"
      writeTableFromYson(Seq(
        """{a = 2; b = "b"; c = 0.5}""",
        """{a = 2; b = "c"; c = 0.0}"""
      ), path1, atomicSchema, optimizeFor)

      val path2 = s"$tmpPath/${optimizeFor.name}_2"
      writeTableFromYson(Seq(
        """{a = 2; d = "hello"}""",
        """{a = 2; d = "ytsaurus"}""",
        """{a = 3; d = "spark"}"""
      ), path2, anotherSchema, optimizeFor)

      val res = spark.sql(
        s"""
           |SELECT t1.a, t2.d
           |FROM yt.`ytTable:/$path1` t1
           |INNER JOIN yt.`ytTable:/$path2` t2 ON t1.a == t2.a""".stripMargin
      )
      res.collect() should contain theSameElementsAs Seq(
        Row(2, "hello"), Row(2, "ytsaurus"),
        Row(2, "hello"), Row(2, "ytsaurus"),
      )
    }
  }

  it should "select from dynamic table" in {
    prepareTestTable(tmpPath, testData, Seq(Seq(), Seq(3), Seq(6, 12)))
    // @latest_version is required
    // otherwise it will be appended to path in runtime and fail because of nested "directory" reading
    val res = spark.sql(s"SELECT * FROM yt.`ytTable:/$tmpPath/@latest_version`")
    res.columns should contain theSameElementsAs Seq("a", "b", "c")
    res.select("a", "b", "c").selectAs[TestRow].collect() should contain theSameElementsAs testData
  }

  it should "join static table with dynamic one" in {
    val path1 = s"$tmpPath/dynamic"
    prepareTestTable(path1, testData, Seq(Seq(), Seq(3), Seq(6, 12)))

    val path2 = s"$tmpPath/static"
    writeTableFromYson(Seq(
      """{a = 5; b = "13"; c = 0.0}""",
      """{a = 6; b = "11"; c = 0.0}""",
      """{a = 5; b = "10"; c = 0.0}"""
    ), path2, atomicSchema)

    val res = spark.sql(
      s"""
         |SELECT t1.a, t2.b
         |FROM yt.`ytTable:/$path1/@latest_version` t1
         |INNER JOIN yt.`ytTable:/$path2` t2
         |ON t1.a == t2.a AND t1.b != CAST(t2.b AS INT)""".stripMargin
    )
    res.columns should contain theSameElementsAs Seq("a", "b")
    res.collect() should contain theSameElementsAs Seq(
      Row(5, "13"),
      Row(6, "11")
    )
  }

  it should "create table" in {
    spark.sql(s"CREATE TABLE yt.`ytTable:/$tmpPath` (id INT, name STRING, age INT) USING yt")
    val res = spark.read.yt(tmpPath)
    res.columns should contain theSameElementsAs Seq("id", "name", "age")
    res.collect() should contain theSameElementsAs Seq()

    a[AnalysisException] shouldBe thrownBy {
      spark.sql(s"CREATE TABLE yt.`ytTable:/$tmpPath` (id INT, name STRING, age INT) USING yt")
    }
  }

  it should "create table as select" in {
    spark.sql(s"CREATE TABLE yt.`ytTable:/$tmpPath` USING yt AS SELECT col1, col2 FROM VALUES (1, false)")
    val res = spark.read.yt(tmpPath)
    res.columns should contain theSameElementsAs Seq("col1", "col2")
    res.collect() should contain theSameElementsAs Seq(Row(1, false))
  }

  it should "drop table" in {
    spark.sql(s"CREATE TABLE yt.`ytTable:/$tmpPath` (id INT, age INT) USING yt")
    YtWrapper.exists(tmpPath) shouldBe true

    spark.sql(s"DROP TABLE yt.`ytTable:/$tmpPath`")
    YtWrapper.exists(tmpPath) shouldBe false

    a[AnalysisException] shouldBe thrownBy {
      spark.sql(s"DROP TABLE yt.`ytTable:/$tmpPath`")
    }

    spark.sql(s"DROP TABLE IF EXISTS yt.`ytTable:/$tmpPath`")
  }

  it should "insert to table" in {
    YtWrapper.createDir(tmpPath)
    val path = s"$tmpPath/original"
    val path2 = s"$tmpPath/copy"
    writeTableFromYson(Seq(
      """{a = 1; d = "a"}""",
      """{a = 2; d = "b"}"""
    ), path, anotherSchema)

    a[AnalysisException] shouldBe thrownBy {
      spark.sql(s"INSERT INTO TABLE yt.`ytTable:/$path2` SELECT * FROM yt.`ytTable:/$path`")
    }

    spark.sql(s"CREATE TABLE yt.`ytTable:/$path2` (a INT, d STRING) USING yt")
    spark.sql(s"INSERT INTO TABLE yt.`ytTable:/$path2` SELECT * FROM yt.`ytTable:/$path`")
    spark.sql(s"INSERT INTO TABLE yt.`ytTable:/$path2`(a, d) VALUES (3, 'c')")

    val res = spark.read.yt(path2)
    res.columns should contain theSameElementsAs Seq("a", "d")
    res.collect() should contain theSameElementsAs Seq(Row(1, "a"), Row(2, "b"), Row(3, "c"))

    spark.sql(s"INSERT OVERWRITE TABLE yt.`ytTable:/$path2`(a, d) VALUES (4, 'd')")
    val res2 = spark.read.yt(path2)
    res2.collect() should contain theSameElementsAs Seq(Row(4, "d"))

    a[AnalysisException] shouldBe thrownBy {
      spark.sql(s"INSERT OVERWRITE TABLE yt.`ytTable:/$path2`(c1) VALUES (0l)")
    }
  }

  it should "refresh when modified externally" in {
    writeTableFromYson(Seq(
      """{a = 1; b = "qwe"; c = 0.0}""",
    ), tmpPath, atomicSchema)
    val res = spark.sql(s"SELECT * FROM yt.`ytTable:/$tmpPath`")
    res.collect() shouldBe Seq(Row(1, "qwe", 0.0))

    YtWrapper.remove(tmpPath)
    writeTableFromYson(Seq(
      """{a = 1; d = "str1"}""",
      """{a = 2; d = "str2"}"""
    ), tmpPath, anotherSchema)
    spark.sql(s"REFRESH TABLE yt.`ytTable:/$tmpPath`")
    val res2 = spark.sql(s"SELECT * FROM yt.`ytTable:/$tmpPath`")
    res2.collect() shouldBe Seq(Row(1, "str1"), Row(2, "str2"))
  }

  it should "count io statistics" in {
    val customPath = "ytTable:/" + tmpPath
    val data = Stream.from(1).take(1000)

    val store = UtilsWrapper.appStatusStore(spark)
    val stagesBefore = store.stageList(null)
    val totalInputBefore = stagesBefore.map(_.inputBytes).sum
    val totalOutputBefore = stagesBefore.map(_.outputBytes).sum

    data.toDF().coalesce(1).write.yt(customPath)
    val allRows = spark.sql(s"SELECT * FROM yt.`ytTable:/${tmpPath}`").collect()
    allRows should have size data.length

    val stages = store.stageList(null)
    val totalInput = stages.map(_.inputBytes).sum
    val totalOutput = stages.map(_.outputBytes).sum

    totalInput should be > totalInputBefore
    totalOutput should be > totalOutputBefore

  }
}
