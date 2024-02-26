package tech.ytsaurus.spyt.format

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkException
import org.apache.spark.sql.execution.InputAdapter
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf._
import org.apache.spark.sql.types._
import org.apache.spark.sql.yson.{UInt64Long, UInt64Type}
import org.apache.spark.sql.{AnalysisException, DataFrameReader, Row, SaveMode}
import org.apache.spark.status.api.v1
import org.apache.spark.test.UtilsWrapper
import org.mockito.scalatest.MockitoSugar
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers, PrivateMethodTester}
import tech.ytsaurus.spyt._
import tech.ytsaurus.spyt.test.{LocalSpark, TestUtils, TmpDir}
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.wrapper.table.OptimizeMode
import tech.ytsaurus.client.rows.{UnversionedRow, UnversionedValue}
import tech.ytsaurus.core.tables.{ColumnValueType, TableSchema}
import tech.ytsaurus.spyt.format.conf.SparkYtConfiguration
import tech.ytsaurus.typeinfo.TiType
import tech.ytsaurus.ysontree.YTree

import java.sql.{Date, Timestamp}
import java.time.LocalDate
import java.util.concurrent.TimeUnit
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Random

class YtFileFormatTest extends FlatSpec with Matchers with LocalSpark
  with TmpDir with TestUtils with MockitoSugar with TableDrivenPropertyChecks with PrivateMethodTester {
  behavior of "YtFileFormat"

  import spark.implicits._

  private val atomicSchema = TableSchema.builder()
    .setUniqueKeys(false)
    .addValue("a", ColumnValueType.INT64)
    .addValue("b", ColumnValueType.STRING)
    .addValue("c", ColumnValueType.DOUBLE)
    .build()

  private val anySchema = TableSchema.builder()
    .setUniqueKeys(false)
    .addValue("value", ColumnValueType.ANY)
    .build()

  it should "read dataset" in {
    writeTableFromYson(Seq(
      """{a = 1; b = "a"; c = 0.3}""",
      """{a = 2; b = "b"; c = 0.5}"""
    ), tmpPath, atomicSchema)

    val res = spark.read.yt(tmpPath)

    res.columns should contain theSameElementsAs Seq("a", "b", "c")
    res.select("a", "b", "c").collect() should contain theSameElementsAs Seq(
      Row(1, "a", 0.3),
      Row(2, "b", 0.5)
    )
  }

  it should "read partitioned dataset" in {
    YtWrapper.createDir(tmpPath)
    writeTableFromYson(Seq(
      """{a = 1; b = "a"; c = 0.3}""",
    ), tmpPath + "/d=11", atomicSchema)
    writeTableFromYson(Seq(
      """{a = 2; b = "b"; c = 0.5}""",
    ), tmpPath + "/d=22", atomicSchema)

    YtWrapper.listDir(tmpPath) should contain theSameElementsAs Seq("d=11", "d=22")

    val res0 = spark.read.option("recursiveFileLookup", "false").yt(tmpPath + "/d=11")
    res0.columns should contain theSameElementsAs Seq("a", "b", "c")

    val res = spark.read.option("recursiveFileLookup", "false").yt(tmpPath).filter("d=11")

    res.columns should contain theSameElementsAs Seq("a", "b", "c", "d")
    res.select("a", "b", "c", "d").collect() should contain theSameElementsAs Seq(
      Row(1, "a", 0.3, 11L)
    )
  }

  it should "read dataset with custom maxSplitRows" in {
    writeTableFromYson(Seq(
      """{a = 1; b = "a"; c = 0.3}""",
      """{a = 2; b = "b"; c = 0.5}"""
    ), tmpPath, atomicSchema)

    val res = spark.read.option("maxSplitRows", "1").yt(tmpPath)

    res.columns should contain theSameElementsAs Seq("a", "b", "c")
    res.select("a", "b", "c").collect() should contain theSameElementsAs Seq(
      Row(1, "a", 0.3),
      Row(2, "b", 0.5)
    )
  }

  it should "read utf8" in {
    writeTableFromYson(Seq(
      """{utf8 = "12"}""",
      """{utf8 = "23"}""",
      """{utf8 = #}""",
      """{utf8 = " apache "}""",
    ), tmpPath, TableSchema.builder()
      .setUniqueKeys(false)
      .addValue("utf8", TiType.optional(TiType.utf8()))
      .build()
    )
    val res = spark.read.yt(tmpPath)

    res.select("utf8")
      .collect() should contain theSameElementsAs Seq(
      Row("12"),
      Row("23"),
      Row(null),
      Row(" apache ")
    )
  }

  it should "read date, datetime datatypes" in {
    writeTableFromYson(Seq(
      """{date = 1; datetime = 1;}""",
      """{date = 9999; datetime = 1611733954}"""
    ), tmpPath, TableSchema.builder()
      .setUniqueKeys(false)
      .addValue("date", TiType.date())
      .addValue("datetime", TiType.datetime())
      .build()
    )

    val cols = Seq("date", "datetime").map(col)
    val res = spark.read.yt(tmpPath)
    res.collect() should contain theSameElementsAs Seq(
      Row(Date.valueOf(LocalDate.ofEpochDay(1)), new Timestamp(1000)),
      Row(Date.valueOf(LocalDate.ofEpochDay(9999)), new Timestamp(1611733954000L))
    )
    res.select(cols.map(_.cast(StringType)): _*).collect() should contain theSameElementsAs Seq(
      Row("1970-01-02", "1970-01-01 03:00:01"),
      Row("1997-05-18", "2021-01-27 10:52:34")
    )
  }

  it should "read timestamp, interval datatypes" in {
    writeTableFromYson(Seq(
      """{timestamp = 1; interval = 1}""",
      """{timestamp = 4; interval = 5}"""
    ), tmpPath, TableSchema.builder()
      .setUniqueKeys(false)
      .addValue("timestamp", TiType.timestamp())
      .addValue("interval", TiType.interval())
      .build()
    )

    val res = spark.read.yt(tmpPath)
    res.collect() should contain theSameElementsAs Seq(
      Row(1, 1),
      Row(4, 5)
    )
  }

  it should "read dataset with nulls" in {
    writeTableFromYson(Seq(
      """{a = 1; b = #; c = 0.3}""",
      """{a = 2; b = "b"; c = #}"""
    ), tmpPath, atomicSchema)

    val res = spark.read.yt(tmpPath)

    res.columns should contain theSameElementsAs Seq("a", "b", "c")
    res.select("a", "b", "c").collect() should contain theSameElementsAs Seq(
      Row(1, null, 0.3),
      Row(2, "b", null)
    )
  }

  private def testEnabledAndDisabledArrow(f: DataFrameReader => Unit): Unit = {
    f(spark.read.enableArrow)
    f(spark.read.disableArrow)
  }

  it should "read float datatype" in {
    Seq(Some(0.3f), Some(0.5f), None)
      .toDF("c").write.optimizeFor(OptimizeMode.Scan).yt(tmpPath)

    testEnabledAndDisabledArrow { reader =>
      val res = reader.yt(tmpPath)

      res.columns should contain theSameElementsAs Seq("c")
      res.select("c").collect() should contain theSameElementsAs Seq(
        Row(0.3f),
        Row(0.5f),
        Row(null)
      )
    }
  }

  it should "write several partitions" in {
    import spark.implicits._
    (1 to 100).toDF.repartition(10).write.yt(tmpPath)

    spark.read.yt(tmpPath).as[Long].collect() should contain theSameElementsAs (1 to 100)
  }

  it should "write several batches" in {
    import spark.implicits._

    spark.sqlContext.setConf("spark.yt.write.batchSize", "10")
    spark.sqlContext.setConf("spark.yt.write.miniBatchSize", "2")
    (1 to 80).toDF.repartition(2).write.yt(tmpPath)

    spark.read.yt(tmpPath).as[Long].collect() should contain theSameElementsAs (1 to 80)
  }

  it should "write all atomic datatypes" in {
    val data = Seq(
      (Some(true), Some(1.toByte), Some(1.0), Some(1.0f), Some(1), Some(1L), Some(1.toShort), Some("a")),
      (None, None, None, None, None, None, None, None)
    )

    data.toDF().write.yt(tmpPath)

    spark.read.yt(tmpPath).collect() should contain theSameElementsAs Seq(
      Row(true, 1.toByte, 1.0, 1.0f, 1, 1L, 1.toShort, "a"),
      Row(null, null, null, null, null, null, null, null)
    )
  }

  it should "write binary type" in {
    val data = Seq(
      Array[Byte](1, 2, 3),
      Array[Byte](4, 5, 6),
      null
    )

    data.toDF().write.yt(tmpPath)

    val result = spark.read.schemaHint("value" -> BinaryType).yt(tmpPath).as[Array[Byte]].collect()
    result should contain theSameElementsAs data
  }

  it should "clean all temporary files if job failed" in {
    import spark.implicits._

    Logger.getRootLogger.setLevel(Level.OFF)

    a[SparkException] shouldBe thrownBy {
      Seq(1, 2, 3).toDF().coalesce(1).map { _ => throw new RuntimeException("Fail job"); 1 }.write.yt(tmpPath)
    }

    Logger.getRootLogger.setLevel(Level.WARN)

    YtWrapper.exists(tmpPath) shouldEqual false
    YtWrapper.exists(s"$tmpPath-tmp") shouldEqual false
  }

  it should "fail if table already exists" in {
    import spark.implicits._

    Seq(1, 2, 3).toDF.coalesce(1).write.yt(tmpPath)
    an[AnalysisException] shouldBe thrownBy {
      Seq(1, 2, 3).toDF.coalesce(1).write.yt(tmpPath)
    }
  }

  it should "overwrite table" in {
    import spark.implicits._

    Seq(1, 2, 3).toDF.coalesce(1).write.yt(tmpPath)

    Seq(4, 5, 6).toDF.coalesce(1).write.mode(SaveMode.Overwrite).yt(tmpPath)

    val res = spark.read.yt(tmpPath)
    res.as[Long].collect() should contain theSameElementsAs Seq(4L, 5L, 6L)
  }

  it should "not lose old table while overwriting by table with errors" in {
    import spark.implicits._

    Seq(1, 2, 3).toDF.coalesce(1).write.yt(tmpPath)

    a[SparkException] shouldBe thrownBy {
      Seq(4, 5, 6).toDS.map(_ / 0).write.mode(SaveMode.Overwrite).yt(tmpPath)
    }

    val res = spark.read.yt(tmpPath)
    res.as[Long].collect() should contain theSameElementsAs Seq(1L, 2L, 3L)
  }

  it should "append rows to table" in {
    import spark.implicits._

    Seq(1, 2, 3).toDF.coalesce(1).write.yt(tmpPath)
    Seq(4, 5, 6).toDF.coalesce(1).write.mode(SaveMode.Append).yt(tmpPath)

    val res = spark.read.yt(tmpPath)
    res.as[Long].collect() should contain theSameElementsAs Seq(1L, 2L, 3L, 4L, 5L, 6L)
  }

  it should "ignore write if table already exists" in {
    import spark.implicits._

    Seq(1, 2, 3).toDF.coalesce(1).write.yt(tmpPath)

    Seq(4, 5, 6).toDF.coalesce(1).write.mode(SaveMode.Ignore).yt(tmpPath)

    val res = spark.read.yt(tmpPath)
    res.as[Long].collect() should contain theSameElementsAs Seq(1L, 2L, 3L)
  }

  it should "count df" in {
    writeTableFromYson(Seq(
      """{a = 1; b = "a"; c = 0.3}""",
      """{a = 2; b = "b"; c = 0.5}"""
    ), tmpPath, atomicSchema)

    val res = spark.read.yt(tmpPath).count()

    res shouldEqual 2
  }

  it should "count large table" in {
    val rng = new Random(0)
    val data = List.fill(10000)(rng.nextInt(200))

    val df = data.toDF("a")
    df.repartition(10).write.yt(tmpPath)

    val res =
      withConf(s"spark.hadoop.yt.${SparkYtConfiguration.Read.CountOptimizationEnabled.name}", "true") {
        withConf(FILES_MAX_PARTITION_BYTES, "1Kb") {
          spark.read.yt(tmpPath).count()
        }
      }
    res shouldBe 10000
  }

  it should "count all partitions" in {
    writeTableFromYson(Seq.fill(100)(
      """{a = 1; b = "a"; c = 0.3}"""
    ), tmpPath, atomicSchema)

    val res = withConf(FILES_MAX_PARTITION_BYTES, "1Kb") {
      spark.read.yt(tmpPath).count()
    }

    res shouldEqual 100
  }

  it should "optimize table for scan" in {
    import spark.implicits._

    Seq(1, 2, 3).toDF.coalesce(1).write.optimizeFor(OptimizeMode.Scan).yt(tmpPath)

    YtWrapper.attribute(tmpPath, "optimize_for").stringValue() shouldEqual "scan"
  }

  it should "use external transaction in writing" in {
    Seq(("a", 1L), ("b", 2L), ("c", 3L)).toDF("c1", "c2").write.yt(tmpPath)

    val transaction = YtWrapper.createTransaction(None, 10 minute)

    Seq(("d", 4L)).toDF("c1", "c2").write
      .mode(SaveMode.Append).transaction(transaction.getId.toString).yt(tmpPath)
    val resultBeforeCommit = spark.read.yt(tmpPath)
    resultBeforeCommit.collect() should contain theSameElementsAs Seq(
      Row("a", 1L), Row("b", 2L), Row("c", 3L)
    )

    transaction.commit().get(10, TimeUnit.SECONDS)

    val resultAfterCommit = spark.read.yt(tmpPath)
    resultAfterCommit.collect() should contain theSameElementsAs Seq(
      Row("a", 1L), Row("b", 2L), Row("c", 3L), Row("d", 4L)
    )
  }

  it should "work with several transactions" in {
    val data1 = Seq(("a", 1L), ("b", 2L), ("c", 3L))
    val data2 = Seq(("d", 4L))
    val data3 = Seq(("e", 5L))
    data1.toDF("_1", "_2").write.yt(tmpPath)

    val trWrite1 = YtWrapper.createTransaction(None, 10 minute)
    val trWrite2 = YtWrapper.createTransaction(None, 10 minute)
    val trRead = YtWrapper.createTransaction(None, 10 minute)
    try {
      data2.toDF("_1", "_2").write.mode(SaveMode.Append).transaction(trWrite1.getId.toString).yt(tmpPath)
      val resultInTransaction = spark.read.option("recursiveFileLookup", "false")
        .transaction(trRead.getId.toString).yt(tmpPath).selectAs[(String, Long)]
      resultInTransaction.collect() should contain theSameElementsAs data1
      trWrite1.commit().get(10, TimeUnit.SECONDS)

      data3.toDF("_1", "_2").write.mode(SaveMode.Append).option("recursiveFileLookup", "false")
        .transaction(trWrite2.getId.toString).yt(tmpPath)
      resultInTransaction.collect() should contain theSameElementsAs data1
      trWrite2.commit().get(10, TimeUnit.SECONDS)

      resultInTransaction.collect() should contain theSameElementsAs data1

      val resultAfterCommit = spark.read.option("recursiveFileLookup", "false")
        .yt(tmpPath).selectAs[(String, Long)]
      trRead.commit()
      resultAfterCommit.collect() should contain theSameElementsAs (data1 ++ data2 ++ data3)
    }
    finally {
      trWrite1.close()
      trWrite2.close()
      trRead.close()
    }
  }

  it should "kill transaction when failed because of timeout" in {
    import spark.implicits._
    spark.sqlContext.setConf("spark.yt.timeout", "5")
    spark.sqlContext.setConf("spark.yt.write.timeout", "0")
    Logger.getRootLogger.setLevel(Level.OFF)

    try {
      a[SparkException] shouldBe thrownBy {
        Seq(1, 2, 3).toDS.coalesce(1).write.yt(tmpPath)
      }

      YtWrapper.exists(s"$tmpPath-tmp") shouldEqual false
    } finally {
      spark.sqlContext.setConf("spark.yt.timeout", "300")
      spark.sqlContext.setConf("spark.yt.write.timeout", "120")
      Logger.getRootLogger.setLevel(Level.WARN)
    }
  }


  it should "read int32" in {
    import scala.collection.JavaConverters._

    val schema = YTree.builder()
      .beginAttributes()
      .key("strict").value(true)
      .key("unique_keys").value(false)
      .endAttributes
      .value(
        Seq(
          YTree.builder()
            .beginMap()
            .key("name").value("a")
            .key("type").value(TiType.int32().getTypeName.getWireName)
            .key("required").value(false)
            .buildMap
        ).asJava)
      .build
    val physicalSchema = TableSchema.builder()
      .setUniqueKeys(false)
      .addValue("a", ColumnValueType.INT64)
      .build()
    writeTableFromYson(Seq(
      """{a = 1}""",
      """{a = 2}"""
    ), tmpPath, schema, physicalSchema, OptimizeMode.Scan, Map.empty)


    val result = spark.read.yt(tmpPath)
    result.schema.fields.head.dataType shouldEqual IntegerType
    result.collect() should contain theSameElementsAs Seq(Row(1), Row(2))
  }

  it should "read empty table" in {
    createEmptyTable(tmpPath, atomicSchema)
    val res = spark.read.yt(tmpPath)
    res.columns should contain theSameElementsAs Seq("a", "b", "c")
    res.collect().isEmpty shouldEqual true
  }

  it should "set custom cypress attributes" in {
    val expirationTime = "2025-06-30T20:44:09.000000Z"
    val myCustomAttribute = "elephant"
    Seq(1, 2, 3).toDF
      .coalesce(1)
      .write
      .option("expiration_time", expirationTime)
      .option("_my_custom_attribute", myCustomAttribute)
      .yt(tmpPath)
    YtWrapper.attribute(tmpPath, "expiration_time").stringValue() shouldEqual expirationTime
    YtWrapper.attribute(tmpPath, "_my_custom_attribute").stringValue() shouldEqual myCustomAttribute
  }

  it should "read many tables" in {
    YtWrapper.createDir(tmpPath)
    val tableCount = 3
    (1 to tableCount).foreach(i =>
      writeTableFromYson(Seq(
        """{a = 1; b = "a"; c = 0.3}""",
        """{a = 2; b = "b"; c = 0.5}"""
      ), s"$tmpPath/$i", atomicSchema)
    )

    val res = withConf(PARALLEL_PARTITION_DISCOVERY_THRESHOLD, "2") {
      spark.read.yt((1 to tableCount).map(i => s"$tmpPath/$i"): _*)
    }

    res.columns should contain theSameElementsAs Seq("a", "b", "c")
    res.select("a", "b", "c").collect() should contain theSameElementsAs (1 to tableCount).flatMap(_ => Seq(
      Row(1, "a", 0.3),
      Row(2, "b", 0.5)
    ))
  }

  it should "read subdirectories" in {
    YtWrapper.createDir(tmpPath)
    YtWrapper.createDir(s"$tmpPath/subdir")
    val table = s"$tmpPath/subdir/table"
    writeTableFromYson(Seq(
      """{a = 0; b = "a"; c = 0.3}""",
      """{a = 3; b = "b"; c = 1.5}"""
    ), table, atomicSchema)

    val res = spark.read.yt(table)
    res.columns should contain theSameElementsAs Seq("a", "b", "c")
    res.select("a", "b", "c").collect() should contain theSameElementsAs Seq(
      Row(0, "a", 0.3),
      Row(3, "b", 1.5)
    )
  }

  it should "enable/disable batch reading" in {
    import OptimizeMode._
    spark.conf.set(SQLConf.PARALLEL_PARTITION_DISCOVERY_THRESHOLD.key, "3")
    val table = Table(
      ("tables", "enableArrow", "expected"),
      (Seq(s"$tmpPath/1" -> Lookup), true, false),
      (Seq(s"$tmpPath/2" -> Scan), false, false),
      (Seq(s"$tmpPath/3" -> Scan), true, true),
      (Seq(s"$tmpPath/4" -> Scan, s"$tmpPath/5" -> Scan), true, true),
      (Seq(s"$tmpPath/6" -> Scan, s"$tmpPath/7" -> Lookup), true, false),
      (Seq(s"$tmpPath/8" -> Scan, s"$tmpPath/9" -> Scan, s"$tmpPath/10" -> Scan, s"$tmpPath/11" -> Scan), true, true),
      (Seq(s"$tmpPath/12" -> Scan, s"$tmpPath/13" -> Scan, s"$tmpPath/14" -> Scan, s"$tmpPath/15" -> Lookup), true, false)
    )

    YtWrapper.createDir(tmpPath)
    forAll(table) { (tables, enableArrow, expected) =>
      tables.foreach { case (path, optimizeFor) =>
        writeTableFromYson(
          Seq("""{a = 1; b = "a"; c = 0.3}"""),
          path, atomicSchema,
          optimizeFor
        )
      }
      val plan = physicalPlan(spark.read.enableArrow(enableArrow).yt(tables.map(_._1): _*))

      val batchEnabled = nodes(plan).collectFirst {
        case scan: InputAdapter => scan.supportsColumnar
      }.get

      batchEnabled shouldEqual expected
    }
  }

  it should "enable batch for yson types" in {
    writeTableFromYson(Seq(
      """{value = {a = 1; b = "a"}}""",
      """{value = {a = 2; b = "b"}}"""
    ), tmpPath, anySchema)

    val res = spark.read.enableArrow(true).schemaHint(
      "value" -> StructType(Seq(StructField("a", LongType), StructField("b", StringType)))
    ).yt(tmpPath)

    val plan = physicalPlan(res)
    val batchEnabled = nodes(plan).collectFirst {
      case scan: InputAdapter => scan.supportsColumnar
    }.get

    batchEnabled shouldEqual false
  }

  it should "enable batch for count" in {
    writeTableFromYson(
      Seq(
        """{a = 1; b = "a"; c = 0.3}""",
        """{a = 2; b = "b"; c = 0.5}"""
      ),
      tmpPath, atomicSchema,
      OptimizeMode.Lookup
    )
    spark.conf.set("spark.sql.adaptive.enabled", "false")

    val res = spark.read.disableArrow.yt(tmpPath).groupBy().count()
    val plan = physicalPlan(res)
    val batchEnabled = nodes(plan).collectFirst {
      case scan: InputAdapter => scan.supportsColumnar
    }.get

    batchEnabled shouldEqual true
  }

  it should "read arrow" in {
    writeTableFromYson(Seq(
      """{a = 1; b = "a"; c = 0.3}""",
      """{a = 2; b = "b"; c = 0.5}"""
    ), tmpPath, atomicSchema)

    val df = spark.read.enableArrow.yt(tmpPath)
    df.columns should contain theSameElementsAs Seq("a", "b", "c")
    df.select("a", "b", "c").collect() should contain theSameElementsAs Seq(
      Row(1, "a", 0.3),
      Row(2, "b", 0.5)
    )
  }

  it should "read categorical arrow" in {
    val data = (0 to 500).map(i => if (i % 2 == 0) "a" else "b")
    data.toDF("a").write.optimizeFor("scan").yt(tmpPath)

    val df = spark.read.enableArrow.yt(tmpPath)
    df.select("a").collect().map(_.get(0)) should contain theSameElementsAs data
  }

  it should "read primitives in any column" in {
    val data = Seq(
      Seq[Any](0L, 0.0, 0.0f, false, 0.toByte, UInt64Long(0)),
      Seq[Any](65L, 1.5, 7.2f, true, 3.toByte, UInt64Long(4)))

    val preparedData = Seq(
      Seq[Any](0L, 0.0, 0.0, false, 0L, UInt64Long(0).toLong),
      Seq[Any](65L, 1.5, 7.2, true, 3L, UInt64Long(4).toLong))

    writeTableFromURow(
      preparedData.map(x => new UnversionedRow(java.util.List.of[UnversionedValue](
        new UnversionedValue(0, ColumnValueType.INT64, false, x(0)),
        new UnversionedValue(1, ColumnValueType.DOUBLE, false, x(1)),
        new UnversionedValue(2, ColumnValueType.DOUBLE, false, x(2)),
        new UnversionedValue(3, ColumnValueType.BOOLEAN, false, x(3)),
        new UnversionedValue(4, ColumnValueType.INT64, false, x(4)),
        new UnversionedValue(5, ColumnValueType.UINT64, false, x(5)),
      ))),
      tmpPath, TableSchema.builder()
        .setUniqueKeys(false)
        .addValue("int64", ColumnValueType.ANY)
        .addValue("double", ColumnValueType.ANY)
        .addValue("float", ColumnValueType.ANY)
        .addValue("boolean", ColumnValueType.ANY)
        .addValue("int8", ColumnValueType.ANY)
        .addValue("uint64", ColumnValueType.ANY)
        .build())

    val schemaHint = StructType(Seq(
      StructField("int64", LongType), StructField("double", DoubleType), StructField("float", FloatType),
      StructField("boolean", BooleanType), StructField("int8", ByteType), StructField("uint64", UInt64Type)))

    val arrowRes = spark.read.enableArrow.schemaHint(schemaHint).yt(tmpPath).collect()
    val wireRes = spark.read.disableArrow.schemaHint(schemaHint).yt(tmpPath).collect()
    val ans = data.map(Row.fromSeq)

    arrowRes should contain theSameElementsAs ans
    wireRes should contain theSameElementsAs ans
  }

  it should "read wire" in {
    writeTableFromYson(Seq(
      """{a = 1; b = "a"; c = 0.3}""",
      """{a = 2; b = "b"; c = 0.5}"""
    ), tmpPath, atomicSchema)

    val df = spark.read.disableArrow.yt(tmpPath)
    df.columns should contain theSameElementsAs Seq("a", "b", "c")
    df.select("a", "b", "c").collect() should contain theSameElementsAs Seq(
      Row(1, "a", 0.3),
      Row(2, "b", 0.5)
    )
  }

  it should "read arrow when schema was changed" in {
    val newSchema = TableSchema.builder()
      .setUniqueKeys(false)
      .addAll(atomicSchema.getColumns)
      .addValue("d", ColumnValueType.STRING)
      .build()
    YtWrapper.createDir(tmpPath)
    writeTableFromYson(Seq(
      """{a = 1; b = "a"; c = 0.3}""",
      """{a = 2; b = "b"; c = 0.5}"""
    ), s"$tmpPath/in", atomicSchema)
    YtWrapper.createTable(
      s"$tmpPath/out",
      Map(
        "schema" -> newSchema.toYTree,
        "optimize_for" -> YTree.stringNode(OptimizeMode.Scan.name)
      ),
      transaction = None
    )
    YtWrapper.mergeTables(tmpPath, s"$tmpPath/out", sorted = false)

    val df = spark.read.enableArrow.yt(s"$tmpPath/out")

    df.columns should contain theSameElementsAs Seq("a", "b", "c", "d")
    df.select("a", "b", "c", "d").collect() should contain theSameElementsAs Seq(
      Row(1, "a", 0.3, null),
      Row(2, "b", 0.5, null)
    )
  }

  it should "write null type" in {
    val data = Seq((null, null), (null, null))

    a[IllegalArgumentException] shouldBe thrownBy {
      data.toDF("a", "b").coalesce(1).write.yt(tmpPath)
    }
    data.toDF("a", "b").coalesce(1).write
      .option("null_type_allowed", value = true).yt(tmpPath)

    spark.read.yt(tmpPath).collect() should contain theSameElementsAs data.map(Row.fromTuple)
  }

  it should "write double" in {
    val data = Seq(0.4, 0.5, 0.6)
    data.toDF().coalesce(1).write.yt(tmpPath)

    spark.read.yt(tmpPath).as[Double].collect() should contain theSameElementsAs data
  }

  it should "write date, datetime, timestamp, interval datatypes" in {
    val data = Seq(
      (Date.valueOf(LocalDate.ofEpochDay(1)), new Timestamp(1000), 1, 1),
      (Date.valueOf(LocalDate.ofEpochDay(9999)), new Timestamp(1611733954000L), 4, 5)
    )
    data.toDF("date", "datetime", "timestamp", "interval").write
      .yt(tmpPath)

    val cols = Seq("date", "datetime", "timestamp", "interval").map(col)
    val res = spark.read.yt(tmpPath)
    res.select(cols: _*).collect() should contain theSameElementsAs data.map(Row.fromTuple(_))
    res.select(cols.map(_.cast(StringType)): _*).collect() should contain theSameElementsAs Seq(
      Row("1970-01-02", "1970-01-01 03:00:01", "1", "1"),
      Row("1997-05-18", "2021-01-27 10:52:34", "4", "5")
    )
  }

  it should "ytTable:/ write and read" in {
    val customPath = "ytTable:/" + tmpPath
    val data = Seq(0.4, 0.5, 0.6)
    data.toDF().coalesce(1).write.yt(customPath)

    spark.read.yt(customPath).as[Double].collect() should contain theSameElementsAs data
  }

  it should "read dataframe from several tables" in {
    YtWrapper.createDir(tmpPath)
    val table1 = s"$tmpPath/t1"
    val table2 = s"$tmpPath/t2"
    writeTableFromYson(Seq(
      """{a = 1; b = "a"; c = 0.3}"""
    ), table1, atomicSchema)
    writeTableFromYson(Seq(
      """{a = 2; b = "b"; c = 0.5}"""
    ), table2, atomicSchema)

    val df = spark.read.yt(table1, table2)

    df.columns should contain theSameElementsAs Seq("a", "b", "c")
    df.select("a", "b", "c").collect() should contain theSameElementsAs Seq(
      Row(1, "a", 0.3),
      Row(2, "b", 0.5)
    )
  }

  it should "count io statistics" in {
    val customPath = "ytTable:/" + tmpPath
    val data = Stream.from(1).take(1000)

    val store = UtilsWrapper.appStatusStore(spark)
    val stagesBefore = store.stageList(null)
    val totalInputBefore = stagesBefore.map(_.inputBytes).sum
    val totalOutputBefore = stagesBefore.map(_.outputBytes).sum

    data.toDF().coalesce(1).write.yt(customPath)
    val allRows = spark.read.yt(customPath).collect()
    allRows should have size data.length

    val stages = store.stageList(null)
    val totalInput = stages.map(_.inputBytes).sum
    val totalOutput = stages.map(_.outputBytes).sum
    totalInput should be > totalInputBefore
    totalOutput should be > totalOutputBefore
  }
}

object Counter {
  var counter = 0
}
