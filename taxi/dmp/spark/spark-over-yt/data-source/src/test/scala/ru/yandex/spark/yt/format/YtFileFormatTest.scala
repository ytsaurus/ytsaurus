package ru.yandex.spark.yt.format

import org.apache.hadoop.fs.Path
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkException
import org.apache.spark.sql.execution.InputAdapter
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf._
import org.apache.spark.sql.types._
import org.apache.spark.sql.v2.YtUtils
import org.apache.spark.sql.yson.UInt64Long.{fromStringUdf, toStringUdf}
import org.apache.spark.sql.yson.{UInt64Long, UInt64Type}
import org.apache.spark.sql.{AnalysisException, Row, SaveMode}
import org.apache.spark.status.api.v1
import org.mockito.Mockito
import org.mockito.scalatest.MockitoSugar
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers, PrivateMethodTester}
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTree
import ru.yandex.spark.yt._
import ru.yandex.spark.yt.fs.YtClientConfigurationConverter.ytClientConfiguration
import ru.yandex.spark.yt.fs.YtTableFileSystem
import ru.yandex.spark.yt.serializers.YtLogicalType
import ru.yandex.spark.yt.test.{LocalSpark, TestUtils, TmpDir}
import ru.yandex.spark.yt.wrapper.YtWrapper
import ru.yandex.spark.yt.wrapper.client.YtClientProvider
import ru.yandex.spark.yt.wrapper.table.OptimizeMode
import ru.yandex.type_info.TiType
import ru.yandex.yt.ytclient.proxy.CompoundClient
import ru.yandex.yt.ytclient.proxy.request.GetNode
import ru.yandex.yt.ytclient.tables.{ColumnValueType, TableSchema}

import java.nio.charset.StandardCharsets
import java.sql.{Date, Timestamp}
import java.time.LocalDate
import scala.concurrent.duration._
import scala.language.postfixOps

class YtFileFormatTest extends FlatSpec with Matchers with LocalSpark
  with TmpDir with TestUtils with MockitoSugar with TableDrivenPropertyChecks with PrivateMethodTester {
  behavior of "YtFileFormat"

  import spark.implicits._

  private val atomicSchema = new TableSchema.Builder()
    .setUniqueKeys(false)
    .addValue("a", ColumnValueType.INT64)
    .addValue("b", ColumnValueType.STRING)
    .addValue("c", ColumnValueType.DOUBLE)
    .build()

  private val anySchema = new TableSchema.Builder()
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

  it should "read utf8, date, datetime, timestamp, interval datatypes" in {
    writeTableFromYson(Seq(
      """{utf8 = "12"; date = 1; datetime = 1000; timestamp = 1; interval = 1}""",
      """{utf8 = "23"; date = 2; datetime = 3000; timestamp = 4; interval = 5}"""
    ), tmpPath, new TableSchema.Builder()
      .setUniqueKeys(false)
      .addValue("utf8", TiType.utf8())
      .addValue("date", TiType.date())
      .addValue("datetime", TiType.datetime())
      .addValue("timestamp", TiType.timestamp())
      .addValue("interval", TiType.interval())
      .build()
    )
    val res = spark.read.yt(tmpPath)

    res.select("utf8", "date", "datetime", "timestamp", "interval")
      .collect() should contain theSameElementsAs Seq(
      Row("12", Date.valueOf(LocalDate.ofEpochDay(1)), new Timestamp(1), 1, 1),
      Row("23", Date.valueOf(LocalDate.ofEpochDay(2)), new Timestamp(3), 4, 5)
    )
  }

  it should "read dataset with column name containing spaces and digits" in {
    val columnName = "3a b"
    writeTableFromYson(Seq(
      s"""{"$columnName" = 1}""",
      s"""{"$columnName" = 2}"""
    ), tmpPath, new TableSchema.Builder().addKey(columnName, ColumnValueType.INT64).build())

    val res = spark.read.yt(tmpPath)

    res.columns should contain theSameElementsAs Seq(columnName)
    res.select(columnName).collect() should contain theSameElementsAs Seq(
      Row(1),
      Row(2)
    )
  }

  it should "read dataset with column name containing dots" in {
    val columnName = "a.b"
    val sparkColumnName = columnName.replace('.', '_')
    writeTableFromYson(Seq(
      s"""{"$columnName" = 1}""",
      s"""{"$columnName" = 2}"""
    ), tmpPath, new TableSchema.Builder().addKey(columnName, ColumnValueType.INT64).build())

    val res = spark.read.yt(tmpPath)

    res.columns should contain theSameElementsAs Seq(sparkColumnName)
    res.select(sparkColumnName).collect() should contain theSameElementsAs Seq(
      Row(1),
      Row(2)
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

  it should "read float datatype" in {
    Seq(Some(0.3f), Some(0.5f), None)
      .toDF("c").write.yt(tmpPath)

    val res = spark.read.yt(tmpPath)

    res.columns should contain theSameElementsAs Seq("c")
    res.select("c").collect() should contain theSameElementsAs Seq(
      Row(0.3f),
      Row(0.5f),
      Row(null)
    )
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
            .key("type").value(YtLogicalType.Int32.name)
            .key("required").value(false)
            .buildMap
        ).asJava)
      .build
    val physicalSchema = new TableSchema.Builder()
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

  it should "read csv" in {
    YtWrapper.createFile(tmpPath)
    val os = YtWrapper.writeFile(tmpPath, 1 minute, None)
    try {
      os.write(
        """a,b,c
          |1,2,3
          |4,5,6""".stripMargin.getBytes(StandardCharsets.UTF_8))
    } finally os.close()

    val res = spark.read.option("header", "true").csv(tmpPath.drop(1))

    res.columns should contain theSameElementsAs Seq("a", "b", "c")
    res.select("a", "b", "c").collect() should contain theSameElementsAs Seq(
      Row("1", "2", "3"),
      Row("4", "5", "6")
    )
  }

  it should "accept schema hint for case-sensitive column name" in {
    Seq(
      Seq(1, 2, 3),
      Seq(4, 5, 6)
    ).toDF("EventValue").write.yt(tmpPath)

    val res = spark.read
      .schemaHint("EventValue" -> ArrayType(IntegerType))
      .yt(tmpPath)
      .schema
      .find(_.name == "EventValue")
      .get

    res.dataType shouldEqual ArrayType(IntegerType)
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
      """{value = {a = 2; b = "b"}}""",
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
    val newSchema = new TableSchema.Builder()
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

  it should "read big csv" in {
    writeFileFromResource("test.csv", tmpPath)
    spark.read.csv(s"yt:/$tmpPath").count() shouldEqual 100000
  }

  it should "write double" in {
    val data = Seq(0.4, 0.5, 0.6)
    data.toDF().coalesce(1).write.yt(tmpPath)

    spark.read.yt(tmpPath).as[Double].collect() should contain theSameElementsAs data
  }

  it should "ytTable:/ write and read" in {
    val customPath = "ytTable:/" + tmpPath
    val data = Seq(0.4, 0.5, 0.6)
    data.toDF().coalesce(1).write.yt(customPath)

    spark.read.yt(customPath).as[Double].collect() should contain theSameElementsAs data
  }

  it should "read table from several files" in {
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

  it should "read tables with different schemas when mergeSchema is enabled in read's option" in {
    YtWrapper.createDir(tmpPath)
    val table1 = s"$tmpPath/t1"
    val table2 = s"$tmpPath/t2"
    writeTableFromYson(Seq(
      """{a = 1; b = "a"; c = 0.3}"""
    ), table1, atomicSchema)
    writeTableFromYson(Seq(
      """{c = 2.0; d = "t"}"""
    ), table2, new TableSchema.Builder()
      .setUniqueKeys(false)
      .addValue("c", ColumnValueType.DOUBLE)
      .addValue("d", ColumnValueType.STRING)
      .build()
    )

    val df = spark.read.option("mergeSchema", "true").yt(table1, table2)

    df.columns should contain theSameElementsAs Seq("a", "b", "c", "d")
    df.select("a", "b", "c", "d").collect() should contain theSameElementsAs Seq(
      Row(1, "a", 0.3, null),
      Row(null, null, 2.0, "t")
    )
  }

  it should "read tables with different schemas when mergeSchema is enabled in spark conf" in {
    YtWrapper.createDir(tmpPath)
    val table1 = s"$tmpPath/t1"
    val table2 = s"$tmpPath/t2"
    writeTableFromYson(Seq(
      """{a = 1; b = "a"; c = 0.3}"""
    ), table1, atomicSchema)
    writeTableFromYson(Seq(
      """{c = 2.0; d = "t"}"""
    ), table2, new TableSchema.Builder()
      .setUniqueKeys(false)
      .addValue("c", ColumnValueType.DOUBLE)
      .addValue("d", ColumnValueType.STRING)
      .build()
    )
    val df = withConf("spark.sql.yt.mergeSchema", "true") {
      spark.read.yt(table1, table2)
    }

    df.columns should contain theSameElementsAs Seq("a", "b", "c", "d")
    df.select("a", "b", "c", "d").collect() should contain theSameElementsAs Seq(
      Row(1, "a", 0.3, null),
      Row(null, null, 2.0, "t")
    )
  }

  it should "read tables with same schemas but different order" in {
    YtWrapper.createDir(tmpPath)
    val table1 = s"$tmpPath/t1"
    val table2 = s"$tmpPath/t2"
    writeTableFromYson(Seq(
      """{a = 1; b = "f"; c = 0.3}"""
    ), table1, atomicSchema)
    writeTableFromYson(Seq(
      """{c = 1.2; a = 2; b = "g"}"""
    ), table2, new TableSchema.Builder()
      .setUniqueKeys(false)
      .addValue("c", ColumnValueType.DOUBLE)
      .addValue("a", ColumnValueType.INT64)
      .addValue("b", ColumnValueType.STRING)
      .build()
    )
    val df = spark.read.yt(table1, table2)

    df.columns should contain theSameElementsAs Seq("a", "b", "c")
    df.select("a", "b", "c").collect() should contain theSameElementsAs Seq(
      Row(1, "f", 0.3),
      Row(2, "g", 1.2)
    )
  }

  it should "fail reading tables with incompatible schemas" in {
    YtWrapper.createDir(tmpPath)
    val table1 = s"$tmpPath/t1"
    val table2 = s"$tmpPath/t2"
    writeTableFromYson(Seq(
      """{a = 1; b = "a"; c = 0.3}"""
    ), table1, atomicSchema)
    writeTableFromYson(Seq(
      """{c = "a"}"""
    ), table2, new TableSchema.Builder()
      .setUniqueKeys(false)
      .addValue("c", ColumnValueType.STRING)
      .build()
    )

    a[SparkException] shouldBe thrownBy {
      spark.read.option("mergeSchema", "true").yt(table1, table2)
    }
  }

  it should "fail reading tables with different schemas when mergeSchema is disabled" in {
    YtWrapper.createDir(tmpPath)
    val table1 = s"$tmpPath/t1"
    val table2 = s"$tmpPath/t2"
    writeTableFromYson(Seq(
      """{a = 1; b = "a"; c = 0.3}"""
    ), table1, atomicSchema)
    writeTableFromYson(Seq(
      """{c = 0.2}"""
    ), table2, new TableSchema.Builder()
      .setUniqueKeys(false)
      .addValue("c", ColumnValueType.DOUBLE)
      .build()
    )

    a[SparkException] shouldBe thrownBy {
      spark.read.yt(table1, table2)
    }
  }

  it should "infer table's schema one time" in {
    YtWrapper.createDir(tmpPath)
    val filesCount = 3
    val tables = (1 to filesCount).map(x => s"$tmpPath/t$x")
    tables.foreach {
      table => List(1, 2, 3, 4, 5).toDF().repartition(2).write.yt(table)
    }

    val fs = new YtTableFileSystem
    fs.initialize(new Path("/").toUri, spark.sparkContext.hadoopConfiguration)
    fs.setConf(spark.sparkContext.hadoopConfiguration)

    val filesStatus = tables.flatMap(x => fs.listStatus(new Path(s"ytTable:/$x")))

    val mockYt: CompoundClient = Mockito.spy(YtClientProvider.ytClient(ytClientConfiguration(spark)))
    YtUtils.inferSchema(spark, Map.empty, filesStatus)(mockYt)

    // getNode invoked in YtWrapper.attribute(path, "schema"), that might be invoked for every chunk in inferSchema
    // schema should be asked exactly 1 time for every file
    verify(mockYt, times(tables.length)).getNode(any[GetNode])
    Mockito.reset(mockYt)
  }

  it should "count io statistics" in {
    val customPath = "ytTable:/" + tmpPath
    val data = Stream.from(1).take(1000)

    val statusStore = PrivateMethod[AnyRef]('statusStore)
    val stageList = PrivateMethod[Seq[v1.StageData]]('stageList)
    val store = spark.sparkContext invokePrivate statusStore()
    val stagesBefore = store invokePrivate stageList(null)
    val totalInputBefore = stagesBefore.map(_.inputBytes).sum
    val totalOutputBefore = stagesBefore.map(_.outputBytes).sum

    data.toDF().coalesce(1).write.yt(customPath)
    val allRows = spark.read.yt(customPath).collect()
    allRows should have size data.length

    val stages = store invokePrivate stageList(null)
    val totalInput = stages.map(_.inputBytes).sum
    val totalOutput = stages.map(_.outputBytes).sum
    totalInput should be > totalInputBefore
    totalOutput should be > totalOutputBefore
  }

}

object Counter {
  var counter = 0
}
