package ru.yandex.spark.yt.format

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkException
import org.apache.spark.sql.execution.InputAdapter
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{AnalysisException, Row, SaveMode}
import org.mockito.scalatest.MockitoSugar
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTree
import ru.yandex.spark.yt._
import ru.yandex.spark.yt.serializers.YtLogicalType
import ru.yandex.spark.yt.test.{LocalSpark, TestUtils, TmpDir}
import ru.yandex.spark.yt.wrapper.YtWrapper
import ru.yandex.spark.yt.wrapper.table.OptimizeMode
import ru.yandex.yt.ytclient.tables.{ColumnValueType, TableSchema}

import scala.concurrent.duration._
import scala.language.postfixOps

class YtFileFormatTest extends FlatSpec with Matchers with LocalSpark
  with TmpDir with TestUtils with MockitoSugar with TableDrivenPropertyChecks {

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

  "YtFileFormat" should "read dataset" in {
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

    val res = spark.read.yt(tmpPath)

    res.count() shouldEqual 2
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

    try {
      Logger.getRootLogger.setLevel(Level.OFF)
      a[SparkException] shouldBe thrownBy {
        Seq(1, 2, 3).toDS.coalesce(1).write.yt(tmpPath)
      }
      Logger.getRootLogger.setLevel(Level.WARN)

      YtWrapper.exists(s"$tmpPath-tmp") shouldEqual false
    } finally {
      spark.sqlContext.setConf("spark.yt.timeout", "300")
      spark.sqlContext.setConf("spark.yt.write.timeout", "120")
    }
  }

  it should "delete failed task results" ignore {
    import spark.implicits._
    spark.sqlContext.setConf("spark.yt.write.batchSize", "2")
    spark.sqlContext.setConf("spark.yt.write.miniBatchSize", "2")
    spark.sqlContext.setConf("spark.task.maxFailures", "4")

    (1 to 10).toDS.repartition(1).write.yt("/home/sashbel/test/test")

    spark.read.yt("/home/sashbel/test/test").as[Long].map { i =>
      Counter.counter += 1
      if (Counter.counter > 5) {
        val tmpFilePath = "/tmp/retry"
        if (!Files.exists(Paths.get(tmpFilePath))) {
          println("THROW!!!!!!!!!!")
          Files.createFile(Paths.get(tmpFilePath))
          throw new RuntimeException("Write failed")
        }
      }
      i
    }.show()

    //.write.mode(SaveMode.Overwrite).yt(s"/home/sashbel/test-2")

    spark.read.yt("/home/sashbel/test-2").as[Long].map { i =>
      val tmpFilePath = "/tmp/retry"
      Files.deleteIfExists(Paths.get(tmpFilePath))
    }.show()
  }

  it should "write sorted table" in {
    import spark.implicits._

    import scala.collection.JavaConverters._

    (1 to 9).toDF.coalesce(3).write.sortedBy("value").yt(tmpPath)

    val sortColumns = YtWrapper.attribute(tmpPath, "sorted_by").asList().asScala.map(_.stringValue())
    sortColumns should contain theSameElementsAs Seq("value")

    val schemaCheck = Seq("name", "type", "sort_order")
    val schema = YtWrapper.attribute(tmpPath, "schema").asList().asScala.map { field =>
      val map = field.asMap()
      schemaCheck.map(n => n -> map.getOrThrow(n).stringValue())
    }
    schema should contain theSameElementsAs Seq(
      Seq("name" -> "value", "type" -> "int32", "sort_order" -> "ascending")
    )
  }

  it should "abort transaction if failed to create sorted table" in {
    val df = (1 to 9).toDF("my_name").coalesce(3)
    an[Exception] shouldBe thrownBy {
      df.write.sortedBy("bad_name").yt(tmpPath)
    }
    noException shouldBe thrownBy {
      df.write.sortedBy("my_name").yt(tmpPath)
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
    val tableCount = 35
    (1 to tableCount).par.foreach(i =>
      writeTableFromYson(Seq(
        """{a = 1; b = "a"; c = 0.3}""",
        """{a = 2; b = "b"; c = 0.5}"""
      ), s"$tmpPath/$i", atomicSchema)
    )

    val res = spark.read.yt((1 to tableCount).map(i => s"$tmpPath/$i"): _*)

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

  it should "read big csv" in {
    writeFileFromResource("test.csv", tmpPath)
    spark.read.csv(s"yt:/$tmpPath").count() shouldEqual 100000
  }

  it should "write double" in {
    val data = Seq(0.4, 0.5, 0.6)
    data.toDF().coalesce(1).write.yt(tmpPath)

    spark.read.yt(tmpPath).as[Double].collect() should contain theSameElementsAs data
  }
}

object Counter {
  var counter = 0
}
