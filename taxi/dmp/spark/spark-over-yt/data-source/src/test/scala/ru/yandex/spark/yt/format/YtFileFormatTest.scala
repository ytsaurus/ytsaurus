package ru.yandex.spark.yt.format

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkException
import org.apache.spark.sql.execution.InputAdapter
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.{AnalysisException, Encoders, Row, SaveMode}
import org.mockito.scalatest.MockitoSugar
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTree
import ru.yandex.spark.yt._
import ru.yandex.spark.yt.format.tmp._
import ru.yandex.spark.yt.fs.conf.YtLogicalType
import ru.yandex.spark.yt.test.{LocalSpark, TestUtils, TmpDir}
import ru.yandex.spark.yt.wrapper.YtWrapper
import ru.yandex.spark.yt.wrapper.table.OptimizeMode
import ru.yandex.yt.ytclient.tables.{ColumnValueType, TableSchema}

import scala.concurrent.duration._
import scala.language.postfixOps

class YtFileFormatTest extends FlatSpec with Matchers with LocalSpark with TmpDir with TestUtils with MockitoSugar with TableDrivenPropertyChecks {

  import YtFileFormatTest._
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

  private def writeComplexTable(path: String): Unit = {
    val ytSchema = new TableSchema.Builder()
      .setUniqueKeys(false)
      .addValue("f1", ColumnValueType.ANY)
      .addValue("f2", ColumnValueType.ANY)
      .addValue("f3", ColumnValueType.ANY)
      .addValue("f4", ColumnValueType.ANY)
      .addValue("f5", ColumnValueType.ANY)
      .addValue("f6", ColumnValueType.ANY)
      .addValue("f7", ColumnValueType.ANY)
      .addValue("f8", ColumnValueType.ANY)
      .addValue("f9", ColumnValueType.ANY)
      .build()
    writeTableFromYson(Seq(
      """{
        |f1={a={aa=1};b=#;c={cc=#}};
        |f2={a={a="aa"};b=#;c={a=#}};
        |f3={a=[0.1];b=#;c=[#]};
        |f4={a=%true;b=#};
        |f5={a={a=1;b=#};b={a="aa"};c=[%true;#];d=0.1};
        |f6=[{a=1;b=#};#];
        |f7=[{a="aa"};{a=#};#];
        |f8=[[1;#];#];
        |f9=[0.1;#]
        |}""".stripMargin
    ), path, ytSchema)
  }

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

  it should "read dataset with list of long" in {
    writeTableFromYson(Seq(
      "{value = [1; 2; 3]}",
      "{value = [4; 5; 6]}"
    ), tmpPath, anySchema)

    val res = spark.read.schemaHint("value" -> ArrayType(LongType)).yt(tmpPath)

    res.columns should contain theSameElementsAs Seq("value")
    res.collect() should contain theSameElementsAs Seq(
      Row(Seq(1, 2, 3)),
      Row(Seq(4, 5, 6))
    )
  }

  it should "read dataset with list of string" in {
    writeTableFromYson(Seq(
      """{value = ["a"; "b"; "c"]}"""
    ), tmpPath, anySchema)

    val res = spark.read.schemaHint("value" -> ArrayType(StringType)).yt(tmpPath)

    res.columns should contain theSameElementsAs Seq("value")
    res.collect() should contain theSameElementsAs Seq(
      Row(Seq("a", "b", "c"))
    )
  }

  it should "read dataset with list of lists" in {
    writeTableFromYson(Seq(
      "{value = [[1]; [2; 3]]}",
      "{value = [[4]; #]}"
    ), tmpPath, anySchema)

    val res = spark.read
      .schemaHint("value" -> ArrayType(ArrayType(LongType)))
      .yt(tmpPath)

    res.columns should contain theSameElementsAs Seq("value")
    res.collect() should contain theSameElementsAs Seq(
      Row(Seq(Seq(1), Seq(2, 3))),
      Row(Seq(Seq(4), null))
    )
  }

  it should "read dataset with struct of atomic" in {
    writeTableFromYson(Seq(
      """{value = {a = 1; b = "a"}}""",
      """{value = {a = 2; b = "b"}}""",
    ), tmpPath, anySchema)

    val res = spark.read
      .schemaHint("value" -> StructType(Seq(StructField("a", LongType), StructField("b", StringType))))
      .yt(tmpPath)

    res.columns should contain theSameElementsAs Seq("value")
    res.collect() should contain theSameElementsAs Seq(
      Row(Row(1, "a")),
      Row(Row(2, "b"))
    )
  }

  it should "read dataset with struct of lists" in {
    writeTableFromYson(Seq(
      """{value = {a = [1; 2; 3]; b = ["a"; #]; c = [{a = 1; b = "a"}; {a = 2; b = "b"}]}}""",
      """{value = {a = #; b = [#; "b"]; c = [{a = 3; b = "c"}; {a = 4; b = "d"}]}}"""
    ), tmpPath, anySchema)

    val res = spark.read
      .schemaHint("value" -> StructType(Seq(
        StructField("a", ArrayType(LongType)),
        StructField("b", ArrayType(StringType)),
        StructField("c", ArrayType(StructType(Seq(
          StructField("a", LongType),
          StructField("b", StringType)
        ))))
      )))
      .yt(tmpPath)

    res.columns should contain theSameElementsAs Seq("value")
    res.collect() should contain theSameElementsAs Seq(
      Row(Row(Seq(1, 2, 3), Seq("a", null), Seq(Row(1, "a"), Row(2, "b")))),
      Row(Row(null, Seq(null, "b"), Seq(Row(3, "c"), Row(4, "d"))))
    )
  }

  it should "read dataset with map of atomic" in {
    writeTableFromYson(Seq(
      "{value = {a = 1; b = 2}}",
      "{value = {c = 3; d = 4}}"
    ), tmpPath, anySchema)

    val res = spark.read
      .schemaHint("value" -> MapType(StringType, LongType))
      .yt(tmpPath)

    res.columns should contain theSameElementsAs Seq("value")
    res.collect() should contain theSameElementsAs Seq(
      Row(Map("a" -> 1, "b" -> 2)),
      Row(Map("c" -> 3, "d" -> 4))
    )
  }

  it should "read dataset with complex types" in {
    writeComplexTable(tmpPath)

    val schema = Encoders.product[Test].schema
    val res = spark.read.schemaHint(schema).yt(tmpPath)

    res.columns should contain theSameElementsAs schema.fieldNames
    res.as[Test].collect() should contain theSameElementsAs Seq(testRow)
  }

  it should "read some columns from dataset with complex row" in {
    writeTableFromYson(Seq(
      """{value={
        |f1={a={aa=1};b=#;c={cc=#}};
        |f2={a={a="aa"};b=#;c={a=#}};
        |f3={a=[0.1];b=#;c=[#]};
        |f4={a=%true;b=#};
        |f5={a={a=1;b=#};b={a="aa"};c=[%true;#];d=0.1};
        |f6=[{a=1;b=#};#];
        |f7=[{a="aa"};{a=#};#];
        |f8=[[1;#];#];
        |f9=[0.1;#]
        |}}""".stripMargin
    ), tmpPath, anySchema)
    val res = spark.read
      .schemaHint("value" -> Encoders.product[TestSmall].schema)
      .yt(tmpPath)

    res.columns should contain theSameElementsAs Seq("value")
    res.select($"value.*").as[TestSmall].collect() should contain theSameElementsAs Seq(testRowSmall)
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

  it should "write dataset with complex types" in {
    import spark.implicits._

    Seq(
      (Seq(1, 2, 3), A(1, Some("a")), Map("1" -> 0.1)),
      (Seq(4, 5, 6), A(2, None), Map("2" -> 0.3))
    )
      .toDF("a", "b", "c").coalesce(1)
      .write.mode(SaveMode.Overwrite)
      .yt(tmpPath)

    val res = spark.read
      .schemaHint(
        "a" -> ArrayType(LongType),
        "b" -> StructType(Seq(StructField("field1", LongType), StructField("field2", StringType))),
        "c" -> MapType(StringType, DoubleType)
      )
      .yt(tmpPath)

    res.columns should contain theSameElementsAs Seq("a", "b", "c")
    res.select("a", "b", "c").collect() should contain theSameElementsAs Seq(
      Row(Seq(1, 2, 3), Row(1, "a"), Map("1" -> 0.1)),
      Row(Seq(4, 5, 6), Row(2, null), Map("2" -> 0.3))
    )
  }

  it should "read any as binary if no schema hint provided" in {
    writeComplexTable(tmpPath)
    val res = spark.read.yt(tmpPath)

    res.columns should contain theSameElementsAs Encoders.product[Test].schema.fieldNames
    res.schema.map(_.dataType) should contain theSameElementsAs Encoders.product[Test].schema.map(_ => BinaryType)
    res.collect().length shouldEqual 1
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

  it should "serialize yson" in {
    import org.apache.spark.sql.functions._
    val df1 = Seq("a", "b", "c")
      .toDF("value1")
      .withColumn("value2", lit(null).cast(StringType))
      .withYsonColumn("info", struct('value1, 'value2))

    val df2 = Seq("A", "B", "C")
      .toDF("value2")
      .withColumn("value1", lit(null).cast(StringType))
      .withYsonColumn("info", struct('value1, 'value2))

    df1.unionByName(df2).coalesce(1).write.yt(tmpPath)

    val res = spark.read
      .schemaHint(
        "info" -> StructType(Seq(
          StructField("value1", StringType),
          StructField("value2", StringType)
        ))
      )
      .yt(tmpPath)

    res.collect() should contain theSameElementsAs Seq(
      Row("a", null, Row("a", null)),
      Row("b", null, Row("b", null)),
      Row("c", null, Row("c", null)),
      Row(null, "A", Row(null, "A")),
      Row(null, "B", Row(null, "B")),
      Row(null, "C", Row(null, "C"))
    )
  }

  it should "set custom cypress attributes" in {
    val expirationTime = "2020-06-30T20:44:09.000000Z"
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

  it should "read and write yson" in {
    val initial = Seq(Seq(1, 2, 3), Seq(4, 5, 6))
    initial.toDF().coalesce(1).write.yt(s"$tmpPath/1")
    spark.read.yt(s"$tmpPath/1").printSchema()
    spark.read.yt(s"$tmpPath/1").show()
    spark.read.yt(s"$tmpPath/1").withColumn("value", 'value.cast(BinaryType)).show()

    val expected = spark.read.yt(s"$tmpPath/1").as[Array[Byte]].collect()

    spark.read.yt(s"$tmpPath/1").write.yt(s"$tmpPath/2")

    val resBinary = spark.read.yt(s"$tmpPath/2").as[Array[Byte]].collect()
    resBinary should contain theSameElementsAs expected

    val dfWithHint = spark.read.schemaHint("value" -> ArrayType(LongType)).yt(s"$tmpPath/2")
    dfWithHint.as[Array[Long]].collect() should contain theSameElementsAs initial

    dfWithHint.write.yt(s"$tmpPath/3")

    val resBinary2 = spark.read.yt(s"$tmpPath/3").as[Array[Byte]].collect()
    resBinary2 should contain theSameElementsAs expected
  }

  it should "write non-yson bytes as string" in {
    val initial = Seq(
      Array(1, 2, 3, 4, 5).map(_.toByte),
      Array(6, 7, 8, 9, 10).map(_.toByte)
    )
    initial.toDF().coalesce(1).write.schemaHint("value" -> YtLogicalType.String).yt(tmpPath)

    val res = spark.read.schemaHint("value" -> BinaryType).yt(tmpPath).as[Array[Byte]].collect()
    res should contain theSameElementsAs initial
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

object YtFileFormatTest {
  private val testRow = Test(
    Map("a" -> Some(Map("aa" -> Some(1L))), "b" -> None, "c" -> Some(Map("cc" -> None))),
    Map("a" -> Some(B(Some("aa"))), "b" -> None, "c" -> Some(B(None))),
    Map("a" -> Some(Seq(Some(0.1))), "b" -> None, "c" -> Some(Seq(None))),
    Map("a" -> Some(true), "b" -> None),
    C(
      Map("a" -> Some(1L), "b" -> None),
      B(Some("aa")),
      Seq(Some(true), None),
      Some(0.1)
    ),
    Seq(Some(Map("a" -> Some(1L), "b" -> None)), None),
    Seq(Some(B(Some("aa"))), Some(B(None)), None),
    Seq(Some(Seq(Some(1L), None)), None),
    Seq(Some(0.1), None)
  )

  private val testRowSmall = TestSmall(
    testRow.f1,
    testRow.f4,
    testRow.f7
  )
}
