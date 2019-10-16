package ru.yandex.spark.yt.format

import java.nio.file.{Files, Paths}
import java.time.{ZoneOffset, ZonedDateTime}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkException
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types._
import org.apache.spark.sql.{AnalysisException, Encoders, Row, SaveMode}
import org.scalatest.{FlatSpec, Matchers}
import ru.yandex.spark.yt._
import ru.yandex.spark.yt.utils.DateTimeUtils.{emptyStringToNull, format, formatDatetime, time}

class YtFileFormatTest extends FlatSpec with Matchers with TmpTable {
  import YtFileFormatTest._

  "YtFileFormat" should "read dataset" in {
    val res = spark.read.yt("/home/sashbel/data/test_read_atomic")

    res.columns should contain theSameElementsAs Seq("a", "b", "c")
    res.select("a", "b", "c").collect() should contain theSameElementsAs Seq(
      Row(1, "a", 0.3),
      Row(2, "b", 0.5)
    )
  }

  it should "read dataset with custom maxSplitRows" in {
    val res = spark.read.option("maxSplitRows", "1").yt("/home/sashbel/data/test_read_atomic")

    res.columns should contain theSameElementsAs Seq("a", "b", "c")
    res.select("a", "b", "c").collect() should contain theSameElementsAs Seq(
      Row(1, "a", 0.3),
      Row(2, "b", 0.5)
    )
  }

  it should "read dataset with nulls" in {
    val res = spark.read.yt("/home/sashbel/data/test_read_atomic_with_null")

    res.columns should contain theSameElementsAs Seq("a", "b", "c")
    res.select("a", "b", "c").collect() should contain theSameElementsAs Seq(
      Row(1, null, 0.3),
      Row(2, "b", null)
    )
  }

  it should "read dataset with list of long" in {
    val res = spark.read
      .schemaHint("value" -> ArrayType(LongType))
      .yt("/home/sashbel/data/test_read_list_atomic")

    res.columns should contain theSameElementsAs Seq("value")
    res.collect() should contain theSameElementsAs Seq(
      Row(Seq(1, 2, 3)),
      Row(Seq(4, 5, 6))
    )
  }

  it should "read dataset with list of string" in {
    val res = spark.read
      .schemaHint("value" -> ArrayType(StringType))
      .yt("/home/sashbel/data/test_read_list_string")

    res.columns should contain theSameElementsAs Seq("value")
    res.collect() should contain theSameElementsAs Seq(
      Row(Seq("a", "b", "c"))
    )
  }

  it should "read dataset with list of lists" in {
    val res = spark.read
      .schemaHint("value" -> ArrayType(ArrayType(LongType)))
      .yt("/home/sashbel/data/test_read_list_of_lists")

    res.columns should contain theSameElementsAs Seq("value")
    res.collect() should contain theSameElementsAs Seq(
      Row(Seq(Seq(1), Seq(2, 3))),
      Row(Seq(Seq(4), null))
    )
  }

  it should "read dataset with struct of atomic" in {
    val res = spark.read
      .schemaHint("value" -> StructType(Seq(StructField("a", LongType), StructField("b", StringType))))
      .yt("/home/sashbel/data/test_read_struct_atomic")

    res.columns should contain theSameElementsAs Seq("value")
    res.collect() should contain theSameElementsAs Seq(
      Row(Row(1, "a")),
      Row(Row(2, "b"))
    )
  }

  it should "read dataset with struct of lists" in {
    val res = spark.read
      .schemaHint("value" -> StructType(Seq(
        StructField("a", ArrayType(LongType)),
        StructField("b", ArrayType(StringType)),
        StructField("c", ArrayType(StructType(Seq(
          StructField("a", LongType),
          StructField("b", StringType)
        ))))
      )))
      .yt("/home/sashbel/data/test_read_struct_list")

    res.columns should contain theSameElementsAs Seq("value")
    res.collect() should contain theSameElementsAs Seq(
      Row(Row(Seq(1, 2, 3), Seq("a", null), Seq(Row(1, "a"), Row(2, "b")))),
      Row(Row(null, Seq(null, "b"), Seq(Row(3, "c"), Row(4, "d"))))
    )
  }

  it should "read dataset with map of atomic" in {
    val res = spark.read
      .schemaHint("value" -> MapType(StringType, LongType))
      .yt("/home/sashbel/data/test_read_map_atomic")

    res.columns should contain theSameElementsAs Seq("value")
    res.collect() should contain theSameElementsAs Seq(
      Row(Map("a" -> 1, "b" -> 2)),
      Row(Map("c" -> 3, "d" -> 4))
    )
  }

  it should "read dataset with complex types" in {
    import spark.implicits._

    val schema = Encoders.product[Test].schema
    val res = spark.read.schemaHint(schema).yt("/home/sashbel/data/test_read_complex")

    res.columns should contain theSameElementsAs schema.fieldNames
    res.as[Test].collect() should contain theSameElementsAs Seq(testRow)
  }

  it should "read some columns from dataset with complex row" in {
    import spark.implicits._

    val res = spark.read
      .schemaHint("value" -> Encoders.product[TestSmall].schema)
      .yt("/home/sashbel/data/test_read_complex_row")

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

    YtTableUtils.exists(tmpPath) shouldEqual false
    YtTableUtils.exists(s"$tmpPath-tmp") shouldEqual false
  }

  it should "write dataset with complex types" in {
    import spark.implicits._

    Seq(
      (Seq(1, 2, 3), A(1, Some("a")), Map("1" -> 0.1)),
      (Seq(4, 5, 6), A(2, None), Map("2" -> 0.3))
    )
      .toDF("a", "b", "c").coalesce(1)
      .write.mode(SaveMode.Overwrite)
      .yt("/home/sashbel/data/test_write_complex")

    val res = spark.read
      .schemaHint(
        "a" -> ArrayType(LongType),
        "b" -> StructType(Seq(StructField("field1", LongType), StructField("field2", StringType))),
        "c" -> MapType(StringType, DoubleType)
      )
      .yt("/home/sashbel/data/test_write_complex")

    res.columns should contain theSameElementsAs Seq("a", "b", "c")
    res.select("a", "b", "c").collect() should contain theSameElementsAs Seq(
      Row(Seq(1, 2, 3), Row(1, "a"), Map("1" -> 0.1)),
      Row(Seq(4, 5, 6), Row(2, null), Map("2" -> 0.3))
    )
  }

  it should "read any as binary if no schema hint provided" in {
    val res = spark.read.yt("/home/sashbel/data/test_read_complex")

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
    val res = spark.read.yt("/home/sashbel/data/test_read_atomic")
    res.count() shouldEqual 2
  }

  it should "optimize table for scan" in {
    import spark.implicits._

    Seq(1, 2, 3).toDF.coalesce(1).write.optimizeFor(OptimizeMode.Scan).yt(tmpPath)

    YtTableUtils.tableAttribute(tmpPath, "optimize_for").stringValue() shouldEqual "scan"
  }

  it should "kill transaction when failed because of timeout" in {
    import spark.implicits._
    spark.sqlContext.setConf("spark.yt.timeout", "5")
    spark.sqlContext.setConf("spark.yt.write.timeout", "0")

    a[SparkException] shouldBe thrownBy {
      Seq(1, 2, 3).toDS.coalesce(1).write.yt(tmpPath)
    }

    YtTableUtils.exists(s"$tmpPath-tmp") shouldEqual false
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

