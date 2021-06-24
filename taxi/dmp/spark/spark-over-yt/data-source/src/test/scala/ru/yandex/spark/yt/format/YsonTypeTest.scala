package ru.yandex.spark.yt.format

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf.{CODEGEN_FACTORY_MODE, WHOLESTAGE_CODEGEN_ENABLED}
import org.apache.spark.sql.types._
import org.apache.spark.sql.yson.YsonType
import org.apache.spark.sql.{Encoders, Row}
import org.scalatest.{FlatSpec, Matchers}
import ru.yandex.inside.yt.kosher.ytree.YTreeNode
import ru.yandex.spark.yt._
import ru.yandex.spark.yt.format.tmp.Test
import ru.yandex.spark.yt.test.{LocalSpark, TestUtils, TmpDir}
import ru.yandex.spark.yt.wrapper.YtWrapper
import ru.yandex.spark.yt.wrapper.table.OptimizeMode

class YsonTypeTest extends FlatSpec with Matchers with LocalSpark with TmpDir with TestUtils {
  import spark.implicits._

  def collectBytes(path: String): Seq[Array[Byte]] = {
    spark.read.yt(path)
      .withColumn("value", 'value.cast(BinaryType))
      .as[Array[Byte]].collect()
  }

  def typeV3(path: String, fieldName: String): String = {
    val schema = YtWrapper.attribute(path, "schema")
    schema.asList()
      .find((t: YTreeNode) => t.asMap().getOrThrow("name").stringValue() == fieldName)
      .get().asMap()
      .getOrThrow("type_v3").asMap()
      .getOrThrow("item").stringValue()
  }

  def typeV2(path: String, fieldName: String): String = {
    val schema = YtWrapper.attribute(path, "schema")
    schema.asList()
      .find((t: YTreeNode) => t.asMap().getOrThrow("name").stringValue() == fieldName)
      .get().asMap()
      .getOrThrow("type_v2").asMap()
      .getOrThrow("element").stringValue()
  }

  def typeV1(path: String, fieldName: String): String = {
    val schema = YtWrapper.attribute(path, "schema")
    schema.asList()
      .find((t: YTreeNode) => t.asMap().getOrThrow("name").stringValue() == fieldName)
      .get().asMap()
      .getOrThrow("type").stringValue()
  }

  "YtFormat" should "read and write yson" in {
    val initial = Seq(Seq(1, 2, 3), Seq(4, 5, 6))
    initial.toDF().coalesce(1).write.yt(s"$tmpPath/1")
    val expected = collectBytes(s"$tmpPath/1")

    spark.read.yt(s"$tmpPath/1").write.yt(s"$tmpPath/2")
    val dfWithHint = spark.read.schemaHint("value" -> ArrayType(LongType)).yt(s"$tmpPath/2")
    dfWithHint.write.yt(s"$tmpPath/3")

    spark.read.yt(s"$tmpPath/1").schema.head.dataType shouldEqual YsonType
    collectBytes(s"$tmpPath/2") should contain theSameElementsAs expected
    collectBytes(s"$tmpPath/3") should contain theSameElementsAs expected
    dfWithHint.as[Array[Long]].collect() should contain theSameElementsAs initial
  }

  it should "cast yson to binary" in {
    Seq(Seq(1, 2, 3), Seq(4, 5, 6)).toDF().coalesce(1).write.yt(tmpPath)

    val res = spark.read.yt(tmpPath)
      .withColumn("value", 'value.cast(BinaryType))
      .as[Array[Byte]].collect()

    res should contain theSameElementsAs Seq(
      Array[Byte](91, 2, 2, 59, 2, 4, 59, 2, 6, 93),
      Array[Byte](91, 2, 8, 59, 2, 10, 59, 2, 12, 93)
    )
  }

  it should "cast binary to yson" in {
    Seq(
      Array[Byte](91, 2, 2, 59, 2, 4, 59, 2, 6, 93),
      Array[Byte](91, 2, 8, 59, 2, 10, 59, 2, 12, 93)
    ).toDF()
      .coalesce(1)
      .withColumn("value", 'value.cast(YsonType))
      .write.yt(tmpPath)
    val res = spark.read.schemaHint("value" -> ArrayType(LongType)).yt(tmpPath).as[Array[Long]]

    typeV1(tmpPath, "value") shouldEqual "any"
    res.collect().map(_.toList) should contain theSameElementsAs Seq(
      Seq(1L, 2L, 3L),
      Seq(4L, 5L, 6L)
    )
  }

  it should "write non-yson bytes as string" in {
    val initial = Seq(
      Array(1, 2, 3, 4, 5).map(_.toByte),
      Array(6, 7, 8, 9, 10).map(_.toByte)
    )
    initial.toDF().coalesce(1).write.yt(tmpPath)

    val res = spark.read.schemaHint("value" -> BinaryType).yt(tmpPath).as[Array[Byte]].collect()
    res should contain theSameElementsAs initial
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

    val rawSchema = spark.read.yt(tmpPath).schema
    val res = spark.read
      .schemaHint(
        "info" -> StructType(Seq(
          StructField("value1", StringType),
          StructField("value2", StringType)
        ))
      )
      .yt(tmpPath)

    rawSchema.find(_.name == "info").get.dataType shouldEqual YsonType
    res.collect() should contain theSameElementsAs Seq(
      Row("a", null, Row("a", null)),
      Row("b", null, Row("b", null)),
      Row("c", null, Row("c", null)),
      Row(null, "A", Row(null, "A")),
      Row(null, "B", Row(null, "B")),
      Row(null, "C", Row(null, "C"))
    )
  }

  it should "read any as yson if no schema hint provided" in {
    writeComplexTable(tmpPath)
    val res = spark.read.yt(tmpPath)

    res.columns should contain theSameElementsAs Encoders.product[Test].schema.fieldNames
    res.schema.map(_.dataType) should contain theSameElementsAs Encoders.product[Test].schema.map(_ => YsonType)
    res.collect().length shouldEqual 1
  }

  it should "cast yson in arrow to binary" in {
    Seq(Seq(1, 2, 3), Seq(4, 5, 6)).toDF().coalesce(1).write.optimizeFor(OptimizeMode.Scan).yt(tmpPath)

    val res = spark.read.yt(tmpPath)
      .withColumn("value", 'value.cast(BinaryType))
      .as[Array[Byte]].collect()

    res should contain theSameElementsAs Seq(
      Array[Byte](91, 2, 2, 59, 2, 4, 59, 2, 6, 93),
      Array[Byte](91, 2, 8, 59, 2, 10, 59, 2, 12, 93)
    )
  }

  it should "validate binary cast to yson" in {
    Logger.getRootLogger.setLevel(Level.OFF)
    an [Exception] should be thrownBy {
      Seq(
        Array[Byte](4, 5, 6),
        Array[Byte](7, 8, 9)
      ).toDF()
        .coalesce(1)
        .withColumn("value", 'value.cast(YsonType))
        .write.yt(tmpPath)
    }
    Logger.getRootLogger.setLevel(Level.WARN)
  }

  it should "broadcast dataframe with yson" in {
    Seq(1 -> Seq(1, 2, 3), 2 -> Seq(4, 5, 6)).toDF("key", "value1").coalesce(1).write.yt(s"$tmpPath/1")
    val df1 = spark.read.yt(s"$tmpPath/1")
    val df2 = Seq(1 -> "a", 2 -> "b").toDF("key", "value2")

    df2.join(broadcast(df1), Seq("key")).write.yt(s"$tmpPath/2")
    val res = spark.read.schemaHint("value1" -> ArrayType(LongType)).yt(s"$tmpPath/2")
      .select("key", "value1", "value2")
      .as[(Int, Array[Long], String)]
      .collect()
      .map{case (k, v1, v2) => (k, v1.toList, v2)}

    res should contain theSameElementsAs Seq(
      (1, Seq(1L, 2L, 3L), "a"),
      (2, Seq(4L, 5L, 6L), "b")
    )
    typeV1(s"$tmpPath/2", "value1") shouldEqual "any"
  }

  it should "prohibit yson type in complex types" in {
    Seq(1 -> Seq(1, 2, 3), 2 -> Seq(4, 5, 6)).toDF("key", "value1").coalesce(1).write.yt(s"$tmpPath/1")

    an [Exception] should be thrownBy {
      spark.read.yt(s"$tmpPath/1")
        .withColumn("value2", struct('key, 'value1))
        .write.yt(s"$tmpPath/2")
    }

    an [Exception] should be thrownBy {
      spark.read.yt(s"$tmpPath/1")
        .withColumn("value2", map('key, 'value1))
        .write.yt(s"$tmpPath/2")
    }

    an [Exception] should be thrownBy {
      spark.read.yt(s"$tmpPath/1")
        .withColumn("value2", array('value1))
        .write.yt(s"$tmpPath/2")
    }
  }

  it should "cast yson to binary with disabled codegen" in {
    spark.conf.set(WHOLESTAGE_CODEGEN_ENABLED.key, "false")
    spark.conf.set(CODEGEN_FACTORY_MODE.key, CodegenObjectFactoryMode.NO_CODEGEN.toString)
    sys.props += "spark.testing" -> "true"

    try {
      Seq(Seq(1, 2, 3), Seq(4, 5, 6)).toDF().coalesce(1).write.yt(tmpPath)

      val res = spark.read.yt(tmpPath)
        .withColumn("value", 'value.cast(BinaryType))
        .as[Array[Byte]].collect()

      res should contain theSameElementsAs Seq(
        Array[Byte](91, 2, 2, 59, 2, 4, 59, 2, 6, 93),
        Array[Byte](91, 2, 8, 59, 2, 10, 59, 2, 12, 93)
      )
    } finally {
      spark.conf.set(WHOLESTAGE_CODEGEN_ENABLED.key, "true")
      spark.conf.set(CODEGEN_FACTORY_MODE.key, CodegenObjectFactoryMode.FALLBACK.toString)
      sys.props -= "spark.testing"
    }
  }

  it should "cast binary to yson with disabled codegen" in {
    spark.conf.set(WHOLESTAGE_CODEGEN_ENABLED.key, "false")
    spark.conf.set(CODEGEN_FACTORY_MODE.key, CodegenObjectFactoryMode.NO_CODEGEN.toString)
    sys.props += "spark.testing" -> "true"

    try {
      Seq(
        Array[Byte](91, 2, 2, 59, 2, 4, 59, 2, 6, 93),
        Array[Byte](91, 2, 8, 59, 2, 10, 59, 2, 12, 93)
      ).toDF()
        .coalesce(1)
        .withColumn("value", 'value.cast(YsonType))
        .write.yt(tmpPath)

      sys.props -= "spark.testing"
      val res = spark.read.schemaHint("value" -> ArrayType(LongType)).yt(tmpPath).as[Array[Long]]

      typeV1(tmpPath, "value") shouldEqual "any"
      res.collect() should contain theSameElementsAs Seq(
        Seq(1L, 2L, 3L),
        Seq(4L, 5L, 6L)
      )
    } finally {
      spark.conf.set(WHOLESTAGE_CODEGEN_ENABLED.key, "true")
      spark.conf.set(CODEGEN_FACTORY_MODE.key, CodegenObjectFactoryMode.FALLBACK.toString)
      sys.props -= "spark.testing"
    }
  }
}
