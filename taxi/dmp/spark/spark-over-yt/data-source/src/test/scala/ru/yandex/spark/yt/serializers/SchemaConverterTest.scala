package ru.yandex.spark.yt.serializers

import org.apache.spark.sql.types._
import org.apache.spark.sql.yson.{UInt64Type, YsonType}
import org.mockito.scalatest.MockitoSugar
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}
import ru.yandex.spark.yt.format.conf.SparkYtConfiguration.Read.ParsingTypeV3
import ru.yandex.spark.yt.test.{LocalSpark, TestUtils, TmpDir}
import ru.yandex.spark.yt.{SchemaTestUtils, YtReader}
import ru.yandex.type_info.StructType.Member
import ru.yandex.type_info.TiType
import ru.yandex.yt.ytclient.tables.{ColumnValueType, TableSchema}

class SchemaConverterTest extends FlatSpec with Matchers
  with TestUtils with TmpDir with LocalSpark with SchemaTestUtils with MockitoSugar with TableDrivenPropertyChecks {
  behavior of "SchemaConverter"

  it should "convert yt schema to spark one" in {
    val schema = new TableSchema.Builder()
      .setUniqueKeys(false)
      .addKey("a", ColumnValueType.STRING)
      .addKey("c.e", ColumnValueType.INT64)
      .addValue("d", ColumnValueType.INT64)
      .build()
    val res = SchemaConverter.sparkSchema(schema.toYTree)
    res shouldBe StructType(Seq(
      structField("a", StringType, keyId = 0, nullable = true),
      structField("c_e", LongType, originalName = Some("c.e"), 1, nullable = true),
      structField("d", LongType, nullable = true)
    ))
  }

  private val schema = new TableSchema.Builder().setUniqueKeys(false)
    .addValue("NULL", ColumnValueType.NULL)
    .addValue("INT64", ColumnValueType.INT64)
    .addValue("int64_3", TiType.int64())
    .addValue("UINT64", ColumnValueType.UINT64)
    .addValue("uint64_3", TiType.uint64())
    .addValue("floatType", TiType.floatType())
    .addValue("DOUBLE", ColumnValueType.DOUBLE)
    .addValue("doubleType", TiType.doubleType())
    .addValue("BOOLEAN", ColumnValueType.BOOLEAN)
    .addValue("bool", TiType.bool())
    .addValue("STRING", ColumnValueType.STRING)
    .addValue("string_3", TiType.string())
    .addValue("ANY", ColumnValueType.ANY)
    .addValue("yson", TiType.optional(TiType.yson()))
    .addValue("int8", TiType.int8())
    .addValue("uint8", TiType.uint8())
    .addValue("int16", TiType.int16())
    .addValue("uint16", TiType.uint16())
    .addValue("int32", TiType.int32())
    .addValue("uint32", TiType.uint32())
    .addValue("utf8", TiType.utf8())
    .addValue("date", TiType.date())
    .addValue("datetime", TiType.datetime())
    .addValue("timestamp", TiType.timestamp())
    .addValue("interval", TiType.interval())
    .addValue("list", TiType.list(TiType.bool()))
    .addValue("dict", TiType.dict(TiType.doubleType(), TiType.string()))
    .addValue("struct", TiType.struct(new Member("a", TiType.string()), new Member("b", TiType.optional(TiType.yson()))))
    .addValue("tuple", TiType.tuple(TiType.bool(), TiType.date()))
    .addValue("variantOverStruct",
      TiType.variantOverStruct(java.util.List.of[Member](new Member("c", TiType.uint16()), new Member("d", TiType.timestamp()))))
    .addValue("variantOverTuple", TiType.variantOverTuple(TiType.floatType(), TiType.interval()))
    .build()

  it should "read schema without parsing type v3" in {
    // in sparkSchema.toYTree no type_v1 type names
    spark.conf.set(s"spark.yt.${ParsingTypeV3.name}", value = false)
    createEmptyTable(tmpPath, schema)
    val res = spark.read.yt(tmpPath).schema

    spark.conf.set(s"spark.yt.${ParsingTypeV3.name}", value = true)
    val res2 = spark.read.option("parsingtypev3", "false").yt(tmpPath).schema

    res shouldBe res2
    res shouldBe StructType(Seq(
      structField("NULL", NullType, nullable = true),
      structField("INT64", LongType, nullable = true),
      structField("int64_3", LongType, nullable = true),
      structField("UINT64", UInt64Type, nullable = true),
      structField("uint64_3", UInt64Type, nullable = true),
      structField("floatType", FloatType, nullable = true),
      structField("DOUBLE", DoubleType, nullable = true),
      structField("doubleType", DoubleType, nullable = true),
      structField("BOOLEAN", BooleanType, nullable = true),
      structField("bool", BooleanType, nullable = true),
      structField("STRING", StringType, nullable = true),
      structField("string_3", StringType, nullable = true),
      structField("ANY", YsonType, nullable = true),
      structField("yson", YsonType, nullable = true),
      structField("int8", ByteType, nullable = true),
      structField("uint8", ShortType, nullable = true),
      structField("int16", ShortType, nullable = true),
      structField("uint16", IntegerType, nullable = true),
      structField("int32", IntegerType, nullable = true),
      structField("uint32", LongType, nullable = true),
      structField("utf8", StringType, nullable = true),
      structField("date", DateType, nullable = true),
      structField("datetime", TimestampType, nullable = true),
      structField("timestamp", LongType, nullable = true),
      structField("interval", LongType, nullable = true),
      structField("list", YsonType, nullable = true),
      structField("dict", YsonType, nullable = true),
      structField("struct", YsonType, nullable = true),
      structField("tuple", YsonType, nullable = true),
      structField("variantOverStruct", YsonType, nullable = true),
      structField("variantOverTuple", YsonType, nullable = true)
    ))
  }

  it should "convert supported types with parsing type v3" in {
    val res = SchemaConverter.sparkSchema(schema.toYTree, parsingTypeV3 = true)
    res shouldBe StructType(Seq(
      structField("NULL", NullType, nullable = false),
      structField("INT64", LongType, nullable = true),
      structField("int64_3", LongType, nullable = false),
      structField("UINT64", UInt64Type, nullable = true),
      structField("uint64_3", UInt64Type, nullable = false),
      structField("floatType", FloatType, nullable = false),
      structField("DOUBLE", DoubleType, nullable = true),
      structField("doubleType", DoubleType, nullable = false),
      structField("BOOLEAN", BooleanType, nullable = true),
      structField("bool", BooleanType, nullable = false),
      structField("STRING", StringType, nullable = true),
      structField("string_3", StringType, nullable = false),
      structField("ANY", YsonType, nullable = true),
      structField("yson", YsonType, nullable = true),
      structField("int8", ByteType, nullable = false),
      structField("uint8", ShortType, nullable = false),
      structField("int16", ShortType, nullable = false),
      structField("uint16", IntegerType, nullable = false),
      structField("int32", IntegerType, nullable = false),
      structField("uint32", LongType, nullable = false),
      structField("utf8", StringType, nullable = false),
      structField("date", DateType, nullable = false),
      structField("datetime", TimestampType, nullable = false),
      structField("timestamp", LongType, nullable = false),
      structField("interval", LongType, nullable = false),
      structField("list", ArrayType(BooleanType, containsNull = false), nullable = false),
      structField("dict", MapType(DoubleType, StringType, valueContainsNull = false), nullable = false),
      structField("struct", StructType(Seq(StructField("a", StringType, nullable = false), StructField("b", YsonType, nullable = true))), nullable = false),
      structField("tuple", StructType(Seq(StructField("_1", BooleanType, nullable = false), StructField("_2", DateType, nullable = false))), nullable = false),
      structField("variantOverStruct", StructType(Seq(StructField("_vc", IntegerType, nullable = false), StructField("_vd", LongType, nullable = false))), nullable = false),
      structField("variantOverTuple", StructType(Seq(StructField("_v_1", FloatType, nullable = false), StructField("_v_2", LongType, nullable = false))), nullable = false)
    ))
  }

  it should "use schema hint" in {
    val schema = new TableSchema.Builder()
      .setUniqueKeys(false)
      .addKey("a", ColumnValueType.STRING)
      .addValue("b", ColumnValueType.INT64)
      .build()
    val res = SchemaConverter.sparkSchema(schema.toYTree,
      Some(StructType(Seq(structField("a", UInt64Type, Some("x"), 1, nullable = false))))
    )
    res shouldBe StructType(Seq(
      structField("a", UInt64Type, keyId = 0, nullable = false),
      structField("b", LongType, nullable = true)
    ))
  }
}
