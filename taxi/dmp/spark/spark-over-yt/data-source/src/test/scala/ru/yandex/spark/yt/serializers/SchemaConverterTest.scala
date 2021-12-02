package ru.yandex.spark.yt.serializers

import org.apache.spark.sql.types._
import org.apache.spark.sql.yson.{UInt64Type, YsonType}
import org.mockito.scalatest.MockitoSugar
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}
import ru.yandex.spark.yt.SchemaTestUtils
import ru.yandex.spark.yt.test.TestUtils
import ru.yandex.type_info.TiType
import ru.yandex.yt.ytclient.tables.{ColumnValueType, TableSchema}

class SchemaConverterTest extends FlatSpec with Matchers
  with TestUtils with SchemaTestUtils with MockitoSugar with TableDrivenPropertyChecks {
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
      structField("a", StringType, keyId = 0),
      structField("c_e", LongType, originalName = Some("c.e"), 1),
      structField("d", LongType)
    ))
  }

  it should "convert supported types" in {
    val schema = new TableSchema.Builder().setUniqueKeys(false)
      .addValue("NULL", ColumnValueType.NULL)
      .addValue("INT64", ColumnValueType.INT64)
      .addValue("UINT64", ColumnValueType.UINT64)
      .addValue("floatType", TiType.floatType())
      .addValue("DOUBLE", ColumnValueType.DOUBLE)
      .addValue("BOOLEAN", ColumnValueType.BOOLEAN)
      .addValue("STRING", ColumnValueType.STRING)
      .addValue("ANY", ColumnValueType.ANY)
      .addValue("yson", TiType.yson())
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
      .addValue("list", TiType.list(TiType.floatType()))
      .addValue("list2", TiType.list(TiType.list(TiType.date())))
      .addValue("list3", TiType.list(TiType.dict(TiType.int32(), TiType.doubleType())))
      .addValue("dict", TiType.dict(TiType.string(), TiType.bool()))
      .addValue("dict2", TiType.dict(TiType.int64(), TiType.list(TiType.uint32())))
      .build()

    val res = SchemaConverter.sparkSchema(schema.toYTree)
    res shouldBe StructType(Seq(
      structField("NULL", NullType),
      structField("INT64", LongType),
      structField("UINT64", UInt64Type),
      structField("floatType", FloatType),
      structField("DOUBLE", DoubleType),
      structField("BOOLEAN", BooleanType),
      structField("STRING", StringType),
      structField("ANY", YsonType),
      structField("yson", YsonType),
      structField("int8", ByteType),
      structField("uint8", ShortType),
      structField("int16", ShortType),
      structField("uint16", IntegerType),
      structField("int32", IntegerType),
      structField("uint32", LongType),
      structField("utf8", StringType),
      structField("date", DateType),
      structField("datetime", TimestampType),
      structField("timestamp", LongType),
      structField("interval", LongType),
      structField("list", YsonType),
      structField("list2", YsonType),
      structField("list3", YsonType),
      structField("dict", YsonType),
      structField("dict2", YsonType)
    ))
  }

  it should "use schema hint" in {
    val schema = new TableSchema.Builder()
      .setUniqueKeys(false)
      .addKey("a", ColumnValueType.STRING)
      .addValue("b", ColumnValueType.INT64)
      .build()
    val res = SchemaConverter.sparkSchema(schema.toYTree,
      Some(StructType(Seq(structField("a", UInt64Type, Some("x"), 1))))
    )
    res shouldBe StructType(Seq(
      structField("a", UInt64Type, keyId = 0),
      structField("b", LongType)
    ))
  }
}
