package tech.ytsaurus.spyt.serializers

import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.{FlatSpec, Matchers}
import tech.ytsaurus.client.rows.{UnversionedRow, UnversionedRowset, UnversionedValue}
import tech.ytsaurus.core.tables.{ColumnValueType, TableSchema}
import tech.ytsaurus.typeinfo.TiType

class UnversionedRowsetDeserializerTest extends FlatSpec with Matchers {
  private def deserialize(sparkSchema: StructType, rowset: UnversionedRowset): Seq[Seq[Any]] = {
    val deserializer = new UnversionedRowsetDeserializer(sparkSchema)
    val internalRows = deserializer.deserializeRowset(rowset)
    internalRows.map(_.toSeq(sparkSchema)).toSeq
  }

  it should "deserialize nulls" in {
    import scala.collection.JavaConverters._
    val tableSchema = TableSchema.builder()
      .addValue("a", TiType.int64()).addValue("b", TiType.string())
      .build()
    val rows = Seq(
      new UnversionedRow(Seq(
        new UnversionedValue(0, ColumnValueType.NULL, false, null),
        new UnversionedValue(1, ColumnValueType.STRING, false, "".getBytes("UTF8")),
      ).asJava),
      new UnversionedRow(Seq(
        new UnversionedValue(0, ColumnValueType.INT64, false, Int.MaxValue.toLong),
        new UnversionedValue(1, ColumnValueType.NULL, false, null),
      ).asJava),
    )
    val rowset = new UnversionedRowset(tableSchema, rows.asJava)
    val sparkSchema = StructType(Seq(StructField("a", LongType), StructField("b", StringType)))
    val result = deserialize(sparkSchema, rowset)
    result should contain theSameElementsAs Seq(
      Array(null, UTF8String.fromString("")),
      Array(Int.MaxValue.toLong, null)
    )
  }

  it should "deserialize primitive types" in {
    import scala.collection.JavaConverters._
    val tableSchema = TableSchema.builder()
      .addValue("a", TiType.int64()).addValue("b", TiType.string()).addValue("c", TiType.bool())
      .build()
    val rows = Seq(
      new UnversionedRow(Seq(
        new UnversionedValue(0, ColumnValueType.INT64, false, 1L),
        new UnversionedValue(1, ColumnValueType.STRING, false, "text".getBytes("UTF8")),
        new UnversionedValue(2, ColumnValueType.BOOLEAN, false, true),
      ).asJava),
      new UnversionedRow(Seq(
        new UnversionedValue(0, ColumnValueType.INT64, false, -768L),
        new UnversionedValue(1, ColumnValueType.STRING, false, "string".getBytes("UTF8")),
        new UnversionedValue(2, ColumnValueType.BOOLEAN, false, false),
      ).asJava),
    )
    val rowset = new UnversionedRowset(tableSchema, rows.asJava)
    val sparkSchema = StructType(
      Seq(StructField("a", LongType), StructField("b", StringType), StructField("c", BooleanType)))
    val result = deserialize(sparkSchema, rowset)
    result should contain theSameElementsAs Seq(
      Array(1L, UTF8String.fromString("text"), true),
      Array(-768L, UTF8String.fromString("string"), false)
    )
  }

  it should "deserialize part of columns" in {
    import scala.collection.JavaConverters._
    val tableSchema = TableSchema.builder()
      .addValue("a", TiType.floatType()).addValue("b", TiType.bool()).addValue("c", TiType.int32())
      .build()
    val rows = Seq(
      new UnversionedRow(Seq(
        new UnversionedValue(0, ColumnValueType.DOUBLE, false, 0.13),
        new UnversionedValue(1, ColumnValueType.BOOLEAN, false, false),
        new UnversionedValue(2, ColumnValueType.INT64, false, 5L),
      ).asJava),
      new UnversionedRow(Seq(
        new UnversionedValue(0, ColumnValueType.DOUBLE, false, 2.0),
        new UnversionedValue(1, ColumnValueType.BOOLEAN, false, true),
        new UnversionedValue(2, ColumnValueType.INT64, false, 0L),
      ).asJava),
    )
    val rowset = new UnversionedRowset(tableSchema, rows.asJava)
    val sparkSchema = StructType(Seq(StructField("b", BooleanType), StructField("c", IntegerType)))
    val result = deserialize(sparkSchema, rowset)
    result should contain theSameElementsAs Seq(
      Array(false, 5),
      Array(true, 0)
    )
  }

  it should "deserialize complex types" in {
    import scala.collection.JavaConverters._
    val tableSchema = TableSchema.builder()
      .addValue("c1", TiType.dict(TiType.int32(), TiType.string())).addValue("c2", TiType.list(TiType.doubleType()))
      .build()
    val rows = Seq(
      new UnversionedRow(Seq(
        new UnversionedValue(0, ColumnValueType.COMPOSITE, false,
          YsonRowConverter.serialize(Map(1 -> "row_0_0", 2 -> "row_0_1"), MapType(IntegerType, StringType), false)),
        new UnversionedValue(1, ColumnValueType.COMPOSITE, false,
          YsonRowConverter.serialize(Seq(1.2, 1.6), ArrayType(DoubleType), false)),
      ).asJava),
      new UnversionedRow(Seq(
        new UnversionedValue(0, ColumnValueType.COMPOSITE, false,
          YsonRowConverter.serialize(Map(3 -> "row_1_0", 4 -> "row_1_1"), MapType(IntegerType, StringType), false)),
        new UnversionedValue(1, ColumnValueType.COMPOSITE, false,
          YsonRowConverter.serialize(Seq(0.0, 0.5, 2.12), ArrayType(DoubleType), false)),
      ).asJava),
    )
    val rowset = new UnversionedRowset(tableSchema, rows.asJava)
    val sparkSchema = StructType(
      Seq(StructField("c1", MapType(IntegerType, StringType)), StructField("c2", ArrayType(DoubleType))))
    val result = deserialize(sparkSchema, rowset)
    val castedResult = result.map {
      case Seq(map: MapData, array: ArrayData) =>
        val castedMap = map.keyArray.toSeq[UTF8String](StringType).map(_.toString.toInt)
          .zip(map.valueArray.toSeq[String](StringType)).toMap
        val castedArray = array.toSeq[Double](DoubleType)
        (castedMap, castedArray)
    }
    castedResult should contain theSameElementsAs Seq(
      (Map(1 -> UTF8String.fromString("row_0_0"), 2 -> UTF8String.fromString("row_0_1")), Seq(1.2, 1.6)),
      (Map(3 -> UTF8String.fromString("row_1_0"), 4 -> UTF8String.fromString("row_1_1")), Seq(0.0, 0.5, 2.12)),
    )
  }
}
