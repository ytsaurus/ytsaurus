package ru.yandex.spark.yt.serializers

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, MapData}
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.{FlatSpec, Matchers}
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTree
import ru.yandex.inside.yt.kosher.ytree.YTreeNode
import ru.yandex.type_info.TiType

import scala.util.Random

class YsonRowConverterTest extends FlatSpec with Matchers {

  behavior of "YsonRowConverterTest"

  it should "sortMapData" in {
    def mapData(map: Seq[(String, String)]): MapData = {
      val utf8Map = stringToUtf8Map(map)
      val keys = ArrayData.toArrayData(utf8Map.map(_._1).toArray)
      val values = ArrayData.toArrayData(utf8Map.map(_._2).toArray)
      new ArrayBasedMapData(keys, values)
    }

    def stringToUtf8Map(map: Seq[(String, String)]): Seq[(UTF8String, UTF8String)] = {
      map.map { case (x, y) => UTF8String.fromString(x) -> UTF8String.fromString(y) }
    }

    def utf8ToStringMap(map: Iterable[(Any, Any)]): Seq[(String, String)] = {
      map
        .asInstanceOf[Iterable[(UTF8String, UTF8String)]].toList
        .map { case (x, y) => (x.toString, y.toString) }
    }

    def stringMap(map: Iterable[(Any, Any)]): Seq[(String, String)] = {
      map.asInstanceOf[Iterable[(String, String)]].toList
    }

    def check(data: Seq[(String, String)]): Unit = {
      val sorted = data.sortBy(_._1)

      val result = utf8ToStringMap(YsonRowConverter.sortMapData(mapData(data), StringType))
      val result2 = stringMap(YsonRowConverter.sortMapData(data.toMap, StringType))
      val result3 = utf8ToStringMap(YsonRowConverter.sortMapData(stringToUtf8Map(data).toMap, StringType))

      result should contain theSameElementsInOrderAs sorted
      result2 should contain theSameElementsInOrderAs sorted
      result3 should contain theSameElementsInOrderAs sorted
    }

    val r = new Random()

    check(Nil)
    check(Seq("1" -> "a", "2" -> "b", "3" -> "c"))
    check(Seq("2" -> "b", "1" -> "a", "3" -> "c"))
    check(Seq.fill(100)(r.nextInt().toString -> r.nextPrintableChar().toString).toMap.toSeq)
  }

  private val sparkStruct = StructType(Seq(StructField("a", DoubleType), StructField("b", BooleanType)))
  private val sparkTupleStruct = StructType(Seq(StructField("_1", IntegerType), StructField("_2", StringType)))
  private val ytTupleStruct = TiType.tuple(TiType.int32(), TiType.string())
  private val ytVariantOverTuple = TiType.variantOverTuple(TiType.tuple(TiType.int32(), TiType.string()))

  private def buildList(values: Any*): YTreeNode = {
    val builder = YTree.builder().beginList()
    values.foreach {
      v =>
        builder.onListItem()
        builder.value(v)
    }
    builder.endList().build()
  }

  private def buildMapAsDict(values: (String, Any)*): YTreeNode = {
    val builder = YTree.builder().beginMap()
    values.foreach {
      v =>
        builder.key(v._1)
        builder.value(v._2)
    }
    builder.endMap().build()
  }

  private def buildMapAsList(values: (Any, Any)*): YTreeNode = {
    val builder = YTree.builder().beginList()
    values.foreach {
      v =>
        builder.onListItem()
        builder.value(buildList(v._1, v._2))
    }
    builder.endList().build()
  }

  private def serializeStruct(converter: YsonRowConverter, row: Row): YTreeNode = {
    val builder = YTree.builder()
    converter.serializeStruct(row, builder)
    builder.build()
  }

  it should "convert struct" in {
    val converter = YsonRowConverter.getOrCreate(
      sparkStruct, config = YsonEncoderConfig(skipNulls = false, typeV3Format = false))
    serializeStruct(converter, Row(5.0, true)) shouldBe buildMapAsDict(("a", 5.0), ("b", true))
  }

  it should "convert struct with type v3" in {
    val converter = YsonRowConverter.getOrCreate(
      sparkStruct, config = YsonEncoderConfig(skipNulls = false, typeV3Format = true))
    serializeStruct(converter, Row(5.0, true)) shouldBe buildMapAsDict(("a", 5.0), ("b", true))
  }

  it should "convert variant with type v3" in {
    val converter = YsonRowConverter.getOrCreate(
      sparkStruct, YtTypeHolder(ytVariantOverTuple), YsonEncoderConfig(skipNulls = false, typeV3Format = true))
    serializeStruct(converter, Row(4.0, null)) shouldBe buildList(0, 4.0)
    serializeStruct(converter, Row(null, false)) shouldBe buildList(1, false)
  }

  it should "convert tuple" in {
    val converter = YsonRowConverter.getOrCreate(
      sparkTupleStruct, config = YsonEncoderConfig(skipNulls = false, typeV3Format = false))
    serializeStruct(converter, Row(1, "str")) shouldBe buildMapAsDict(("_1", 1), ("_2", "str"))
  }

  it should "convert tuple with type v3" in {
    val converter = YsonRowConverter.getOrCreate(
      sparkTupleStruct, YtTypeHolder(ytTupleStruct), config = YsonEncoderConfig(skipNulls = false, typeV3Format = true))
    serializeStruct(converter, Row(1, "str")) shouldBe buildList(1, "str")
  }
}
