package ru.yandex.spark.yt.serializers

import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, MapData}
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.{FlatSpec, Matchers}

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

}
