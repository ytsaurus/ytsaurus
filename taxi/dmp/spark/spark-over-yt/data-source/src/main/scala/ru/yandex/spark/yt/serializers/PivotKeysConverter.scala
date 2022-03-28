package ru.yandex.spark.yt.serializers

import java.io.ByteArrayInputStream

import org.apache.spark.sql.types.{DataType, StructType}
import ru.yandex.bolts.collection.Cf
import ru.yandex.inside.yt.kosher.cypress.RangeLimit
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.YTreeBinarySerializer
import ru.yandex.inside.yt.kosher.ytree.YTreeNode

object PivotKeysConverter {
  def toMap(pivotKey: Array[Byte]): Map[String, YTreeNode] = {
    import scala.collection.JavaConverters._
    val is = new ByteArrayInputStream(pivotKey)
    try {
      YTreeBinarySerializer.deserialize(is).asMap().asScala.toMap
    } finally {
      is.close()
    }
  }

  def toList(pivotKey: Array[Byte], columnsOrder: Seq[String]): Seq[YTreeNode] = {
    columnsOrder.flatMap(toMap(pivotKey).get)
  }

  def toRangeLimit(pivotKey: Array[Byte], columnsOrder: Seq[String]): RangeLimit = {
    toRangeLimit(toList(pivotKey, columnsOrder))
  }

  def toRangeLimit(list: Seq[YTreeNode]): RangeLimit = {
    new RangeLimit(Cf.list[YTreeNode](list:_*), -1, -1)
  }
}
