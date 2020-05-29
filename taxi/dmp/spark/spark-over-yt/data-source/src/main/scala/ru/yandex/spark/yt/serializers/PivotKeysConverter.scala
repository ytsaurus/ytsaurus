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

  def toRangeLimit(pivotKey: Array[Byte], columnsOrder: Seq[String]): RangeLimit = {
    val map = toMap(pivotKey)
    val list = columnsOrder.flatMap(map.get)
    new RangeLimit(Cf.list[YTreeNode](list:_*), -1, -1)
  }
}
