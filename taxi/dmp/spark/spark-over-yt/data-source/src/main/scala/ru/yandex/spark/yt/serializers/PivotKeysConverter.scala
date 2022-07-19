package ru.yandex.spark.yt.serializers

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import org.apache.spark.sql.types.{DataType, StructType}
import ru.yandex.bolts.collection.Cf
import ru.yandex.inside.yt.kosher.cypress.RangeLimit
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTree
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.YTreeBinarySerializer
import ru.yandex.inside.yt.kosher.ytree.YTreeNode
import ru.yandex.spark.yt.common.utils.{ExpressionTransformer, TuplePoint}

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

  def toByteArray(pivotKey: Map[String, YTreeNode]): Array[Byte] = {
    val node = YTree.builder
    node.beginMap
    pivotKey.foreach { case (k, v) => node.key(k).value(v) }
    node.endMap
    val rNode = node.build()

    val os = new ByteArrayOutputStream()
    YTreeBinarySerializer.serialize(rNode, os)
    try {
      os.toByteArray
    } finally {
      os.close()
    }
  }

  def toList(pivotKey: Array[Byte], columnsOrder: Seq[String]): Seq[YTreeNode] = {
    columnsOrder.flatMap(toMap(pivotKey).get)
  }

  def toPoint(pivotKey: Array[Byte], columnsOrder: Seq[String]): Option[TuplePoint] = {
    if (pivotKey.isEmpty) {
      None
    } else {
      val nodes = toList(pivotKey, columnsOrder)
      if (nodes.forall(ExpressionTransformer.isSupportedNodeType)) {
        Some(TuplePoint(nodes.map(ExpressionTransformer.nodeToPoint)))
      } else {
        None
      }
    }
  }

  def toRangeLimit(pivotKey: Array[Byte], columnsOrder: Seq[String]): RangeLimit = {
    toRangeLimit(toList(pivotKey, columnsOrder))
  }

  def toRangeLimit(list: Seq[YTreeNode]): RangeLimit = {
    RangeLimit.builder.setKey(Cf.list[YTreeNode](list:_*)).build()
  }
}
