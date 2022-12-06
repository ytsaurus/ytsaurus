package ru.yandex.spark.yt.serializers

import ru.yandex.spark.yt.common.utils.{ExpressionTransformer, TuplePoint}
import tech.ytsaurus.core.cypress.RangeLimit
import tech.ytsaurus.ysontree.{YTree, YTreeBinarySerializer, YTreeNode}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

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
    RangeLimit.builder.setKey(list: _*).build()
  }
}
