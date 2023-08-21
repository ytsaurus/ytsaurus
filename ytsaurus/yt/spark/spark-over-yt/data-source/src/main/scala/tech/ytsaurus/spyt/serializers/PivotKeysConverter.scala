package tech.ytsaurus.spyt.serializers

import tech.ytsaurus.core.cypress.RangeLimit
import tech.ytsaurus.spyt.common.utils.TuplePoint
import tech.ytsaurus.spyt.wrapper.YtWrapper.PivotKey
import tech.ytsaurus.spyt.common.utils.{ExpressionTransformer, MInfinity, PInfinity, Point, RealValue}
import tech.ytsaurus.ysontree.{YTree, YTreeBinarySerializer, YTreeBooleanNodeImpl, YTreeDoubleNodeImpl, YTreeEntityNodeImpl, YTreeIntegerNodeImpl, YTreeNode, YTreeStringNodeImpl}

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

object PivotKeysConverter {
  def toMap(pivotKey: PivotKey): Map[String, YTreeNode] = {
    import scala.collection.JavaConverters._
    val is = new ByteArrayInputStream(pivotKey)
    try {
      YTreeBinarySerializer.deserialize(is).asMap().asScala.toMap
    } finally {
      is.close()
    }
  }

  def toByteArray(pivotKey: Map[String, YTreeNode]): PivotKey = {
    val node = YTree.builder
    node.beginMap
    pivotKey.foreach { case (k, v) => node.key(k).value(v) }
    node.endMap
    toByteArray(node.build())
  }

  def toByteArray(pivotKey: Seq[YTreeNode]): PivotKey = {
    val node = YTree.builder
    node.beginList
    pivotKey.foreach(v => node.value(v))
    node.endList
    toByteArray(node.build())
  }

  private def toByteArray(node: YTreeNode): PivotKey = {
    val os = new ByteArrayOutputStream()
    YTreeBinarySerializer.serialize(node, os)
    try {
      os.toByteArray
    } finally {
      os.close()
    }
  }

  def toList(pivotKey: PivotKey, columnsOrder: Seq[String]): Seq[YTreeNode] = {
    columnsOrder.flatMap(toMap(pivotKey).get)
  }

  def toList(pivotKey: PivotKey): Seq[YTreeNode] = {
    import scala.collection.JavaConverters._
    val is = new ByteArrayInputStream(pivotKey)
    try {
      YTreeBinarySerializer.deserialize(is).asList().asScala.toList
    } finally {
      is.close()
    }
  }

  def toPoint(pivotKey: PivotKey, columnsOrder: Seq[String]): Option[TuplePoint] = {
    if (pivotKey.isEmpty) None
    else toPoint(toList(pivotKey, columnsOrder))
  }

  def toPoint(pivotKey: PivotKey): Option[TuplePoint] = {
    if (pivotKey.isEmpty) None
    else toPoint(toList(pivotKey))
  }

  def toPoint(pivotKey: Seq[YTreeNode]): Option[TuplePoint] = {
    if (pivotKey.forall(ExpressionTransformer.isSupportedNodeType)) {
      Some(TuplePoint(pivotKey.map(ExpressionTransformer.nodeToPoint)))
    } else {
      None
    }
  }

  def toRangeLimit(pivotKey: PivotKey): RangeLimit = {
    toRangeLimit(toList(pivotKey))
  }

  def toRangeLimit(pivotKey: PivotKey, columnsOrder: Seq[String]): RangeLimit = {
    toRangeLimit(toList(pivotKey, columnsOrder))
  }

  def toRangeLimit(list: TuplePoint): RangeLimit = {
    toRangeLimit(list.points.map(prepareKey))
  }

  private def getSpecifiedEntity(value: String): YTreeNode = {
    new YTreeEntityNodeImpl(java.util.Map.of("type", new YTreeStringNodeImpl(value, null)))
  }

  private lazy val minimumKey: YTreeNode = getSpecifiedEntity("min")

  private lazy val maximumKey: YTreeNode = getSpecifiedEntity("max")

  def prepareKey(point: Point): YTreeNode = point match {
    case MInfinity() => minimumKey
    case PInfinity() => maximumKey
    case rValue: RealValue[_] if rValue.value == null =>
      new YTreeEntityNodeImpl(null)
    case rValue: RealValue[_] if rValue.value.isInstanceOf[Double] =>
      new YTreeDoubleNodeImpl(rValue.value.asInstanceOf[Double], null)
    case rValue: RealValue[_] if rValue.value.isInstanceOf[Long] =>
      new YTreeIntegerNodeImpl(true, rValue.value.asInstanceOf[Long], null)
    case rValue: RealValue[_] if rValue.value.isInstanceOf[Boolean] =>
      new YTreeBooleanNodeImpl(rValue.value.asInstanceOf[Boolean], null)
    case rValue: RealValue[_] if rValue.value.isInstanceOf[String] =>
      new YTreeStringNodeImpl(rValue.value.asInstanceOf[String], null)
  }

  def toRangeLimit(list: Seq[YTreeNode]): RangeLimit = {
    RangeLimit.builder.setKey(list: _*).build()
  }
}
