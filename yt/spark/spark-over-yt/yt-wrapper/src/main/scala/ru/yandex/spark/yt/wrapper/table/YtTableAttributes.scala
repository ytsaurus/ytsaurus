package ru.yandex.spark.yt.wrapper.table

import ru.yandex.inside.yt.kosher.ytree.YTreeNode
import ru.yandex.spark.yt.wrapper.cypress.{YtAttributes, YtCypressUtils}
import ru.yandex.yt.ytclient.proxy.{CompoundClient, YtClient}

trait YtTableAttributes {
  self: YtCypressUtils =>

  def rowCount(path: String, transaction: Option[String] = None)(implicit yt: CompoundClient): Long = {
    attribute(path, YtAttributes.rowCount, transaction).longValue()
  }

  def rowCount(attrs: Map[String, YTreeNode]): Long = {
    attrs(YtAttributes.rowCount).longValue()
  }

  def chunkCount(path: String, transaction: Option[String] = None)(implicit yt: CompoundClient): Int = {
    attribute(path, YtAttributes.chunkCount, transaction).longValue().toInt
  }

  def chunkCount(attrs: Map[String, YTreeNode]): Int = {
    attrs(YtAttributes.chunkCount).longValue().toInt
  }

  def optimizeMode(path: String, transaction: Option[String] = None)(implicit yt: CompoundClient): OptimizeMode = {
    optimizeMode(attribute(path, YtAttributes.optimizeFor, transaction))
  }

  def optimizeMode(node: YTreeNode): OptimizeMode = {
    OptimizeMode.fromName(node.stringValue())
  }

  def optimizeMode(attrs: Map[String, YTreeNode]): OptimizeMode = {
    optimizeMode(attrs(YtAttributes.optimizeFor))
  }

  def tableType(path: String, transaction: Option[String] = None)(implicit yt: CompoundClient): TableType = {
    tableType(attribute(path, YtAttributes.dynamic, transaction))
  }

  def tableType(node: YTreeNode): TableType = {
    if (node.boolValue()) TableType.Dynamic else TableType.Static
  }

  def tableType(attrs: Map[String, YTreeNode]): TableType = {
    tableType(attrs(YtAttributes.dynamic))
  }

  def dataWeight(attrs: Map[String, YTreeNode]): Long = {
   attrs(YtAttributes.dataWeight).longValue()
  }
}
