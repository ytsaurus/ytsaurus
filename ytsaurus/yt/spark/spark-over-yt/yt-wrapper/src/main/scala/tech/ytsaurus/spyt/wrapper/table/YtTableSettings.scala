package tech.ytsaurus.spyt.wrapper.table

import tech.ytsaurus.ysontree.{YTreeBuilder, YTreeNode}

trait YtTableSettings {
  def ytSchema: YTreeNode

  def optionsAny: Map[String, Any]

  def options: Map[String, YTreeNode] = {
    optionsAny.mapValues{v => new YTreeBuilder().value(v).build()} + ("schema" -> ytSchema)
  }
}
