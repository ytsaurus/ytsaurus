package ru.yandex.spark.yt.wrapper.table

import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTreeBuilder
import ru.yandex.inside.yt.kosher.ytree.YTreeNode

trait YtTableSettings {
  def ytSchema: YTreeNode

  def optionsAny: Map[String, Any]

  def options: Map[String, YTreeNode] = {
    optionsAny.mapValues{v => new YTreeBuilder().value(v).build()} + ("schema" -> ytSchema)
  }
}
