package ru.yandex.spark.yt.wrapper.cypress

import tech.ytsaurus.ysontree.YTreeNode

trait YsonWriter[T] {
  def toYson(t: T): YTreeNode
}
