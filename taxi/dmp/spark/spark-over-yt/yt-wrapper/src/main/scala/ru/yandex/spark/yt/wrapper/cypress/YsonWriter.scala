package ru.yandex.spark.yt.wrapper.cypress

import ru.yandex.inside.yt.kosher.ytree.YTreeNode

trait YsonWriter[T] {
  def toYson(t: T): YTreeNode
}
