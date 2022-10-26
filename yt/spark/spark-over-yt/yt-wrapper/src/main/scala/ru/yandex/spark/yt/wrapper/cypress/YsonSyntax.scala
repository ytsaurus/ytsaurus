package ru.yandex.spark.yt.wrapper.cypress

import ru.yandex.inside.yt.kosher.ytree.YTreeNode

object YsonSyntax {
  implicit class Ysonable[T](t: T) {
    def toYson(implicit w: YsonWriter[T]): YTreeNode = w.toYson(t)
  }
}
