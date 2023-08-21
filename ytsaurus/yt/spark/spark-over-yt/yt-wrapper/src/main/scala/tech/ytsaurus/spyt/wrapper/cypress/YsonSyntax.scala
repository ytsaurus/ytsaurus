package tech.ytsaurus.spyt.wrapper.cypress

import tech.ytsaurus.ysontree.YTreeNode

object YsonSyntax {
  implicit class Ysonable[T](t: T) {
    def toYson(implicit w: YsonWriter[T]): YTreeNode = w.toYson(t)
  }
}
