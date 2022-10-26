package ru.yandex.spark.yt.fs.eventlog

import ru.yandex.inside.yt.kosher.impl.ytree.serialization.YTreeTextSerializer
import ru.yandex.inside.yt.kosher.ytree.YTreeNode
import ru.yandex.spark.yt.wrapper.model.EventLogSchema.Key._

case class YtEventLogBlock(id: String,
                           order: Long,
                           log: Array[Byte]) {
  def toList: List[Any] = {
    List(id, order, log)
  }

  def toJavaMap: java.util.Map[String, Any] = {
    java.util.Map.of(
      ID, id,
      ORDER, order,
      LOG, log
    )
  }
}

object YtEventLogBlock {
  def apply(node: YTreeNode): YtEventLogBlock = {
    import ru.yandex.spark.yt.wrapper.YtJavaConverters._
    val mp = node.asMap()

    new YtEventLogBlock(
      mp.getOrThrow(ID).stringValue(),
      mp.getOrThrow(ORDER).longValue(),
      mp.getOrThrow(LOG).bytesValue()
    )
  }

  def apply(s: String): YtEventLogBlock = {
    apply(YTreeTextSerializer.deserialize(s))
  }
}
