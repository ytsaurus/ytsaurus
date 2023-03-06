package tech.ytsaurus.spyt.fs.eventlog

import tech.ytsaurus.spyt.wrapper.model.EventLogSchema.Key._
import tech.ytsaurus.ysontree.{YTreeNode, YTreeTextSerializer}

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
    import tech.ytsaurus.spyt.wrapper.YtJavaConverters._
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
