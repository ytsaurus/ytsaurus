package ru.yandex.spark.yt.fs.eventlog

import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTreeBuilder
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.YTreeTextSerializer
import ru.yandex.inside.yt.kosher.ytree.YTreeNode
import ru.yandex.spark.yt.wrapper.YtWrapper
import ru.yandex.spark.yt.wrapper.model.EventLogSchema.Key._

case class YtEventLogFileMeta(rowSize: Int,
                              blocksCnt: Int,
                              length: Long,
                              modificationTs: Long) {
  def toYson: Array[Byte] = {
    YtWrapper.serialiseYson(
      new YTreeBuilder()
        .beginMap()
        .key(ROW_SIZE).value(rowSize)
        .key(BLOCKS_CNT).value(blocksCnt)
        .key(LENGTH).value(length)
        .key(MODIFICATION_TS).value(modificationTs)
        .endMap()
        .build())
  }
}

object YtEventLogFileMeta {
  def apply(node: YTreeNode): YtEventLogFileMeta = {
    import ru.yandex.spark.yt.wrapper.YtJavaConverters._
    val mp = node.asMap()

    YtEventLogFileMeta(
      mp.getOrThrow(ROW_SIZE).intValue(),
      mp.getOrThrow(BLOCKS_CNT).intValue(),
      mp.getOrThrow(LENGTH).longValue(),
      mp.getOrThrow(MODIFICATION_TS).longValue())
  }

  def apply(s: String): YtEventLogFileMeta = {
    apply(YTreeTextSerializer.deserialize(s))
  }
}
