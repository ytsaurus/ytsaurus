package ru.yandex.spark.yt.fs.eventlog

import ru.yandex.inside.yt.kosher.impl.ytree.serialization.YTreeTextSerializer
import ru.yandex.inside.yt.kosher.ytree.YTreeNode
import ru.yandex.spark.yt.wrapper.model.EventLogSchema.Key._

case class YtEventLogFileDetails(fileName: String,
                                 id: String,
                                 meta: YtEventLogFileMeta) {
  def toJavaMap: java.util.Map[String, Any] = {
    java.util.Map.of(
      FILENAME, fileName,
      ID, id,
      META, meta.toYson
    )
  }

  def toList: List[Any] = {
    List(fileName, id, meta.toYson)
  }
}

object YtEventLogFileDetails {
  def apply(node: YTreeNode): YtEventLogFileDetails = {
    import ru.yandex.spark.yt.wrapper.YtJavaConverters._
    val mp = node.asMap()

    YtEventLogFileDetails(
      mp.getOrThrow(FILENAME).stringValue(),
      mp.getOrThrow(ID).stringValue(),
      YtEventLogFileMeta(mp.getOrThrow(META)))
  }

  def apply(s: String): YtEventLogFileDetails = {
    apply(YTreeTextSerializer.deserialize(s))
  }
}


