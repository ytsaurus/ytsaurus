package tech.ytsaurus.spyt.fs.eventlog

import tech.ytsaurus.spyt.wrapper.model.EventLogSchema.Key._
import tech.ytsaurus.ysontree.{YTreeNode, YTreeTextSerializer}

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
    import tech.ytsaurus.spyt.wrapper.YtJavaConverters._
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


