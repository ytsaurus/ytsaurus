package ru.yandex.spark.yt.fs.eventlog

import ru.yandex.inside.yt.kosher.impl.ytree.serialization.YTreeTextSerializer
import ru.yandex.inside.yt.kosher.ytree.YTreeNode
import ru.yandex.spark.yt.fs.eventlog.YtEventLogFileDetails.{FILE_NAME, ID, META}

case class YtEventLogFileDetails(fileName: String,
                                 id: String,
                                 meta: YtEventLogFileMeta) {
  def toJavaMap: java.util.Map[String, Any] = {
    java.util.Map.of(
      FILE_NAME, fileName,
      ID, id,
      META, meta.toYson
    )
  }

  def toList: List[Any] = {
    List(fileName, id, meta.toYson)
  }
}

object YtEventLogFileDetails {
  private val FILE_NAME = "file_name"
  private val ID = "id"
  private val META = "meta"

  def apply(node: YTreeNode): YtEventLogFileDetails = {
    val mp = node.asMap()

    YtEventLogFileDetails(
      mp.getOrThrow(FILE_NAME).stringValue(),
      mp.getOrThrow(ID).stringValue(),
      YtEventLogFileMeta(mp.getOrThrow(META)))
  }

  def apply(s: String): YtEventLogFileDetails = {
    apply(YTreeTextSerializer.deserialize(s))
  }
}


