package ru.yandex.spark.yt.wrapper.model

import ru.yandex.inside.yt.kosher.ytree.YTreeNode
import ru.yandex.spark.yt.wrapper.model.WorkerLogSchema.Key.{APP_DRIVER, EXEC_ID, LENGTH, STREAM, TABLE_NAME}
import ru.yandex.yt.ytclient.tables.ColumnValueType

import java.time.LocalDate
import java.util

case class WorkerLogMeta(appDriver: String,
                         execId: String,
                         stream: String,
                         tableName: String,
                         length: Long) {
  def toList: List[Any] = {
    List(appDriver, execId, stream, tableName, length)
  }

  def toJavaMap: java.util.Map[String, Any] = {
    java.util.Map.of(
      APP_DRIVER, appDriver,
      EXEC_ID, execId,
      STREAM, stream,
      TABLE_NAME, tableName,
      LENGTH, length
    )
  }
}

object WorkerLogMeta {
  def apply(node: YTreeNode): WorkerLogMeta = {
    import ru.yandex.spark.yt.wrapper.YtJavaConverters._
    val mp = node.asMap()

    new WorkerLogMeta(
      mp.getOrThrow(APP_DRIVER).stringValue(),
      mp.getOrThrow(EXEC_ID).stringValue(),
      mp.getOrThrow(STREAM).stringValue(),
      mp.getOrThrow(TABLE_NAME).stringValue(),
      mp.getOrThrow(LENGTH).longValue())
  }
}
