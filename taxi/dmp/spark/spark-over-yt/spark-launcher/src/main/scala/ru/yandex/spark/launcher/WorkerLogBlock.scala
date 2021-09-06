package ru.yandex.spark.launcher

import ru.yandex.inside.yt.kosher.impl.ytree.serialization.YTreeTextSerializer
import ru.yandex.inside.yt.kosher.ytree.YTreeNode
import ru.yandex.spark.yt.wrapper.model.WorkerLogSchema.Key._

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, LocalDateTime}
import java.util

case class WorkerLogBlock(appDriver: String,
                          execId: String,
                          stream: String,
                          rowId: Long,
                          inner: WorkerLogBlockInner) {
  def toList: util.List[Any] = {
    // unpacking field from this and inner (excluding duplicated datetime information field)
    val list = new util.ArrayList[Any](productArity - 1 + inner.productArity - 1)
    list.add(appDriver)
    list.add(execId)
    list.add(stream)
    list.add(rowId)
    inner.productIterator.foreach {
      case opt: Option[_] => list.add(opt.orNull)
      case dt: LocalDate =>
      case other => list.add(other)
    }
    list
  }
}

object WorkerLogBlock {
  val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")

  def fromJson(json: String, stream: String, appDriver: String, executorId: String,
               rowId: Long, fileCreationTime: LocalDateTime): WorkerLogBlock = {
    WorkerLogBlock(appDriver, executorId, stream, rowId, WorkerLogBlockInner.fromJson(json, fileCreationTime))
  }

  def fromMessage(message: String, stream: String, appDriver: String, executorId: String,
                  rowId: Long, fileCreationTime: LocalDateTime): WorkerLogBlock = {
    WorkerLogBlock(appDriver, executorId, stream, rowId, WorkerLogBlockInner.fromMessage(message, fileCreationTime))
  }

  def apply(node: YTreeNode): WorkerLogBlock = {
    import ru.yandex.spark.yt.wrapper.YtJavaConverters._
    val mp = node.asMap()

    new WorkerLogBlock(
      mp.getOrThrow(APP_DRIVER).stringValue(),
      mp.getOrThrow(EXEC_ID).stringValue(),
      mp.getOrThrow(STREAM).stringValue(),
      mp.getOrThrow(ROW_ID).longValue(),
      WorkerLogBlockInner(node)
    )
  }

  def apply(s: String): WorkerLogBlock = {
    apply(YTreeTextSerializer.deserialize(s))
  }
}
