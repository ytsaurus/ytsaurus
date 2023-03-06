package tech.ytsaurus.spyt.wrapper.model

import tech.ytsaurus.spyt.wrapper.model.WorkerLogSchema.Key._
import tech.ytsaurus.ysontree.YTreeNode

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
    import tech.ytsaurus.spyt.wrapper.YtJavaConverters._
    val mp = node.asMap()

    new WorkerLogMeta(
      mp.getOrThrow(APP_DRIVER).stringValue(),
      mp.getOrThrow(EXEC_ID).stringValue(),
      mp.getOrThrow(STREAM).stringValue(),
      mp.getOrThrow(TABLE_NAME).stringValue(),
      mp.getOrThrow(LENGTH).longValue())
  }
}
