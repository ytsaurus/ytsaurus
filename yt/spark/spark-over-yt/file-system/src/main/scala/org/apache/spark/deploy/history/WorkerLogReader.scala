package org.apache.spark.deploy.history

import tech.ytsaurus.spyt.wrapper.model.{WorkerLogBlock, WorkerLogMeta}
import tech.ytsaurus.spyt.wrapper.model.WorkerLogSchema.Key.{APP_DRIVER, EXEC_ID, ROW_ID, STREAM}
import tech.ytsaurus.spyt.wrapper.{LogLazy, YtWrapper}
import tech.ytsaurus.client.CompoundClient

object WorkerLogReader extends LogLazy {
  def read(tablePath: String, appDriver: String, execId: String, logType: String, startIndex: Long, endIndex: Long)
          (implicit yt: CompoundClient): Seq[WorkerLogBlock] = {
    YtWrapper.selectRows(tablePath, Some(
      s"""$APP_DRIVER="$appDriver" and $EXEC_ID="$execId" and
         | $STREAM="$logType" and $startIndex <= $ROW_ID and $ROW_ID < $endIndex""".stripMargin
    )).map(WorkerLogBlock(_))
  }

  def getLogMeta(metaTablePath: String, appDriver: String, execId: String, logType: String)
                (implicit yt: CompoundClient): Option[WorkerLogMeta] = {
    YtWrapper.selectRows(metaTablePath, Some(
      s"""$APP_DRIVER="$appDriver" and $EXEC_ID="$execId" and $STREAM="$logType""""
    )).map(WorkerLogMeta(_)).headOption
  }
}
