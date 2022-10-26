package org.apache.spark.deploy.history

import ru.yandex.spark.yt.wrapper.model.{WorkerLogBlock, WorkerLogMeta}
import ru.yandex.spark.yt.wrapper.model.WorkerLogSchema.Key.{APP_DRIVER, EXEC_ID, ROW_ID, STREAM}
import ru.yandex.spark.yt.wrapper.{LogLazy, YtWrapper}
import ru.yandex.yt.ytclient.proxy.CompoundClient

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
