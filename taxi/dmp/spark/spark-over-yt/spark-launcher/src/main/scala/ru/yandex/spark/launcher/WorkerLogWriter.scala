package ru.yandex.spark.launcher

import org.slf4j.LoggerFactory
import ru.yandex.spark.launcher.WorkerLogLauncher.WorkerLogConfig
import ru.yandex.spark.yt.wrapper.YtWrapper
import ru.yandex.spark.yt.wrapper.model.WorkerLogSchema.schema
import ru.yandex.yt.ytclient.proxy.CompoundClient

import java.time.LocalDate
import scala.collection.mutable.ArrayBuffer

class WorkerLogWriter(workerLogConfig: WorkerLogConfig)(implicit yt: CompoundClient) {
  private val log = LoggerFactory.getLogger(getClass)
  private val buffer = new ArrayBuffer[WorkerLogBlock](workerLogConfig.bufferSize)

  def write(block: WorkerLogBlock): Int = {
    if (buffer.length == workerLogConfig.bufferSize) {
      flush()
    }
    if (block.inner.message.length > workerLogConfig.ytTableRowLimit) {
      block.inner.message.grouped(workerLogConfig.ytTableRowLimit).foldLeft(0) {
        case (index, s) =>
          write(block.copy(inner = block.inner.copy(message = s), rowId = block.rowId + index))
          index + 1
      }
    } else {
      buffer += block
      1
    }
  }

  def flush(): Unit = {
    if (buffer.nonEmpty) {
      log.info(s"Flushing ${buffer.length} new log lines")
      val headDate = buffer.head.inner.date
      if (buffer.forall(log => log.inner.date == headDate)) {
        log.debug("All logs have same date")
        uploadOneTable(headDate, buffer)
      } else {
        log.debug("Not all logs have same date")
        buffer.groupBy(log => log.inner.date)
          .foreach { case (date, logs) => uploadOneTable(date, logs) }
      }
      buffer.clear()
      log.debug("Finished flushing")
    }
  }

  private def uploadOneTable(date: LocalDate, logArray: Seq[WorkerLogBlock]): Unit = {
    import scala.collection.JavaConverters._
    YtWrapper.createDynTableAndMount(s"${workerLogConfig.tablesPath}/$date", schema)
    YtWrapper.insertRows(
      s"${workerLogConfig.tablesPath}/$date",
      schema,
      logArray.map(l => l.toList).asJava,
      None
    )
  }
}
