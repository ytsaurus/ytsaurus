package ru.yandex.spark.launcher

import org.slf4j.LoggerFactory
import ru.yandex.spark.launcher.WorkerLogLauncher.WorkerLogConfig
import ru.yandex.spark.yt.wrapper.model.{WorkerLogBlock, WorkerLogMeta}
import ru.yandex.spark.yt.wrapper.model.WorkerLogSchema.{getMetaPath, metaSchema, schema}
import ru.yandex.spark.yt.wrapper.{LogLazy, YtWrapper}
import ru.yandex.yt.ytclient.proxy.{ApiServiceTransaction, CompoundClient}

import java.time.{LocalDate, LocalDateTime}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class WorkerLogWriter(workerLogConfig: WorkerLogConfig)(implicit yt: CompoundClient) extends LogLazy {
  private val log = LoggerFactory.getLogger(getClass)
  private val buffer = new ArrayBuffer[WorkerLogBlock](workerLogConfig.bufferSize)
  private val workerLogStartDate = LocalDate.now().toString
  private var unflushedNewFiles = List.empty[MetaKey]
  private val rowCounter = mutable.HashMap.empty[MetaKey, Long]
  private val creationTimes = mutable.HashMap.empty[MetaKey, LocalDateTime]

  private type MetaKey = (String, String, String)

  case object MetaKey {
    def apply(block: WorkerLogBlock): MetaKey = (block.appDriver, block.execId, block.stream)
    def apply(meta: WorkerLogMeta): MetaKey = (meta.appDriver, meta.execId, meta.stream)
  }

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

  def newEmptyFile(metaKey: MetaKey): Unit = {
    unflushedNewFiles = metaKey +: unflushedNewFiles
  }

  def flush(): Unit = {
    if (buffer.nonEmpty || unflushedNewFiles.nonEmpty) {
      log.debugLazy(s"Flushing ${buffer.length} new log lines")
      val list = YtWrapper.runWithRetry {
        transaction =>
          // Optimized groupBy
          // It works because .flush() will be called after reading of all files once
          val newData = buffer.foldRight[List[(MetaKey, List[WorkerLogBlock])]](List.empty) {
            case (block, seq) =>
              val metaKey = MetaKey(block)
              seq match {
                case Nil => List((metaKey, List(block)))
                case (headKey, headSeq) :: tail =>
                  if (headKey == metaKey) (headKey, block :: headSeq) :: tail
                  else (metaKey, List(block)) :: seq
              }
          }
          val newDataMeta = newData.map { case (metaKey, blocks) => (metaKey, blocks.length.toLong) }
          val newEmptyFilesMeta = unflushedNewFiles.map((_, 0L))

          val unflushedMeta = (newDataMeta ++ newEmptyFilesMeta).map {
            case (key@(appDriver, execId, stream), count) =>
              WorkerLogMeta(appDriver, execId, stream,
                workerLogStartDate, count + rowCounter.getOrElse(key, 0L))
          }

          val (existing, nonExisting) = unflushedMeta
            .partition(meta => rowCounter.contains(MetaKey(meta)))

          insertMetas(nonExisting, Some(transaction))
          existing.foreach(meta => updateMeta(meta, Some(transaction)))
          newData.foreach { case (key, logs) =>
            upload(creationTimes(key).toLocalDate, logs, Some(transaction)) }

          unflushedMeta
      }
      list.foreach(meta => rowCounter(MetaKey(meta)) = meta.length)
      unflushedNewFiles = List.empty[MetaKey]
      buffer.clear()
      log.debugLazy("Finished flushing")
    }
  }

  def setCreationTime(appDriver: String, execId: String, logFileName: String, creationTime: LocalDateTime): Unit = {
    if (!creationTimes.contains(appDriver, execId, logFileName)) {
      creationTimes((appDriver, execId, logFileName)) = creationTime
    }
  }

  private def insertMetas(metas: Seq[WorkerLogMeta], transaction: Option[ApiServiceTransaction]): Unit = {
    YtWrapper.insertRows(
      getMetaPath(workerLogConfig.tablesPath), metaSchema, metas.map(_.toList), transaction)
  }

  private def updateMeta(meta: WorkerLogMeta, transaction: Option[ApiServiceTransaction]): Unit = {
    YtWrapper.updateRow(
      getMetaPath(workerLogConfig.tablesPath), metaSchema, meta.toJavaMap, transaction)
  }

  private def upload(date: LocalDate, logArray: Seq[WorkerLogBlock], transaction: Option[ApiServiceTransaction]): Unit = {
    import scala.collection.JavaConverters._
    YtWrapper.createDynTableAndMount(s"${workerLogConfig.tablesPath}/$date", schema,
      Map("expiration_time" -> (System.currentTimeMillis() + workerLogConfig.tableTTL.toMillis)))
    YtWrapper.insertRows(
      s"${workerLogConfig.tablesPath}/$date",
      schema,
      logArray.map(l => l.toList).asJava,
      transaction
    )
  }
}
