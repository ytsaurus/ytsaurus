package ru.yandex.spark.yt.format

import java.util
import java.util.concurrent.{CompletableFuture, TimeUnit}

import org.apache.hadoop.fs.Path
import org.apache.log4j.Logger
import org.apache.spark.metrics.yt.YtMetricsRegister
import org.apache.spark.metrics.yt.YtMetricsRegister.ytMetricsSource._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.OutputWriter
import org.apache.spark.sql.types.StructType
import ru.yandex.inside.yt.kosher.common.GUID
import ru.yandex.spark.yt.format.conf.{SparkYtWriteConfiguration, YtTableSparkSettings}
import ru.yandex.spark.yt.fs.{GlobalTableSettings, YtClientProvider}
import ru.yandex.spark.yt.serializers.InternalRowSerializer
import ru.yandex.spark.yt.wrapper.LogLazy
import ru.yandex.spark.yt.wrapper.client.YtClientConfiguration
import ru.yandex.yt.ytclient.proxy.TableWriter
import ru.yandex.yt.ytclient.proxy.request.{TransactionalOptions, WriteTable}

import scala.concurrent.{Await, Future}
import ru.yandex.spark.yt.fs.conf._

class YtOutputWriter(path: String,
                     schema: StructType,
                     ytClientConfiguration: YtClientConfiguration,
                     writeConfiguration: SparkYtWriteConfiguration,
                     transactionGuid: String,
                     options: Map[String, String]) extends OutputWriter with LogLazy {

  import writeConfiguration._

  private val log = Logger.getLogger(getClass)

  private val schemaHint = options.ytConf(YtTableSparkSettings.WriteSchemaHint)

  private val client = YtClientProvider.ytClient(ytClientConfiguration)
  private val requestPath = s"""<"append"=true>/${new Path(path).toUri}"""

  private var writers = Seq(initializeWriter())
  private var writeFutures = Seq.empty[CompletableFuture[Void]]
  private var prevFuture: Option[Future[Unit]] = None

  private var list = new util.ArrayList[InternalRow](miniBatchSize)
  private var count = 0
  private var batchCount = 0

  YtMetricsRegister.register()
  GlobalTableSettings.setTransaction(path, transactionGuid)

  override def write(record: InternalRow): Unit = {
    try {
      YtMetricsRegister.time(writeTime, writeTimeSum) {
        count += 1
        batchCount += 1
        list.add(record.copy())
        if (count == miniBatchSize) {
          writeMiniBatch()
          list = new util.ArrayList[InternalRow](miniBatchSize)
          count = 0
        }
        if (batchCount == batchSize) {
          writeBatch()
          batchCount = 0
        }
      }
    } catch {
      case e: Throwable =>
        log.warn("Write failed, closing writer")
        closeWriters()
        log.warn("Write failed, writer closed")
        throw e
    }

  }

  private def closeCurrentWriter(): Unit = {
    prevFuture.foreach(Await.result(_, timeout))
    val currentWriter = writers.head
    writeFutures = currentWriter.readyEvent().thenCompose((unused) => {
      currentWriter.close()
    }) +: writeFutures
  }

  private def writeBatch(): Unit = {
    log.debugLazy(s"Batch of size $batchSize")
    closeCurrentWriter()
    writers = initializeWriter() +: writers
    log.debugLazy(s"Batch written")
  }

  private def writeMiniBatch(): Unit = {
    log.debugLazy(s"Writing mini batch of size $miniBatchSize")
    YtMetricsRegister.time(writeBatchTime, writeBatchTimeSum) {
      prevFuture.foreach(Await.result(_, timeout))
      prevFuture = Some(InternalRowSerializer.writeRows(writers.head, list, timeout))
    }
    log.debugLazy(s"Mini batch written")
  }

  private def closeWriters(): Unit = {
    log.debugLazy("Close writer")
    YtMetricsRegister.time(writeCloseTime, writeCloseTimeSum) {
      closeCurrentWriter()
      writeFutures.foreach(_.get(timeout.toMillis, TimeUnit.MILLISECONDS))
    }
    log.debugLazy("Writer closed")
  }

  override def close(): Unit = {
    log.debugLazy("Closing YtOutputWriter")
    YtMetricsRegister.time(writeTime, writeTimeSum) {
      if (count != 0) {
        log.debugLazy(s"Writing last batch, list size: ${list.size()}, writer: $this ")
        writeMiniBatch()
      }
      closeWriters()
    }
  }

  private def initializeWriter(): TableWriter[InternalRow] = {
    val request = new WriteTable[InternalRow](requestPath, new InternalRowSerializer(schema, schemaHint))
      .setTransactionalOptions(new TransactionalOptions(GUID.valueOf(transactionGuid)))
    client.writeTable(request).join()
  }
}
