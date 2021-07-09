package ru.yandex.spark.yt.format

import java.util
import java.util.concurrent.{CompletableFuture, TimeUnit}
import org.apache.hadoop.fs.Path
import org.apache.spark.metrics.yt.YtMetricsRegister
import org.apache.spark.metrics.yt.YtMetricsRegister.ytMetricsSource._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.OutputWriter
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory
import ru.yandex.inside.yt.kosher.common.GUID
import ru.yandex.spark.yt.format.conf.SparkYtWriteConfiguration
import ru.yandex.spark.yt.format.conf.YtTableSparkSettings._
import ru.yandex.spark.yt.fs.conf._
import ru.yandex.spark.yt.fs.GlobalTableSettings
import ru.yandex.spark.yt.serializers.InternalRowSerializer
import ru.yandex.spark.yt.wrapper.LogLazy
import ru.yandex.spark.yt.wrapper.client.{YtClientConfiguration, YtClientProvider}
import ru.yandex.yt.ytclient.proxy.request.{TransactionalOptions, WriteTable}
import ru.yandex.yt.ytclient.proxy.{CompoundClient, TableWriter}

import scala.concurrent.{Await, Future}
import scala.util.{Failure, Try}

class YtOutputWriter(path: String,
                     schema: StructType,
                     ytClientConfiguration: YtClientConfiguration,
                     writeConfiguration: SparkYtWriteConfiguration,
                     transactionGuid: String,
                     options: Map[String, String]) extends OutputWriter with LogLazy {

  import writeConfiguration._

  private val log = LoggerFactory.getLogger(getClass)

  private val schemaHint = options.ytConf(WriteSchemaHint)

  private val client = createYtClient()

  private val splitPartitions = options.ytConf(SortColumns).isEmpty

  private var writers = Seq(initializeWriter())
  private var writeFutures = Seq.empty[CompletableFuture[Void]]
  private var prevFuture: Option[Future[Unit]] = None

  private var list = new util.ArrayList[InternalRow](miniBatchSize)
  private var count = 0L
  private var batchCount = 0L

  initialize()

  /**
   * @deprecated Do not use before YT 21.1 release
   */
  @Deprecated
  private def sortedChunkPath(): String = {
    s"""<
       |chunk_key_column_count=${options.ytConf(SortColumns).length};
       |append=true
       |>/${new Path(path).toUri}""".stripMargin
  }

  private def appendPath(): String = {
    s"""<append=true>/${new Path(path).toUri.getPath}""".stripMargin
  }

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
        if (batchCount == batchSize && splitPartitions) {
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
    val closePrev = prevFuture.map(f => Try(Await.result(f, timeout)))
    val currentWriter = writers.head
    writeFutures = currentWriter.readyEvent().thenComposeAsync((unused) => {
      currentWriter.close().thenAccept(unused => null)
    }) +: writeFutures
    closePrev.foreach {
      case Failure(exception) =>
        throw new IllegalStateException("Yt writer is not closed properly", exception)
      case _ => // ok
    }
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
      val currentClose = Try(closeCurrentWriter())
      val prevClose = writeFutures.map(f => Try(f.get(timeout.toMillis, TimeUnit.MILLISECONDS)))

      (currentClose +: prevClose).collectFirst {
        case Failure(exception) =>
          throw new IllegalStateException("Yt writer is not closed properly", exception)
      }
    }
    log.debugLazy("Writer closed")
  }

  override def close(): Unit = {
    log.debugLazy("Closing YtOutputWriter")
    YtMetricsRegister.time(writeTime, writeTimeSum) {
      try {
        if (count != 0) {
          log.debugLazy(s"Writing last batch, list size: ${list.size()}, writer: $this ")
          writeMiniBatch()
        }
      } finally {
        closeWriters()
      }
    }
  }

  protected def initializeWriter(): TableWriter[InternalRow] = {
    log.debugLazy(s"Initialize new write: ${appendPath()}, transaction: $transactionGuid")
    val request = new WriteTable[InternalRow](appendPath(), new InternalRowSerializer(schema, schemaHint))
      .setTransactionalOptions(new TransactionalOptions(GUID.valueOf(transactionGuid)))
    client.writeTable(request).join()
  }

  protected def createYtClient(): CompoundClient = {
    YtClientProvider.ytClient(ytClientConfiguration)
  }

  protected def initialize(): Unit = {
    YtMetricsRegister.register()
    GlobalTableSettings.setTransaction(path, transactionGuid)
  }
}
