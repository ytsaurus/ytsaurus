package ru.yandex.spark.yt.format

import java.util.concurrent.TimeUnit

import org.apache.log4j.Logger
import org.apache.spark.metrics.yt.YtMetricsRegister
import org.apache.spark.metrics.yt.YtMetricsRegister.ytMetricsSource._
import ru.yandex.spark.yt._
import ru.yandex.yt.ytclient.proxy.TableReader

class TableIterator[T](reader: TableReader[T]) extends Iterator[T] with AutoCloseable {
  private val log = Logger.getLogger(getClass)
  private var chunk: java.util.Iterator[T] = _
  private var prevRowCount: Long = 0

  YtMetricsRegister.register()

  override def hasNext: Boolean = {
    YtMetricsRegister.time(readTime, readTimeSum) {
      if (chunk != null && chunk.hasNext) {
        true
      } else if (reader.canRead) {
        YtMetricsRegister.time(readBatchTime, readBatchTimeSum) {
          readNextBatch()
        }
      } else {
        close()
        false
      }
    }
  }

  private def waitReaderReadyEvent(): Unit = {
    YtMetricsRegister.time(readReadyEventTime, readReadyEventTimeSum) {
      log.debugLazy("Waiting for reader")
      reader.readyEvent().get(30, TimeUnit.SECONDS)
    }
  }

  private def readNextBatch(): Boolean = {
    waitReaderReadyEvent()
    log.debugLazy(s"Reader is ready, total rows ${reader.getTotalRowCount}")
    val list = reader.read()

    log.debugLazy {
      val rowCount = Option(reader.getDataStatistics).map(_.getRowCount)
      val batchSize = rowCount.map(_ - prevRowCount)
      rowCount.foreach(prevRowCount = _)
      s"Reader is read, row count $rowCount, chunk $batchSize#"
    }

    if (list != null) {
      chunk = list.iterator()
      chunk.hasNext || hasNext
    } else {
      close()
      false
    }
  }

  override def next(): T = {
    chunk.next()
  }

  override def close(): Unit = {
    YtMetricsRegister.time(readCloseTime, readCloseTimeSum) {
      log.debugLazy("Close reader")
      reader.close().get(30, TimeUnit.SECONDS)
      log.debugLazy("Reader closed")
    }
  }
}
