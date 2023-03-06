package tech.ytsaurus.spyt.wrapper.table

import org.slf4j.LoggerFactory
import tech.ytsaurus.spyt.wrapper.LogLazy
import tech.ytsaurus.client.TableReader

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration

class TableIterator[T](reader: TableReader[T], timeout: Duration,
                       reportBytesRead: Long => Unit)
    extends Iterator[T] with AutoCloseable with LogLazy {
  private val log = LoggerFactory.getLogger(getClass)
  private var chunk: java.util.Iterator[T] = _
  private var prevRowCount: Long = 0
  private var totalBytesRead: Long = 0

  override def hasNext: Boolean = {
    if (chunk != null && chunk.hasNext) {
      true
    } else if (reader.canRead) {
      readNextBatch()
    } else {
      close()
      false
    }
  }

  private def waitReaderReadyEvent(): Unit = {
    log.debugLazy("Waiting for reader")
    reader.readyEvent().get(timeout.toMillis, TimeUnit.MILLISECONDS)
  }

  private def readNextBatch(): Boolean = {
    waitReaderReadyEvent()
    log.debugLazy(s"Reader is ready, total rows ${reader.getTotalRowCount}")
    val list = reader.read()

    val stats = Option(reader.getDataStatistics)
    stats.foreach { s =>
      reportBytesRead(s.getCompressedDataSize - totalBytesRead)
      totalBytesRead = s.getCompressedDataSize
    }

    log.debugLazy {
      val rowCount = stats.map(_.getRowCount)
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
    if (reader.canRead) {
      reader.cancel()
    } else {
      reader.close().get(timeout.toMillis, TimeUnit.MILLISECONDS)
    }
  }
}
