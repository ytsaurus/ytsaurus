package ru.yandex.spark.yt.wrapper.file

import java.io.InputStream
import java.util.concurrent.TimeUnit

import ru.yandex.yt.ytclient.proxy.FileReader

import scala.concurrent.duration.Duration

class YtFileInputStream(reader: FileReader, timeout: Duration) extends InputStream {
  private var chunk: Iterator[Byte] = _
  private var closed: Boolean = false

  var pos: Long = 0

  override def read(): Int = {
    if (hasNext) next() else -1
  }

  def hasNext: Boolean = {
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
    reader.readyEvent().get(timeout.toMillis, TimeUnit.MILLISECONDS)
  }

  private def readNextBatch(): Boolean = {
    waitReaderReadyEvent()
    val list = reader.read()
    if (list != null) {
      chunk = list.iterator
      chunk.hasNext || hasNext
    } else {
      close()
      false
    }
  }

  def next(): Byte = {
    pos += 1
    chunk.next()
  }

  override def read(b: Array[Byte], off: Int, len: Int): Int = {
    if (!hasNext) {
      -1
    } else {
      (0 until len).view.map { index =>
        val hasNextValue = hasNext
        val byte = if (hasNextValue) next() else (-1).toByte
        b(off + index) = byte
        hasNextValue
      }.takeWhile(_ == true).length
    }
  }

  override def close(): Unit = {
    if (!closed) {
      closed = true
      reader.close().get(timeout.toMillis, TimeUnit.MILLISECONDS)
    }
  }
}
