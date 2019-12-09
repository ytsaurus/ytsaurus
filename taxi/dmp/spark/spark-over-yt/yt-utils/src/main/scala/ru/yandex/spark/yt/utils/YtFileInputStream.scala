package ru.yandex.spark.yt.utils

import java.io.InputStream
import java.util.concurrent.TimeUnit

import org.apache.log4j.Logger
import ru.yandex.yt.ytclient.proxy.FileReader
import ru.yandex.yt.ytclient.proxy.internal.FileReaderImpl

class YtFileInputStream(reader: FileReader) extends InputStream {
  private val log = Logger.getLogger(getClass)
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
    reader.readyEvent().get(30, TimeUnit.SECONDS)
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

  override def read(b: Array[Byte]): Int = {
    read(b, 0, b.length)
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
      if (reader.canRead) {
        reader.asInstanceOf[FileReaderImpl].cancel()
      } else {
        reader.close().get(30, TimeUnit.SECONDS)
      }
    }
  }
}
