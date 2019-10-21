package ru.yandex.spark.yt.format

import java.util.concurrent.TimeUnit

import org.apache.hadoop.fs.FSInputStream
import org.apache.log4j.Logger
import ru.yandex.spark.yt._
import ru.yandex.yt.ytclient.proxy.FileReader

class FileIterator(reader: FileReader) extends FSInputStream {
  private val log = Logger.getLogger(getClass)
  private var chunk: Iterator[Byte] = _
  private var pos: Long = 0

  override def seek(pos: Long): Unit = ???

  override def getPos: Long = pos

  override def seekToNewSource(targetPos: Long): Boolean = ???

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

  override def read(position: Long, buffer: Array[Byte], offset: Int, length: Int): Int = {
    read(buffer, offset, length)
  }

  override def read(b: Array[Byte], off: Int, len: Int): Int = {
    if (!hasNext) {
      -1
    } else {
      (0 until len).map { index =>
        val hasNextValue = hasNext
        val byte = if (hasNextValue) next() else (-1).toByte
        b(off + index) = byte
        hasNextValue
      }.takeWhile(_ == true).length
    }
  }

  override def close(): Unit = {
    log.debugLazy("Close reader")
    reader.close().get(30, TimeUnit.SECONDS)
    log.debugLazy("Reader closed")
  }
}
