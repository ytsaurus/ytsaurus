package ru.yandex.spark.yt.utils

import java.io.OutputStream

import ru.yandex.yt.ytclient.proxy.FileWriter

import scala.annotation.tailrec

class YtFileOutputStream(writer: FileWriter) extends OutputStream {
  private var closed = false

  override def write(b: Int): Unit = {
    write(Array(b.toByte), 0, 1)
  }

  @tailrec
  override final def write(b: Array[Byte], off: Int, len: Int): Unit = {
    if (!writer.write(b, off, len)) {
      writer.readyEvent().join()
      write(b, off, len)
    }
  }

  override def write(b: Array[Byte]): Unit = {
    write(b, 0, b.length)
  }

  override def flush(): Unit = {
  }

  override def close(): Unit = {
    if (!closed) {
      writer.close().join()
      closed = true
    }
  }
}
