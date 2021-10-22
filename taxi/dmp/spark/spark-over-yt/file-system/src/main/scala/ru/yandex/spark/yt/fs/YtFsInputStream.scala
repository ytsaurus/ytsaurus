package ru.yandex.spark.yt.fs

import org.apache.hadoop.fs.{FSInputStream, FileSystem}
import ru.yandex.spark.yt.wrapper.file.YtFileInputStream

import scala.annotation.tailrec

class YtFsInputStream(in: YtFileInputStream, statistics: FileSystem.Statistics) extends FSInputStream {
  @tailrec
  override final def seek(pos: Long): Unit = {
    if (getPos < pos) {
      readInt()
      seek(pos)
    }
  }

  override def getPos: Long = in.pos

  override def seekToNewSource(targetPos: Long): Boolean = ???

  private def readInt(): Int = {
    if (in.hasNext) in.next() & 0xff else -1
  }

  override def read(): Int = {
    val r = readInt()
    if (r >= 0) {
      statistics.incrementBytesRead(1)
    }
    r
  }

  override def read(b: Array[Byte], off: Int, len: Int): Int = {
    val r = in.read(b, off, len)
    if (r >= 0) {
      statistics.incrementBytesRead(r)
    }
    r
  }

  override def close(): Unit = {
    in.close()
  }
}
