package ru.yandex.spark.yt.fs

import org.apache.hadoop.fs.FSInputStream
import ru.yandex.spark.yt.utils.YtFileInputStream

import scala.annotation.tailrec

class YtFsInputStream(in: YtFileInputStream) extends FSInputStream {
  @tailrec
  override final def seek(pos: Long): Unit = {
    if (getPos < pos) {
      read()
      seek(pos)
    }
  }

  override def getPos: Long = in.pos

  override def seekToNewSource(targetPos: Long): Boolean = ???

  override def read(): Int = {
    if (in.hasNext) in.next() else -1
  }

  override def read(b: Array[Byte], off: Int, len: Int): Int = {
    in.read(b, off, len)
  }
}
