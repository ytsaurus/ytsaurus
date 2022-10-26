package ru.yandex.spark.yt.fs

import org.apache.hadoop.fs._
import java.io._
import java.nio.ByteBuffer

// org.apache.hadoop.fs.RawLocalFileSystem.LocalFSFileOutputStream
// but using Spark's class invokes NPE
class LocalFSInputStream
(val f: File) extends FSInputStream with HasFileDescriptor {
  private val fis = new FileInputStream(f)
  private var position = 0L

  override def seek(pos: Long): Unit = {
    if (pos < 0) throw new EOFException(FSExceptionMessages.NEGATIVE_SEEK)
    fis.getChannel.position(pos)
    this.position = pos
  }

  override def getPos: Long = this.position

  override def seekToNewSource(targetPos: Long): Boolean = false

  override def available: Int = fis.available

  override def close(): Unit = fis.close()

  override def markSupported = false

  override def read: Int = {
    val value = fis.read
    if (value >= 0) this.position += 1
    value
  }

  override def read(b: Array[Byte], off: Int, len: Int): Int = {
    val value = fis.read(b, off, len)
    if (value > 0) this.position += value
    value
  }

  override def read(position: Long, b: Array[Byte], off: Int, len: Int): Int = {
    fis.getChannel.read(ByteBuffer.wrap(b, off, len), position)
  }

  override def skip(n: Long): Long = {
    val value = fis.skip(n)
    if (value > 0) this.position += value
    value
  }

  override def getFileDescriptor: FileDescriptor = fis.getFD
}