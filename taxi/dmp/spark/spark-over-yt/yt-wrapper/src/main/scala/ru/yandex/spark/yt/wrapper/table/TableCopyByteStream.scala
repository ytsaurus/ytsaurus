package ru.yandex.spark.yt.wrapper.table

import java.nio.ByteBuffer

import org.apache.log4j.Logger
import ru.yandex.yt.ytclient.proxy.TableReader

import scala.annotation.tailrec
import scala.concurrent.duration.Duration


class TableCopyByteStream(reader: TableReader[ByteBuffer], timeout: Duration) extends YtArrowInputStream {
  private val log = Logger.getLogger(getClass)
  private var _batch: ByteBuffer = _
  private var _batchBytesLeft = 0
  private val nextPageToken: Array[(Byte, Int)] = Array(-1, -1, -1, -1, 0, 0, 0, 0).map(_.toByte).zipWithIndex

  override def read(): Int = ???

  override def read(b: Array[Byte]): Int = {
    read(b, 0, b.length)
  }

  override def read(b: Array[Byte], off: Int, len: Int): Int = {
    read(b, off, len, 0)
  }


  override def isNextPage: Boolean = {
    if (hasNext && _batchBytesLeft >= 8) {
      val res = nextPageToken.forall { case (b, i) => _batch.array()(_batch.arrayOffset() + _batch.position() + i) == b }
      if (res) {
        _batch.position(_batch.position() + 8)
        _batchBytesLeft -= 8
      }
      res
    } else false
  }

  @tailrec
  private def read(b: Array[Byte], off: Int, len: Int, readLen: Int): Int = len match {
    case 0 => readLen
    case _ =>
      if (hasNext) {
        val readBytes = Math.min(len, _batchBytesLeft)
        readFromBatch(b, off, readBytes)
        read(b, off + readBytes, len - readBytes, readLen + readBytes)
      } else readLen
  }

  private def hasNext: Boolean = {
    _batchBytesLeft > 0 || readNextBatch()
  }

  private def readFromBatch(b: Array[Byte], off: Int, len: Int): Unit = {
    System.arraycopy(_batch.array(), _batch.arrayOffset() + _batch.position(), b, off, len)
    _batch.position(_batch.position() + len)
    _batchBytesLeft -= len
  }

  private def readNextBatch(): Boolean = {
    if (reader.canRead) {
      reader.readyEvent().join()
      val res = reader.read()
      if (res != null) {
        _batch = res.get(0)
        _batchBytesLeft = _batch.limit() - _batch.position()
        true
      } else {
        false
      }
    } else {
      false
    }
  }

  override def close(): Unit = {
    if (reader.canRead) {
      reader.cancel()
    } else {
      reader.close().join()
    }
  }
}
