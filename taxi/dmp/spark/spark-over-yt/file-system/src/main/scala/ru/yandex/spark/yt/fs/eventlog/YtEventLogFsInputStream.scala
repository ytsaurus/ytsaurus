package ru.yandex.spark.yt.fs.eventlog

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FSInputStream
import org.slf4j.LoggerFactory
import ru.yandex.spark.yt.wrapper.YtWrapper
import ru.yandex.spark.yt.wrapper.YtWrapper.RichLogger
import ru.yandex.spark.yt.wrapper.model.EventLogSchema.schema
import ru.yandex.yt.ytclient.proxy.CompoundClient

import java.io.IOException
import scala.annotation.tailrec

class YtEventLogFsInputStream(conf: Configuration, path: String, details: YtEventLogFileDetails, implicit val yt: CompoundClient) extends FSInputStream {
  private val log = LoggerFactory.getLogger(getClass)

  private val singleReadLimit = conf.get("fs.ytEventLog.singleReadLimit").toInt
  private var finished = false
  private var manualClosed = false

  private var order: Int = _
  private var globalPos: Long = _
  private var posInCurrentBlock: Int = _
  private var currentBlock: Array[Byte] = _

  resetToStreamStart()

  private def resetToStreamStart(): Unit = {
    order = 0
    globalPos = 0
    posInCurrentBlock = 0
    currentBlock = new Array[Byte](0)
  }

  private def countAvailableInBuffer(): Int = {
    currentBlock.length - posInCurrentBlock
  }

  @tailrec
  override final def seek(pos: Long): Unit = {
    checkClosed()
    if (finished) return
    if (pos < 0) throw new IllegalArgumentException("Negative seek position")
    else if (getPos < pos) {
      val available = countAvailableInBuffer()
      val needRead = pos - getPos
      if (available >= needRead) {
        movePos(needRead.toInt)
      } else {
        movePos(available)
        val left = needRead - available
        val skipBlocksCnt = left / details.meta.rowSize
        order += skipBlocksCnt.toInt
        globalPos += skipBlocksCnt * details.meta.rowSize
        loadNewBlocks()
        seek(pos)
      }
    } else if (getPos > pos) {
      resetToStreamStart()
      seek(pos)
    }
  }

  override def getPos: Long = globalPos

  override def seekToNewSource(targetPos: Long): Boolean = ???

  private def loadNewBlocks(count: Int = 1): Unit = {
    log.debugLazy(s"Loading $count blocks from yt")
    val rows = YtWrapper
      .selectRows(
        path, schema,
        Some(s"""id="${details.id}" and order > $order and order <= ${order + count}""")
      )
      .map(x => YtEventLogBlock(x))
    order += count
    currentBlock = new Array[Byte](rows.map(_.log.length).sum)
    rows.foldLeft(0){case (index, next) =>
      System.arraycopy(next.log, 0, currentBlock, index, next.log.length)
      index + next.log.length
    }

    posInCurrentBlock = 0
    if (currentBlock.isEmpty) {
      finished = true
    }
  }

  def ready(): Boolean = {
    !manualClosed && countAvailableInBuffer() > 0
  }

  override def read(): Int = {
    checkClosed()
    if (finished) return -1
    if (!ready()) {
      loadNewBlocks()
    }
    if (ready()) {
      posInCurrentBlock += 1
      globalPos += 1
      currentBlock(posInCurrentBlock - 1)
    } else {
      -1
    }
  }

  @tailrec
  private def readImpl(b: Array[Byte], off: Int, len: Int, acc: Int): Int = {
    if (finished) return acc
    val copied = copyAvailable(b, off, len)
    if (copied == len) {
      acc + copied
    } else {
      val newOff = off + copied
      val newLen = len - copied
      loadNewBlocks(Math.min(singleReadLimit / details.meta.rowSize, newLen / details.meta.rowSize + 1))
      readImpl(b, newOff, newLen, acc + copied)
    }
  }

  override def read(b: Array[Byte], off: Int, len: Int): Int = {
    checkClosed()
    val res = readImpl(b, off, len, 0)
    if (res > 0) {
      res
    } else {
      -1
    }
  }

  def copyAvailable(b: Array[Byte], off: Int, len: Int): Int = {
    val willBeCopied = Math.min(countAvailableInBuffer(), len)
    System.arraycopy(currentBlock, posInCurrentBlock, b, off, willBeCopied)
    movePos(willBeCopied)
    willBeCopied
  }

  def movePos(count: Int): Unit = {
    globalPos += count
    posInCurrentBlock += count
  }

  def checkClosed(): Unit = {
    if (manualClosed) {
      throw new IOException("Reading from closed stream")
    }
  }

  override def close(): Unit = {
    log.debugLazy(s"Close $path, ${details.fileName}")
    finished = true
    manualClosed = true
  }
}
