package ru.yandex.spark.yt.fs.eventlog

import org.apache.hadoop.conf.Configuration
import org.slf4j.LoggerFactory
import ru.yandex.spark.yt.fs.PathUtils.getMetaPath
import ru.yandex.spark.yt.wrapper.YtWrapper
import ru.yandex.spark.yt.wrapper.model.EventLogSchema.{metaSchema, schema}
import ru.yandex.yt.ytclient.proxy.{ApiServiceTransaction, CompoundClient}

import java.io.OutputStream
import java.time.Clock
import java.util.UUID
import scala.annotation.tailrec

class YtEventLogFsOutputStream(conf: Configuration, path: String, fileName: String, clock: Clock, implicit val yt: CompoundClient) extends OutputStream {
  private val log = LoggerFactory.getLogger(getClass)

  private val metaPath = getMetaPath(path)
  private val rowSize = conf.get("yt.dynTable.rowSize", "16777216").toInt
  private val buffer: Array[Byte] = new Array[Byte](rowSize)

  private case class State(bufferPos: Int,
                           lastFlushPos: Int,
                           flushedDataSize: Long,
                           blockCount: Int,
                           blockUpdateCount: Int) {
    def currentBlockIsFull(): Boolean = {
      bufferPos == rowSize
    }

    def prevFlushWasPartial(): Boolean = {
      lastFlushPos < rowSize
    }

    def nextBlockStartPos: Int = {
      if (currentBlockIsFull()) {
        0
      } else {
        bufferPos
      }
    }

    def incBufferPos(length: Int = 1): State = {
      copy(bufferPos = bufferPos + length)
    }

    def newDataSize: Int = {
      val dataStartPos = if (lastFlushPos < rowSize) lastFlushPos else 0
      bufferPos - dataStartPos
    }

    def hasNewData: Boolean = {
      newDataSize > 0
    }
  }

  private var state = State(0, rowSize, 0, 0, 0)
  private val id = s"${UUID.randomUUID()}"

  open()

  def open(): Unit = {
    YtWrapper.createDynTableAndMount(path, schema)
    YtWrapper.createDynTableAndMount(metaPath, metaSchema)
    YtWrapper.runWithRetry(transaction => updateInfo(Some(transaction)))
  }

  override def write(b: Int): Unit = {
    writeImpl(b)
  }

  override def write(event: Array[Byte], off: Int, len: Int): Unit = {
    writeImpl(event, off, len)
  }

  @tailrec
  private def writeImpl(b: Int): Unit = {
    if (state.currentBlockIsFull()) {
      flush()
      writeImpl(b)
    } else {
      buffer(state.bufferPos) = b.toByte
      state = state.incBufferPos()
    }
  }

  @tailrec
  private def writeImpl(event: Array[Byte], off: Int, len: Int): Unit = {
    val free = rowSize - state.bufferPos
    val copyLen = Math.min(free, len)
    System.arraycopy(event, off, buffer, state.bufferPos, copyLen)
    state = state.incBufferPos(copyLen)
    if (free < len) {
      flush()
      writeImpl(event, off + free, len - free)
    }
  }

  private def tryWithConsistentState(f: => Unit): Unit = {
    val prevState = state
    try {
      f
    } catch {
      case e: Throwable =>
        log.error(s"Error while uploading logs with id=$id and order=${state.blockCount}", e)
        state = prevState
    }
  }

  private def writeNewBlock(data: Array[Byte]): Unit = {
    tryWithConsistentState {
      YtWrapper.runWithRetry{ transaction =>
        YtWrapper.insertRows(path, schema,
          List(new YtEventLogBlock(id, state.blockCount + 1, data).toList),
          Some(transaction)
        )
        state = State(
          bufferPos = state.nextBlockStartPos,
          lastFlushPos = state.bufferPos,
          flushedDataSize = state.flushedDataSize + state.newDataSize,
          blockCount = state.blockCount + 1,
          blockUpdateCount = 0
        )
        updateInfo(Some(transaction))
      }
    }
  }

  private def updateBlock(data: Array[Byte]): Unit = {
    tryWithConsistentState {
      YtWrapper.runWithRetry{ transaction =>
        YtWrapper.updateRow(path, schema,
          new YtEventLogBlock(id, state.blockCount, data).toJavaMap,
          Some(transaction)
        )
        state = State(
          bufferPos = state.nextBlockStartPos,
          lastFlushPos = state.bufferPos,
          flushedDataSize = state.flushedDataSize + state.newDataSize,
          blockCount = state.blockCount,
          blockUpdateCount = state.blockUpdateCount + 1
        )
        updateInfo(Some(transaction))
      }
    }
  }

  private def flushImpl(forced: Boolean): Unit = {
    if (state.hasNewData) {
      val data =
        if (state.currentBlockIsFull()) buffer
        else buffer.slice(0, state.bufferPos)
      if (state.prevFlushWasPartial()) {
        if (state.blockUpdateCount < 3 || forced || state.currentBlockIsFull()) {
          updateBlock(data)
        }
      } else {
        writeNewBlock(data)
      }
    }
  }

  override def flush(): Unit = {
    flushImpl(false)
  }

  private def updateInfo(transaction: Option[ApiServiceTransaction] = None): Unit = {
    YtWrapper.updateRow(
      metaPath,
      metaSchema,
      new YtEventLogFileDetails(
        fileName, id, new YtEventLogFileMeta(rowSize, state.blockCount, state.flushedDataSize, clock.millis())
      ).toJavaMap,
      transaction
    )
  }

  override def close(): Unit = {
    flushImpl(true)
  }
}
