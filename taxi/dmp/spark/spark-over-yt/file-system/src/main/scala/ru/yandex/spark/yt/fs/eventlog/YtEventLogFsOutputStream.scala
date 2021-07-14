package ru.yandex.spark.yt.fs.eventlog

import org.apache.hadoop.conf.Configuration
import ru.yandex.spark.yt.fs.PathUtils.getMetaPath
import ru.yandex.spark.yt.wrapper.YtWrapper
import ru.yandex.spark.yt.wrapper.model.EventLogSchema.{metaSchema, schema}
import ru.yandex.yt.ytclient.proxy.{ApiServiceTransaction, CompoundClient}

import java.io.OutputStream
import java.time.Clock
import java.util.UUID
import scala.annotation.tailrec

class YtEventLogFsOutputStream(conf: Configuration, path: String, fileName: String, clock: Clock, implicit val yt: CompoundClient) extends OutputStream {
  private val meta_path = getMetaPath(path)
  private var order = 0
  private val rowSize = conf.get("fs.ytEventLog.rowSize").toInt
  private val buffer: Array[Byte] = new Array[Byte](rowSize)
  private var bufferPos = 0
  private var flushedDataSize = 0
  private var lastFlushSize = rowSize
  private val id = s"${UUID.randomUUID()}"

  open()

  def open(): Unit = {
    YtWrapper.createDynTableAndMount(path, schema)
    YtWrapper.createDynTableAndMount(meta_path, metaSchema)
    syncInfo()
  }

  override def write(b: Int): Unit = {
    if (bufferPos == rowSize) {
      flush()
    }
    buffer(bufferPos) = b.toByte
    bufferPos += 1
  }

  override def write(event: Array[Byte], off: Int, len: Int): Unit = {
    writeImpl(event, off, len)
  }

  @tailrec
  private def writeImpl(event: Array[Byte], off: Int, len: Int): Unit = {
    val free = rowSize - bufferPos
    val copyLen = Math.min(free, len)
    System.arraycopy(event, off, buffer, bufferPos, copyLen)
    bufferPos += copyLen
    if (free < len) {
      flush()
      writeImpl(event, off + free, len - free)
    }
  }

  override def flush(): Unit = {
    // Second clause - detecting appending data
    // Either position was changed or buffer is full (it couldn't be full after previous flush)
    if (bufferPos > 0 && (lastFlushSize != bufferPos || bufferPos == rowSize)) {
      YtWrapper.runUnderTransaction(None)(transaction => {
        val data = if (bufferPos < buffer.length) buffer.slice(0, bufferPos) else buffer
        if (lastFlushSize < rowSize) {
          YtWrapper.updateRows(path, schema,
            new YtEventLogBlock(id, order, data).toJavaMap,
            Some(transaction)
          )
          flushedDataSize += bufferPos - lastFlushSize
        } else {
          YtWrapper.insertRows(path, schema,
            List(new YtEventLogBlock(id, order + 1, data).toList),
            Some(transaction)
          )
          order += 1
          flushedDataSize += bufferPos
        }
        lastFlushSize = bufferPos
        if (bufferPos == rowSize) {
          bufferPos = 0
        }
        syncInfo(Some(transaction))
      })
    }
  }

  private def syncInfo(transaction: Option[ApiServiceTransaction] = None): Unit = {
    updateInfo(rowSize, order, flushedDataSize, transaction)
  }

  private def updateInfo(rowSize: Int, blocksCnt: Int, length: Long, transaction: Option[ApiServiceTransaction] = None): Unit = {
    YtWrapper.updateRows(meta_path, metaSchema,
      new YtEventLogFileDetails(
        fileName, id, new YtEventLogFileMeta(rowSize, blocksCnt, length, clock.millis())
      ).toJavaMap,
      transaction)
  }

  override def close(): Unit = {
    flush()
  }
}
