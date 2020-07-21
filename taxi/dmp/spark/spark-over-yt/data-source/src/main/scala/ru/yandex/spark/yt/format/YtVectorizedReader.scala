package ru.yandex.spark.yt.format

import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import ru.yandex.spark.yt.format.batch.{ArrowBatchReader, BatchReader, EmptyColumnsBatchReader, WireRowBatchReader}
import ru.yandex.spark.yt.serializers.ArrayAnyDeserializer
import ru.yandex.spark.yt.wrapper.YtWrapper
import ru.yandex.yt.ytclient.proxy.YtClient

import scala.concurrent.duration.Duration

class YtVectorizedReader(split: YtInputSplit,
                         batchMaxSize: Int,
                         returnBatch: Boolean,
                         arrowEnabled: Boolean,
                         timeout: Duration)
                        (implicit yt: YtClient) extends RecordReader[Void, Object] {
  private var _batchIdx = 0

  private val batchReader: BatchReader = {
    val totalRowCount = split.file.length
    if (split.schema.nonEmpty) {
      val path = split.ytPath
      if (arrowEnabled) {
        val stream = YtWrapper.readTableArrowStream(path, timeout = timeout)
        new ArrowBatchReader(stream, totalRowCount, split.schema)
      } else {
        val schema = split.schema
        val rowIterator = YtWrapper.readTable(path, ArrayAnyDeserializer.getOrCreate(schema), timeout)
        new WireRowBatchReader(rowIterator, batchMaxSize, totalRowCount, schema)
      }
    } else {
      new EmptyColumnsBatchReader(totalRowCount)
    }
  }

  override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {}

  override def nextKeyValue(): Boolean = {
    if (returnBatch) {
      nextBatch
    } else {
      _batchIdx += 1
      if (_batchIdx >= batchReader.currentBatchSize) {
        if (nextBatch) {
          _batchIdx = 0
          true
        } else {
          false
        }
      } else {
        true
      }
    }
  }

  override def getCurrentKey: Void = null

  override def getCurrentValue: AnyRef = {
    if (returnBatch) {
      batchReader.currentBatch
    } else {
      batchReader.currentBatch.getRow(_batchIdx)
    }
  }

  override def getProgress: Float = 1f

  override def close(): Unit = {
    batchReader.close()
  }

  private def nextBatch: Boolean = {
    val next = batchReader.nextBatch
    _batchIdx = 0
    next
  }
}
