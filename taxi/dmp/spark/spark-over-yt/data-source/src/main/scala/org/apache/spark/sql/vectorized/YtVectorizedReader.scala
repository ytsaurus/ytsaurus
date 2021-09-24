package org.apache.spark.sql.vectorized

import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import ru.yandex.spark.yt.format.YtInputSplit
import ru.yandex.spark.yt.format.batch.{ArrowBatchReader, BatchReader, EmptyColumnsBatchReader, WireRowBatchReader}
import ru.yandex.spark.yt.serializers.ArrayAnyDeserializer
import ru.yandex.spark.yt.wrapper.YtWrapper
import ru.yandex.yt.ytclient.proxy.CompoundClient

import scala.concurrent.duration.Duration

class YtVectorizedReader(split: YtInputSplit,
                         batchMaxSize: Int,
                         returnBatch: Boolean,
                         arrowEnabled: Boolean,
                         timeout: Duration)
                        (implicit yt: CompoundClient) extends RecordReader[Void, Object] {
  private var _batchIdx = 0

  private val batchReader: BatchReader = {
    val totalRowCount = split.getLength
    if (split.schema.nonEmpty) {
      val path = split.ytPath
      if (arrowEnabled) {
        val stream = YtWrapper.readTableArrowStream(path, timeout)
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

  private lazy val unsafeProjection = if (arrowEnabled) {
    ColumnarBatchRowUtils.unsafeProjection(split.schema)
  } else {
    UnsafeProjection.create(split.schema)
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
      unsafeProjection.apply(batchReader.currentBatch.getRow(_batchIdx))
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
