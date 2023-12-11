package org.apache.spark.sql.vectorized

import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.slf4j.LoggerFactory
import tech.ytsaurus.spyt.format.{YPathUtils, YtInputSplit}
import tech.ytsaurus.spyt.format.batch.{ArrowBatchReader, BatchReader, EmptyColumnsBatchReader, WireRowBatchReader}
import tech.ytsaurus.spyt.serializers.ArrayAnyDeserializer
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.client.CompoundClient
import tech.ytsaurus.spyt.format.conf.SparkYtConfiguration

import scala.concurrent.duration.Duration

class YtVectorizedReader(split: YtInputSplit,
                         batchMaxSize: Int,
                         returnBatch: Boolean,
                         arrowEnabled: Boolean,
                         optimizedForScan: Boolean,
                         timeout: Duration,
                         reportBytesRead: Long => Unit,
                         countOptimizationEnabled: Boolean)
                        (implicit yt: CompoundClient) extends RecordReader[Void, Object] {
  private val log = LoggerFactory.getLogger(getClass)
  private var _batchIdx = 0

  private val batchReader: BatchReader = {
    val path = split.ytPathWithFilters
    log.info(s"Reading from $path")
    val schema = split.schema
    val totalRowCount = YPathUtils.rowCount(path)
    if (countOptimizationEnabled && schema.isEmpty && totalRowCount.isDefined) {
      // Empty schemas always batch readable
      new EmptyColumnsBatchReader(totalRowCount.get)
    } else if (arrowEnabled && optimizedForScan) {
      val stream = YtWrapper.readTableArrowStream(path, timeout, None, reportBytesRead)
      new ArrowBatchReader(stream, schema)
    } else {
      val rowIterator = YtWrapper.readTable(path, ArrayAnyDeserializer.getOrCreate(schema), timeout, None,
        reportBytesRead)
      new WireRowBatchReader(rowIterator, batchMaxSize, schema)
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
