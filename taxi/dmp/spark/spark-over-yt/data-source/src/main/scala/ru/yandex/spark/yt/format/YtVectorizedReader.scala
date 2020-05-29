package ru.yandex.spark.yt.format

import org.apache.hadoop.mapreduce.{InputSplit, RecordReader, TaskAttemptContext}
import org.apache.spark.sql.execution.vectorized.{OnHeapColumnVector, WritableColumnVector}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}
import ru.yandex.spark.yt.serializers.ArrayAnyDeserializer
import ru.yandex.spark.yt.wrapper.YtWrapper
import ru.yandex.yt.ytclient.proxy.YtClient

import scala.concurrent.duration.Duration

class YtVectorizedReader(capacity: Int, timeout: Duration)(implicit yt: YtClient) extends RecordReader[Void, Object] {
  private var _iterator: Iterator[Array[Any]] = _
  private var _batch: ColumnarBatch = _
  private var _columnVectors: Array[WritableColumnVector] = _
  private var _rowsReturned = 0L
  private var _totalRowCount: Long = _
  private var _returnBatch = false
  private var _batchIdx = 0
  private var _numBatched = 0

  override def initialize(split: InputSplit, context: TaskAttemptContext): Unit = {
    split match {
      case ys: YtInputSplit =>
        _columnVectors = OnHeapColumnVector.allocateColumns(capacity, ys.schema).asInstanceOf[Array[WritableColumnVector]]
        _batch = new ColumnarBatch(_columnVectors.asInstanceOf[Array[ColumnVector]])
        _totalRowCount = ys.getLength

        if (_columnVectors.nonEmpty) {
          _iterator = YtWrapper.readTable(ys.ytPath, ArrayAnyDeserializer.getOrCreate(ys.schema), timeout)
        }
    }
  }

  override def nextKeyValue(): Boolean = {
    if (_returnBatch) {
      nextBatch
    } else {
      if (_batchIdx >= _numBatched && !nextBatch) {
        false
      } else {
        _batchIdx += 1
        true
      }
    }
  }

  override def getCurrentKey: Void = null

  override def getCurrentValue: AnyRef = {
    if (_returnBatch) {
      _batch
    } else {
      val res = _batch.getRow(_batchIdx)
      _batchIdx += 1
      res
    }
  }

  override def getProgress: Float = 1f

  override def close(): Unit = {

  }

  def enableBatch(): Unit = {
    _returnBatch = true
  }

  def nextBatch: Boolean = {
    _columnVectors.foreach(_.reset())
    _batch.setNumRows(0)
    if (_rowsReturned >= _totalRowCount) {
      false
    } else {
      val num = if (_columnVectors.nonEmpty) {
        Math.min(capacity.toLong, _totalRowCount - _rowsReturned).toInt
      } else {
        (_totalRowCount - _rowsReturned).toInt
      }

      if (_columnVectors.nonEmpty) {
        readBatchFromIterator(num)
      }

      _batch.setNumRows(num)
      _rowsReturned += num
      _batchIdx = 0
      _numBatched = num
      true
    }
  }

  private def readBatchFromIterator(batchSize: Int): Unit = {
    for (i <- 0 until batchSize) {
      val row = if (_iterator.hasNext) _iterator.next() else throw new IllegalStateException("")
      for (j <- _columnVectors.indices) {
        if (row(j) == null) {
          _columnVectors(j).putNull(i)
        } else {
          _columnVectors(j).dataType() match {
            case StringType => _columnVectors(j).putByteArray(i, row(j).asInstanceOf[Array[Byte]])
            case LongType => _columnVectors(j).putLong(i, row(j).asInstanceOf[Long])
            case BooleanType => _columnVectors(j).putBoolean(i, row(j).asInstanceOf[Boolean])
            case IntegerType => _columnVectors(j).putInt(i, row(j).asInstanceOf[Int])
          }
        }
      }
    }
  }
}
