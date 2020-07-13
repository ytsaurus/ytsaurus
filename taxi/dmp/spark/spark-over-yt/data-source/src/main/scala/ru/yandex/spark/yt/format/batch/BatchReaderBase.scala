package ru.yandex.spark.yt.format.batch

import org.apache.spark.sql.vectorized.ColumnarBatch

abstract class BatchReaderBase(totalRowCount: Long) extends BatchReader {
  protected var _batch: ColumnarBatch = _

  protected var _rowsReturned = 0L
  protected var _currentBatchSize = 0

  protected def nextBatchInternal: Boolean

  protected def finalRead(): Unit

  override def currentBatchSize: Int = _currentBatchSize

  override def currentBatch: ColumnarBatch = _batch

  protected def setNumRows(num: Int): Unit = {
    _batch.setNumRows(num)
    _rowsReturned += num
    _currentBatchSize = num
  }

  override def nextBatch: Boolean = {
    _batch.setNumRows(0)
    if (_rowsReturned >= totalRowCount) {
      finalRead()
      false
    } else {
      nextBatchInternal
    }
  }
}
