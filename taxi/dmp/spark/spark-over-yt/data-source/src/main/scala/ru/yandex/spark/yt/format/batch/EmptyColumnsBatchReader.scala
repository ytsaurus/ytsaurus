package ru.yandex.spark.yt.format.batch

import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}

class EmptyColumnsBatchReader(totalRowCount: Long) extends BatchReaderBase(totalRowCount) {
  private val columnVectors = OnHeapColumnVector.allocateColumns(1, StructType(Seq()))
  _batch = new ColumnarBatch(columnVectors.asInstanceOf[Array[ColumnVector]])

  override protected def nextBatchInternal: Boolean = {
    val num = Math.min(totalRowCount - _rowsReturned, Int.MaxValue).toInt
    setNumRows(num)
    true
  }


  override protected def finalRead(): Unit = {}

  override def close(): Unit = {}
}
