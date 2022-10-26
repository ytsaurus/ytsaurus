package ru.yandex.spark.yt.format.batch

import org.apache.spark.sql.execution.vectorized.{OnHeapColumnVector, WritableColumnVector}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}
import ru.yandex.spark.yt.wrapper.table.TableIterator

class WireRowBatchReader(rowIterator: TableIterator[Array[Any]],
                         batchMaxSize: Int,
                         totalRowCount: Long,
                         schema: StructType) extends BatchReaderBase(totalRowCount) {
  private val columnVectors = OnHeapColumnVector.allocateColumns(batchMaxSize, schema)
    .asInstanceOf[Array[WritableColumnVector]]
  _batch = new ColumnarBatch(columnVectors.asInstanceOf[Array[ColumnVector]])

  override protected def nextBatchInternal: Boolean = {
    columnVectors.foreach(_.reset())
    val batchSize = Math.min(batchMaxSize.toLong, totalRowCount - _rowsReturned).toInt
    val readBatchSize = rowIterator.take(batchSize).zipWithIndex.foldLeft(0){case (count, (row, i)) =>
      for (j <- columnVectors.indices) {
        if (row(j) == null) {
          columnVectors(j).putNull(i)
        } else {
          columnVectors(j).dataType() match {
            case ShortType => columnVectors(j).putShort(i, row(j).asInstanceOf[Short])
            case StringType | BinaryType => columnVectors(j).putByteArray(i, row(j).asInstanceOf[Array[Byte]])
            case IntegerType => columnVectors(j).putInt(i, row(j).asInstanceOf[Int])
            case LongType => columnVectors(j).putLong(i, row(j).asInstanceOf[Long])
            case DoubleType => columnVectors(j).putDouble(i, row(j).asInstanceOf[Double])
            case BooleanType => columnVectors(j).putBoolean(i, row(j).asInstanceOf[Boolean])
          }
        }
      }

      count + 1
    }
    setNumRows(readBatchSize)
    if (readBatchSize != batchSize) throw new IllegalStateException("Unexpected end of rows")
    true
  }


  override protected def finalRead(): Unit = {
    rowIterator.hasNext
  }

  override def close(): Unit = {
    rowIterator.close()
  }
}
