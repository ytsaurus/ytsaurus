package org.apache.spark.sql.vectorized
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.{DataType, Decimal}
import org.apache.spark.unsafe.types.UTF8String

class SingleValueColumnVector(capacity: Int, dataType: DataType, fromRow: InternalRow, index: Int)
    extends ColumnVector(dataType) {
  override def close(): Unit = {}

  override def hasNull: Boolean = fromRow.isNullAt(index)

  override def numNulls(): Int =
    if (hasNull) capacity
    else 0

  override def isNullAt(rowId: Int): Boolean = {
    checkBounds(rowId)
    hasNull
  }

  override def getBoolean(rowId: Int): Boolean = {
    checkBounds(rowId)
    fromRow.getBoolean(index)
  }

  override def getByte(rowId: Int): Byte = {
    checkBounds(rowId)
    fromRow.getByte(index)
  }

  override def getShort(rowId: Int): Short = {
    checkBounds(rowId)
    fromRow.getShort(index)
  }

  override def getInt(rowId: Int): Int = {
    checkBounds(rowId)
    fromRow.getInt(index)
  }

  override def getLong(rowId: Int): Long = {
    checkBounds(rowId)
    fromRow.getLong(index)
  }

  override def getFloat(rowId: Int): Float = {
    checkBounds(rowId)
    fromRow.getFloat(index)
  }

  override def getDouble(rowId: Int): Double = {
    checkBounds(rowId)
    fromRow.getDouble(index)
  }

  override def getArray(rowId: Int): ColumnarArray =
    throw new UnsupportedOperationException("Arrays not supported")

  override def getMap(ordinal: Int): ColumnarMap =
    throw new UnsupportedOperationException("Maps not supported")

  override def getDecimal(rowId: Int, precision: Int, scale: Int): Decimal = {
    checkBounds(rowId)
    fromRow.getDecimal(index, precision, scale)
  }

  override def getUTF8String(rowId: Int): UTF8String = {
    checkBounds(rowId)
    fromRow.getUTF8String(index)
  }

  override def getBinary(rowId: Int): Array[Byte] = {
    checkBounds(rowId)
    fromRow.getBinary(index)
  }

  override def getChild(ordinal: Int): ColumnVector =
    throw new UnsupportedOperationException("Child vectors not supported")

  private def checkBounds(rowId: Int): Unit =
    if (rowId < 0 || rowId >= capacity)
      throw new IndexOutOfBoundsException(s"Row $rowId is out of bound. Vector capacity is $capacity" )
}
