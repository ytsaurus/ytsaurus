package org.apache.spark.sql.vectorized

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.util.{ArrayData, GenericArrayData, MapData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.spark.YsonDecoder
import ru.yandex.spark.yt.serializers.SchemaConverter

object ColumnarBatchRowUtils {
  def unsafeProjection(schema: StructType): InternalRow => UnsafeRow = {
    val unsafeProjection = UnsafeProjection.create(schema)
    val mutableColumnarRowProjection = new ColumnarBatchRowProjection(schema)

    r => unsafeProjection.apply(mutableColumnarRowProjection.apply(r))
  }

  class ColumnarBatchRowProjection(schema: StructType) {
    private val indexedDataTypes = schema.fields.map(f => SchemaConverter.indexedDataType(f.dataType))

    def apply(row: InternalRow): InternalRow = new ColumnarBatchRowWrapper(row)

    private class ColumnarBatchRowWrapper(row: InternalRow) extends InternalRow {
      override def numFields: Int = row.numFields

      override def setNullAt(i: Int): Unit = row.setNullAt(i)

      override def update(i: Int, value: Any): Unit = row.update(i, value)

      override def copy(): InternalRow = row.copy()

      override def isNullAt(ordinal: Int): Boolean = row.isNullAt(ordinal)

      override def getBoolean(ordinal: Int): Boolean = row.getBoolean(ordinal)

      override def getByte(ordinal: Int): Byte = row.getByte(ordinal)

      override def getShort(ordinal: Int): Short = row.getShort(ordinal)

      override def getInt(ordinal: Int): Int = row.getInt(ordinal)

      override def getLong(ordinal: Int): Long = row.getLong(ordinal)

      override def getFloat(ordinal: Int): Float = row.getFloat(ordinal)

      override def getDouble(ordinal: Int): Double = row.getDouble(ordinal)

      override def getDecimal(ordinal: Int, precision: Int, scale: Int): Decimal = row.getDecimal(ordinal, precision, scale)

      override def getUTF8String(ordinal: Int): UTF8String = row.getUTF8String(ordinal)

      override def getBinary(ordinal: Int): Array[Byte] = row.getBinary(ordinal)

      override def getInterval(ordinal: Int): CalendarInterval = ???

      override def getStruct(ordinal: Int, numFields: Int): InternalRow = {
        YsonDecoder.decode(row.getBinary(ordinal), indexedDataTypes(ordinal)).asInstanceOf[InternalRow]
      }

      override def getArray(ordinal: Int): ArrayData = {
        YsonDecoder.decode(row.getBinary(ordinal), indexedDataTypes(ordinal)).asInstanceOf[GenericArrayData]
      }

      override def getMap(ordinal: Int): MapData = {
        YsonDecoder.decode(row.getBinary(ordinal), indexedDataTypes(ordinal)).asInstanceOf[MapData]
      }

      override def get(ordinal: Int, dataType: DataType): AnyRef = {
        dataType match {
          case StructType(fields) => getStruct(ordinal, fields.length)
          case ArrayType(_, _) => getArray(ordinal)
          case MapType(_, _, _) => getMap(ordinal)
          case _ => row.get(ordinal, dataType)
        }
      }
    }


  }

}
