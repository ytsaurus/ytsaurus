package tech.ytsaurus.spyt.serializers

import org.apache.spark.sql.types._
import org.apache.spark.sql.yson.{UInt64Type, YsonType}
import org.apache.spark.sql.{DataFrame, Row}
import tech.ytsaurus.client.rows.{WireProtocolWriteable, WireProtocolWriter, WireRowSerializer}
import tech.ytsaurus.core.tables.{ColumnValueType, TableSchema}
import tech.ytsaurus.spyt.serialization.YsonEncoder
import tech.ytsaurus.spyt.serializers.SchemaConverter.Unordered

import java.util.Base64

// TODO(alex-shishkin): Supported type v1 only
class GenericRowSerializer(schema: StructType) extends WireRowSerializer[Row] {
  private val tableSchema = SchemaConverter.tableSchema(schema, Unordered, Map.empty)

  override def getSchema: TableSchema = tableSchema

  private def valueId(id: Int, idMapping: Array[Int]): Int = {
    if (idMapping != null) {
      idMapping(id)
    } else id
  }

  private def writeHeader(writeable: WireProtocolWriteable, idMapping: Array[Int], aggregate: Boolean,
                          i: Int, length: Int): Unit = {
    writeable.writeValueHeader(valueId(i, idMapping), tableSchema.getColumnType(i), aggregate, length)
  }

  private def writeBytes(writeable: WireProtocolWriteable, idMapping: Array[Int], aggregate: Boolean,
                         i: Int, bytes: Array[Byte]): Unit = {
    writeHeader(writeable, idMapping, aggregate, i, bytes.length)
    writeable.onBytes(bytes)
  }

  override def serializeRow(row: Row,
                            writeable: WireProtocolWriteable,
                            keyFieldsOnly: Boolean,
                            aggregate: Boolean,
                            idMapping: Array[Int]): Unit = {
    writeable.writeValueCount(row.length)
    for {
      i <- 0 until row.length
    } {
      if (row.isNullAt(i)) {
        writeable.writeValueHeader(valueId(i, idMapping), ColumnValueType.NULL, aggregate, 0)
      } else {
        val sparkField = schema(i)
        sparkField.dataType match {
          case BinaryType | YsonType => writeBytes(writeable, idMapping, aggregate, i, row.getAs[Array[Byte]](i))
          case StringType => writeBytes(writeable, idMapping, aggregate, i, row.getString(i).getBytes)
          case t@(ArrayType(_, _) | StructType(_) | MapType(_, _, _)) =>
            val skipNulls = sparkField.metadata.contains("skipNulls") && sparkField.metadata.getBoolean("skipNulls")
            writeBytes(writeable, idMapping, aggregate, i, YsonEncoder.encode(row.get(i), t, skipNulls))
          case atomic =>
            writeHeader(writeable, idMapping, aggregate, i, 0)
            atomic match {
              case ByteType => writeable.onInteger(row.getByte(i))
              case ShortType => writeable.onInteger(row.getShort(i))
              case IntegerType => writeable.onInteger(row.getInt(i))
              case LongType => writeable.onInteger(row.getLong(i))
              case BooleanType => writeable.onBoolean(row.getBoolean(i))
              case FloatType => writeable.onDouble(row.getFloat(i))
              case DoubleType => writeable.onDouble(row.getDouble(i))
              case UInt64Type => writeable.onInteger(row.getLong(i))
              case DateType => writeable.onInteger(row.getLong(i))
              case TimestampType => writeable.onInteger(row.getLong(i) / 1000000)
            }
        }
      }
    }
  }
}

object GenericRowSerializer {
  def dfToYTFormatWithBase64(df: DataFrame): String = Base64.getEncoder.encodeToString(dfToYTFormat(df))

  def dfToYTFormat(df: DataFrame): Array[Byte] = rowsToYTFormat(df.schema, df.collect())

  def rowsToYTFormat(schema: StructType, rows: Array[Row]): Array[Byte] = {
    serializeRows(new GenericRowSerializer(schema), rows)
  }

  def serializeRows[T](wireRowSerializer: WireRowSerializer[T], rows: Array[T]): Array[Byte] = {
    import scala.collection.JavaConverters._
    val writer = new WireProtocolWriter
    writer.writeUnversionedRowset(rows.toList.asJava, wireRowSerializer)
    val result = writer.finish
    result.get(0)
  }
}
