package tech.ytsaurus.spyt.serializers

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import tech.ytsaurus.client.rows.{UnversionedRowset, UnversionedValue}

class UnversionedRowsetDeserializer(schema: StructType) {
  private val deserializer = InternalRowDeserializer.getOrCreate(schema)

  private def deserializeValue(value: UnversionedValue): Unit = {
    value.getValue.asInstanceOf[Any] match {
      case v if v == null => deserializer.onEntity()
      case v: Boolean => deserializer.onBoolean(v)
      case v: Long => deserializer.onInteger(v)
      case v: Double => deserializer.onDouble(v)
      case v: Array[Byte] => deserializer.onBytes(v)
      case v => throw new MatchError(v)
    }
  }

  private def deserializeValues(values: Seq[UnversionedValue]): InternalRow = {
    deserializer.onNewRow(schema.length)
    values.zipWithIndex.foreach { case (value, index) =>
      deserializer.setId(index)
      deserializer.setType(value.getType)
      deserializeValue(value)
    }
    deserializer.onCompleteRow()
  }

  def deserializeRowset(rowset: UnversionedRowset): Iterator[InternalRow] = {
    import scala.collection.JavaConverters._
    rowset.getRows.asScala.iterator.map { row =>
      val rowValues = row.getValues.asScala
      val values = schema.fields.map { field =>
        rowValues.find(value => rowset.getSchema.getColumnName(value.getId) == field.name)
          .getOrElse(throw new IllegalStateException(s"${field.name} is not found in rowset"))
      }
      deserializeValues(values)
    }
  }
}
