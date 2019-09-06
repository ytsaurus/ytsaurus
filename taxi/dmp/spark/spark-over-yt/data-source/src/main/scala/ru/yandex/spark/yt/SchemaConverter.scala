package ru.yandex.spark.yt

import org.apache.spark.sql.types._
import ru.yandex.bolts.collection.impl.EmptyMap
import ru.yandex.inside.yt.kosher.impl.ytree.{YTreeListNodeImpl, YTreeMapNodeImpl, YTreeStringNodeImpl}
import ru.yandex.inside.yt.kosher.ytree.YTreeNode
import ru.yandex.yt.ytclient.tables.{ColumnSchema, ColumnValueType, TableSchema}

object SchemaConverter {
  def sparkType(sType: String): Option[DataType] = {
    sType match {
      case name if name startsWith "array" =>
        sparkType(name.drop("array(".length).dropRight(1)).map(elementType => ArrayType(elementType))
      case _ =>
        ColumnValueType.fromName(sType) match {
          case ColumnValueType.STRING => Some(StringType)
          case ColumnValueType.INT64 => Some(LongType)
          case ColumnValueType.DOUBLE => Some(DoubleType)
          case ColumnValueType.BOOLEAN => Some(BooleanType)
          case _ => None
        }
    }
  }

  def ytType(sparkType: DataType): ColumnValueType = {
    sparkType match {
      case StringType => ColumnValueType.STRING
      case IntegerType => ColumnValueType.INT64
      case LongType => ColumnValueType.INT64
      case DoubleType => ColumnValueType.DOUBLE
      case BooleanType => ColumnValueType.BOOLEAN
      case _: ArrayType => ColumnValueType.ANY
      case _: StructType => ColumnValueType.ANY
      case _: MapType => ColumnValueType.ANY
      case BinaryType => ColumnValueType.ANY
    }
  }

  def stringType(sparkType: DataType): String = {
    sparkType match {
      case StringType => ColumnValueType.STRING.getName
      case IntegerType => ColumnValueType.INT64.getName
      case LongType => ColumnValueType.INT64.getName
      case DoubleType => ColumnValueType.DOUBLE.getName
      case BooleanType => ColumnValueType.BOOLEAN.getName
      case ArrayType(elementType, _) =>
        "array(" + stringType(elementType) + ")"
    }
  }

  def structField(fieldName: String,
                  stringDataType: String,
                  metadata: Metadata): Option[StructField] = {
    sparkType(stringDataType).map(StructField(fieldName, _, metadata = metadata))
  }

  def structField(fieldName: String,
                  stringDataType: String,
                  schemaHint: Option[StructType],
                  metadata: Metadata): StructField = {
    schemaHint
      .flatMap(_.find(_.name == fieldName).map(_.copy(metadata = metadata)))
      .orElse(structField(fieldName, stringDataType, metadata))
      .getOrElse(StructField(fieldName, BinaryType, metadata = metadata))
  }

  def sparkSchema(schemaTree: YTreeNode, schemaHint: Option[StructType]): StructType = {
    import scala.collection.JavaConverters._
    StructType(schemaTree.asList().asScala.map { fieldSchema =>
      val fieldMap = fieldSchema.asMap()
      val originalName = fieldMap.getOrThrow("name").stringValue()
      val fieldName = originalName.replace(".", "_")
      val stringDataType = fieldMap.getOrThrow("type").stringValue()
      val metadata = new MetadataBuilder()
      metadata.putString("original_name", originalName)
      structField(fieldName, stringDataType, schemaHint, metadata.build())
    })
  }

  def ytSchema(sparkSchema: StructType): YTreeNode = {
    tableSchema(sparkSchema).toYTree
  }

  def tableSchema(sparkSchema: StructType): TableSchema = {
    val builder = new TableSchema.Builder()
      .setStrict(true)
      .setUniqueKeys(false)

    sparkSchema.foreach { field =>
      builder.add(new ColumnSchema(field.name, ytType(field.dataType)))
    }

    builder.build()
  }

}
