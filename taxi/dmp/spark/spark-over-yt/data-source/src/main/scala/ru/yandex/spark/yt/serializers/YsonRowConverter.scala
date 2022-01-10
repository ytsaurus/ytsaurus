package ru.yandex.spark.yt.serializers

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericRowWithSchema, UnsafeArrayData, UnsafeMapData, UnsafeRow}
import org.apache.spark.sql.catalyst.util.MapData
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import ru.yandex.bolts.collection.impl.EmptyMap
import ru.yandex.inside.yt.kosher.impl.ytree.YTreeEntityNodeImpl
import ru.yandex.inside.yt.kosher.impl.ytree.`object`.YTreeSerializer
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTree
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.{YTreeBinarySerializer, YTreeTextSerializer, YsonTags}
import ru.yandex.inside.yt.kosher.ytree.{YTreeBooleanNode, YTreeNode}
import ru.yandex.spark.yt.serializers.YsonRowConverter.{isNull, serializeValue}
import ru.yandex.yson.YsonConsumer
import ru.yandex.yt.ytclient.proxy.TableWriter
import ru.yandex.yt.ytclient.tables.ColumnValueType

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import scala.annotation.tailrec
import scala.collection.mutable

class YsonRowConverter(schema: StructType, ytSchema: YtTypeHolder,
                       config: YsonEncoderConfig) extends YTreeSerializer[Row] {
  private val entityNode = new YTreeEntityNodeImpl(new EmptyMap)
  private val indexedFields = schema.zipWithIndex
  private val indexedFieldsWithHints = {
    if (ytSchema.supportsSearchByName) genHints((f, _) => ytSchema.getByName(f.name))
    else if (ytSchema.supportsSearchByIndex) genHints((_, i) => ytSchema.getByIndex(i))
    else genHints((_, _) => YtTypeHolder.empty)
  }

  private def genHints(f: (StructField, Int) => YtTypeHolder): Seq[(StructField, Int, YtTypeHolder)] = {
    indexedFields.map { case (field, index) => (field, index, f(field, index)) }
  }

  override def getClazz: Class[Row] = classOf[Row]

  override def getColumnValueType: ColumnValueType = ColumnValueType.ANY

  private def skipNullsForField(field: StructField): Boolean = {
    field.metadata.contains("skipNulls") && field.metadata.getBoolean("skipNulls")
  }

  private def serializeField(value: Any, field: StructField, consumer: YsonConsumer, hint: YtTypeHolder): Unit = {
    YsonRowConverter.serializeValue(value, field.dataType,
      YsonEncoderConfig(skipNullsForField(field), config.typeV3Format), consumer, hint)
  }

  def serialize(row: Row, consumer: YsonConsumer): Unit = {
    consumer.onBeginMap()
    indexedFieldsWithHints.foreach { case (field, index, hint) =>
      if (!config.skipNulls || !row.isNullAt(index)) {
        consumer.onKeyedItem(field.name)
        serializeField(row.get(index), field, consumer, hint)
      }
    }
    consumer.onEndMap()
  }

  def serializeAnyRow(row: Any, consumer: YsonConsumer): Unit = {
    row match {
      case row: Row => serialize(row, consumer)
      case row: UnsafeRow => serializeUnsafeRow(row, consumer)
      case row: InternalRow => serializeInternalRow(row, consumer)
    }
  }

  def serializeUnsafeRow(row: UnsafeRow, consumer: YsonConsumer): Unit = serializeInternalRow(row, consumer)

  def serializeInternalRow(row: InternalRow, consumer: YsonConsumer): Unit = {
    consumer.onBeginMap()
    indexedFieldsWithHints.foreach { case (field, index, hint) =>
      if (!config.skipNulls || !row.isNullAt(index)) {
        consumer.onKeyedItem(field.name)
        serializeField(row.get(index, field.dataType), field, consumer, hint)
      }
    }
    consumer.onEndMap()
  }

  def serializeAsList(row: Row, consumer: YsonConsumer): Unit = {
    consumer.onBeginList()
    indexedFieldsWithHints.foreach { case (field, index, hint) =>
      if (!config.skipNulls || !row.isNullAt(index)) {
        consumer.onListItem()
        serializeField(row.get(index), field, consumer, hint)
      }
    }
    consumer.onEndList()
  }

  def serializeAnyRowAsList(value: Any, consumer: YsonConsumer): Unit = {
    value match {
      case row: Row => serializeAsList(row, consumer)
      case row: UnsafeRow => serializeUnsafeRowAsList(row, consumer)
      case row: InternalRow => serializeInternalRowAsList(row, consumer)
    }
  }

  def serializeUnsafeRowAsList(row: UnsafeRow, consumer: YsonConsumer): Unit = serializeInternalRowAsList(row, consumer)

  def serializeInternalRowAsList(row: InternalRow, consumer: YsonConsumer): Unit = {
    consumer.onBeginList()
    indexedFieldsWithHints.foreach { case (field, index, hint) =>
      if (!config.skipNulls || !row.isNullAt(index)) {
        consumer.onListItem()
        serializeField(row.get(index, field.dataType), field, consumer, hint)
      }
    }
    consumer.onEndList()
  }

  def rowToYson(row: Row): YTreeNode = {
    val consumer = YTree.builder()
    serialize(row, consumer)
    consumer.build()
  }

  def serialize(row: Row): Array[Byte] = {
    val output = new ByteArrayOutputStream(200)
    YTreeBinarySerializer.serialize(rowToYson(row), output)
    output.toByteArray
  }

  override def deserialize(node: YTreeNode): Row = {
    import ru.yandex.spark.yt.wrapper.YtJavaConverters._
    val map = node.asMap()
    val values = new Array[Any](schema.fields.length)
    indexedFields.foreach { case (field, index) =>
      val name = field.name
      val node = map.getOption(name).getOrElse(entityNode)
      values(index) = YsonRowConverter.deserializeValue(node, field.dataType)
    }
    new GenericRowWithSchema(values, schema)
  }

  private def serializeVariant(value: Any, consumer: YsonConsumer): Unit = {
    val notNulls = value match {
      case row: Row =>
        indexedFieldsWithHints.filter{ case (_, index, _) => !isNull(row(index))}
      case row: InternalRow =>
        indexedFieldsWithHints.filter{ case (field, index, _) => !isNull(row.get(index, field.dataType))}
    }
    if (notNulls.isEmpty) {
      throw new IllegalArgumentException("All elements in variant is null")
    } else if (notNulls.size > 1) {
      throw new IllegalArgumentException("Not null element must be single")
    } else {
      val (field, index, hint) = notNulls.head
      consumer.onBeginList()

      consumer.onListItem()
      consumer.onInteger(index)

      val item = value match {
        case row: Row => row(index)
        case row: InternalRow => row.get(index, field.dataType)
      }
      consumer.onListItem()
      serializeValue(item, field.dataType, YsonEncoderConfig(skipNullsForField(field), config.typeV3Format), consumer, hint)

      consumer.onEndList()
    }
  }

  def serializeStruct(value: Any, consumer: YsonConsumer): Unit = {
    if (ytSchema.isVariant) {
      serializeVariant(value, consumer)
    } else if (ytSchema.isTuple || (ytSchema.isStruct && config.typeV3Format)) {
      serializeAnyRowAsList(value, consumer)
    } else {
      serializeAnyRow(value, consumer)
    }
  }

  private val tableSchema = SchemaConverter.tableSchema(schema, Nil, Map.empty)

  @tailrec
  final def writeRows(writer: TableWriter[Row], rows: Seq[Row]): Unit = {
    import scala.collection.JavaConverters._
    if (!writer.write(rows.asJava, tableSchema)) {
      writer.readyEvent().join()
      writeRows(writer, rows)
    }
  }
}

object YsonRowConverter {
  def deserializeValue(node: YTreeNode, dataType: DataType): Any = {
    if (node.isEntityNode) {
      null
    } else {
      dataType match {
        case StringType => node match {
          case _: YTreeBooleanNode => node.boolValue().toString
          case _ => node.stringValue()
        }
        case LongType => node.longValue()
        case BooleanType => node.boolValue()
        case DoubleType => node.doubleValue()
        case BinaryType => node.bytesValue()
        case ArrayType(elementType, _) =>
          val input = new ByteArrayInputStream(node.toBinary.dropWhile(_ != YsonTags.BEGIN_LIST))
          val deserialized = YTreeTextSerializer.deserialize(input).asList()
          elementType match {
            case StringType =>
              val builder = Array.newBuilder[String]
              deserialized.forEach((node: YTreeNode) => {
                builder += node.stringValue()
              })
              builder.result()
            case LongType =>
              val builder = Array.newBuilder[Long]
              deserialized.forEach((node: YTreeNode) => {
                builder += node.longValue()
              })
              builder.result()
          }
      }
    }
  }

  private def isNull(any: Any): Boolean = {
    any match {
      case null => true
      case None => true
      case _ => false
    }
  }

  def serializeValue(value: Any, dataType: DataType, config: YsonEncoderConfig, consumer: YsonConsumer,
                     ytType: YtTypeHolder = YtTypeHolder.empty): Unit = {
    if (isNull(value)) {
      consumer.onEntity()
    } else {
      dataType match {
        case IntegerType => consumer.onInteger(value.asInstanceOf[Int].toLong)
        case LongType => consumer.onInteger(value.asInstanceOf[Long])
        case StringType =>
          value match {
            case str: UTF8String =>
              val bytes = str.getBytes
              consumer.onString(bytes, 0, bytes.length)
            case str: String => consumer.onString(str)
          }
        case BooleanType => consumer.onBoolean(value.asInstanceOf[Boolean])
        case DoubleType => consumer.onDouble(value.asInstanceOf[Double])
        case BinaryType =>
          val bytes = value.asInstanceOf[Array[Byte]]
          consumer.onString(bytes, 0, bytes.length)
        case array: ArrayType =>
          serializeArray(value, array, ytType, config, consumer)
        case t: StructType =>
          YsonRowConverter.getOrCreate(t, ytType, config).serializeStruct(value, consumer)
        case map: MapType =>
          serializeMap(value, map, ytType, config, consumer)
      }
    }
  }

  private def serializeArray(value: Any, arrayType: ArrayType, ytType: YtTypeHolder,
                             config: YsonEncoderConfig, consumer: YsonConsumer): Unit = {
    val elementType = arrayType.elementType
    consumer.onBeginList()
    val iterable: Iterable[Any] = value match {
      case a: UnsafeArrayData => a.toSeq(elementType)
      case a: mutable.WrappedArray.ofRef[_] => a
      case a: Seq[_] => a
    }
    iterable.foreach { row =>
      consumer.onListItem()
      serializeValue(row, elementType, YsonEncoderConfig(skipNulls = false, config.typeV3Format),
        consumer, ytType.getListItem)
    }
    consumer.onEndList()
  }

  private def serializeMapTypeV3(map: Iterable[(Any, Any)], mapType: MapType, ytType: YtTypeHolder,
                                 consumer: YsonConsumer): Unit = {
    if (!mapType.valueContainsNull) {
      map.foreach { case (_, value) =>
        if (value == null) {
          throw new IllegalArgumentException("Try to write null value to non-null column")
        }
      }
    }
    consumer.onBeginList()
    map.foreach { case (key, value) =>
      consumer.onListItem()
      consumer.onBeginList()

      consumer.onListItem()
      serializeValue(key, mapType.keyType,
        YsonEncoderConfig(skipNulls = false, typeV3Format = true), consumer, ytType.getMapKey)

      consumer.onListItem()
      serializeValue(value, mapType.valueType,
        YsonEncoderConfig(skipNulls = false, typeV3Format = true), consumer, ytType.getMapValue)

      consumer.onEndList()
    }
    consumer.onEndList()
  }

  private def serializeMapTypeV1(map: Iterable[(Any, Any)], mapType: MapType, consumer: YsonConsumer): Unit = {
    consumer.onBeginMap()
    map.foreach { case (key, mapValue) =>
      consumer.onKeyedItem(key.toString)
      serializeValue(mapValue, mapType.valueType, YsonEncoderConfig(skipNulls = false, typeV3Format = false), consumer)
    }
    consumer.onEndMap()
  }

  private def serializeMap(value: Any, mapType: MapType, ytType: YtTypeHolder,
                           config: YsonEncoderConfig, consumer: YsonConsumer): Unit = {
    val map: Iterable[(Any, Any)] = getMapData(value, mapType.keyType, mapType.valueType)
    if (config.typeV3Format) {
      serializeMapTypeV3(map, mapType, ytType, consumer)
    } else {
      serializeMapTypeV1(map, mapType, consumer)
    }
  }

  def serializeToYson(value: Any, dataType: DataType, skipNulls: Boolean): YTreeNode = {
    val consumer = YTree.builder()
    serializeValue(value, dataType, YsonEncoderConfig(skipNulls, typeV3Format = false), consumer)
    consumer.build()
  }

  def serialize(value: Any, dataType: DataType, skipNulls: Boolean): Array[Byte] = {
    val output = new ByteArrayOutputStream
    YTreeBinarySerializer.serialize(serializeToYson(value, dataType, skipNulls), output)
    output.toByteArray
  }

  private val serializer: ThreadLocal[mutable.Map[(StructType, YtTypeHolder), YsonRowConverter]] = {
    ThreadLocal.withInitial(() => mutable.ListMap.empty)
  }

  def getOrCreate(schema: StructType, ytSchema: YtTypeHolder = YtTypeHolder.empty,
                  config: YsonEncoderConfig): YsonRowConverter = {
    serializer.get().getOrElseUpdate((schema, ytSchema),
      new YsonRowConverter(schema, ytSchema, config))
  }

  private def getMapData(value: Any, keyType: DataType, valueType: DataType): Iterable[(Any, Any)] = {
    keyType match {
      case StringType =>
        sortMapData(value, valueType)
      case _ =>
        value match {
          case m: Map[_, _] => m
          case m: UnsafeMapData =>
            m.keyArray().toSeq(keyType).zip(m.valueArray().toSeq(valueType))
        }
    }
  }

  private[serializers] def sortMapData(mapData: Any, valueType: DataType): Iterable[(Any, Any)] = {
    mapData match {
      case m: Map[_, _] =>
        m.toSeq.sortBy(_._1.toString)
      case m: MapData =>
        m.keyArray().toSeq[UTF8String](StringType)
          .zip(m.valueArray().toSeq(valueType))
          .sortBy(_._1)
    }
  }
}

case class YsonEncoderConfig(skipNulls: Boolean, typeV3Format: Boolean)
