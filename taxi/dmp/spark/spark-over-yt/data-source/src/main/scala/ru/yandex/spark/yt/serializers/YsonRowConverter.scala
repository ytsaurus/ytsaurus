package ru.yandex.spark.yt.serializers

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import org.apache.log4j.Logger
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericRowWithSchema, UnsafeArrayData, UnsafeMapData, UnsafeRow}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import ru.yandex.bolts.collection.impl.EmptyMap
import ru.yandex.inside.yt.kosher.impl.ytree.YTreeEntityNodeImpl
import ru.yandex.inside.yt.kosher.impl.ytree.`object`.YTreeSerializer
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTree
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.{YTreeBinarySerializer, YTreeConsumer, YTreeTextSerializer, YsonEncoder, YsonTags}
import ru.yandex.inside.yt.kosher.ytree.{YTreeBooleanNode, YTreeNode}
import ru.yandex.misc.reflection.ClassX
import ru.yandex.yt.ytclient.proxy.TableWriter

import scala.annotation.tailrec
import scala.collection.mutable

class YsonRowConverter(schema: StructType, skipNulls: Boolean) extends YTreeSerializer[Row] {
  private val log = Logger.getLogger(getClass)

  private val indexedFields = schema.zipWithIndex
  private val entityNode = new YTreeEntityNodeImpl(new EmptyMap)

  override def getClazz: ClassX[Row] = ClassX.wrap(classOf[Row])

  private def skipNullsForField(field: StructField): Boolean = {
    field.metadata.contains("skipNulls") && field.metadata.getBoolean("skipNulls")
  }

  override def serialize(row: Row, consumer: YTreeConsumer): Unit = {
    consumer.onBeginMap()
    indexedFields.foreach { case (field, index) =>
      if (!skipNulls || !row.isNullAt(index)) {
        consumer.onKeyedItem(field.name)
        YsonRowConverter.serializeValue(row.get(index), field.dataType, skipNullsForField(field), consumer)
      }
    }
    consumer.onEndMap()
  }

  def serializeUnsafeRow(row: UnsafeRow, consumer: YTreeConsumer): Unit = {
    consumer.onBeginMap()
    indexedFields.foreach { case (field, index) =>
      if (!skipNulls || !row.isNullAt(index)) {
        consumer.onKeyedItem(field.name)
        YsonRowConverter.serializeValue(row.get(index, field.dataType), field.dataType, skipNullsForField(field), consumer)
      }
    }
    consumer.onEndMap()
  }

  def serializeInternalRow(row: InternalRow, consumer: YTreeConsumer): Unit = {
    consumer.onBeginMap()
    indexedFields.foreach { case (field, index) =>
      if (!skipNulls || !row.isNullAt(index)) {
        consumer.onKeyedItem(field.name)
        YsonRowConverter.serializeValue(row.get(index, field.dataType), field.dataType, skipNullsForField(field), consumer)
      }
    }
    consumer.onEndMap()
  }

  def serialize(row: Row, consumer: YsonEncoder): Unit = {
    consumer.onBeginMap()
    indexedFields.foreach { case (field, index) =>
      if (!skipNulls || !row.isNullAt(index)) {
        consumer.onKeyedItem(field.name)
        YsonRowConverter.serializeValue(row.get(index), field.dataType, skipNullsForField(field), consumer)
      }
    }
    consumer.onEndMap()
  }

  def serializeUnsafeRow(row: UnsafeRow, consumer: YsonEncoder): Unit = {
    consumer.onBeginMap()
    indexedFields.foreach { case (field, index) =>
      if (!skipNulls || !row.isNullAt(index)) {
        consumer.onKeyedItem(field.name)
        YsonRowConverter.serializeValue(row.get(index, field.dataType), field.dataType, skipNullsForField(field), consumer)
      }
    }
    consumer.onEndMap()
  }

  def serializeInternalRow(row: InternalRow, consumer: YsonEncoder): Unit = {
    consumer.onBeginMap()
    indexedFields.foreach { case (field, index) =>
      if (!skipNulls || !row.isNullAt(index)) {
        consumer.onKeyedItem(field.name)
        YsonRowConverter.serializeValue(row.get(index, field.dataType), field.dataType, skipNullsForField(field), consumer)
      }
    }
    consumer.onEndMap()
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
    val map = node.asMap()
    val values = new Array[Any](schema.fields.length)
    indexedFields.foreach { case (field, index) =>
      val name = field.name
      val node = map.getOrElse(name, entityNode)
      values(index) = YsonRowConverter.deserializeValue(node, field.dataType)
    }
    new GenericRowWithSchema(values, schema)
  }

  private val tableSchema = SchemaConverter.tableSchema(schema, Nil)

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

  def serializeValue(value: Any, dataType: DataType, skipNulls: Boolean, consumer: YTreeConsumer): Unit = {
    if (isNull(value)) {
      consumer.onEntity()
    } else {
      dataType match {
        case IntegerType => consumer.onInteger(value.asInstanceOf[Int].toLong)
        case LongType => consumer.onInteger(value.asInstanceOf[Long])
        case StringType =>
          value match {
            case str: String => consumer.onString(str)
            case str: UTF8String => consumer.onString(str.toString)
          }
        case BooleanType => consumer.onBoolean(value.asInstanceOf[Boolean])
        case DoubleType => consumer.onDouble(value.asInstanceOf[Double])
        case BinaryType => consumer.onBytes(value.asInstanceOf[Array[Byte]])
        case ArrayType(elementType, _) =>
          consumer.onBeginList()
          val iterable: Iterable[Any] = value match {
            case a: UnsafeArrayData => a.toSeq(elementType)
            case a: mutable.WrappedArray.ofRef[_] => a
            case a: Seq[_] => a
          }
          iterable.foreach { row =>
            consumer.onListItem()
            serializeValue(row, elementType, skipNulls = false, consumer)
          }
          consumer.onEndList()
        case t: StructType =>
          value match {
            case row: Row => YsonRowConverter.getOrCreate(t, skipNulls).serialize(row, consumer)
            case row: UnsafeRow => YsonRowConverter.getOrCreate(t, skipNulls).serializeUnsafeRow(row, consumer)
            case row: InternalRow => YsonRowConverter.getOrCreate(t, skipNulls).serializeInternalRow(row, consumer)
          }
        case MapType(StringType, valueType, _) =>
          consumer.onBeginMap()
          val map: Iterable[(Any, Any)] = value match {
            case m: Map[_, _] => m
            case m: UnsafeMapData => m.keyArray().toSeq[UTF8String](StringType).zip(m.valueArray().toSeq(valueType))
          }
          map.foreach { case (key, mapValue) =>
            consumer.onKeyedItem(key.toString)
            serializeValue(mapValue, valueType, skipNulls = false, consumer)
          }
          consumer.onEndMap()
      }
    }

  }

  def serializeValue(value: Any, dataType: DataType, skipNulls: Boolean, consumer: YsonEncoder): Unit = {
    if (isNull(value)) {
      consumer.onEntity()
    } else {
      dataType match {
        case IntegerType => consumer.onInteger(value.asInstanceOf[Int].toLong)
        case LongType => consumer.onInteger(value.asInstanceOf[Long])
        case StringType =>
          value match {
            case str: UTF8String => consumer.onString(str)
          }
        case BooleanType => consumer.onBoolean(value.asInstanceOf[Boolean])
        case DoubleType => consumer.onDouble(value.asInstanceOf[Double])
        case BinaryType => consumer.onBytes(value.asInstanceOf[Array[Byte]])
        case ArrayType(elementType, _) =>
          consumer.onBeginList()
          val iterable: Iterable[Any] = value match {
            case a: UnsafeArrayData => a.toSeq(elementType)
            case a: mutable.WrappedArray.ofRef[_] => a
            case a: Seq[_] => a
          }
          iterable.foreach { row =>
            consumer.onListItem()
            serializeValue(row, elementType, skipNulls = false, consumer)
          }
          consumer.onEndList()
        case t: StructType =>
          value match {
            case row: Row => YsonRowConverter.getOrCreate(t, skipNulls).serialize(row, consumer)
            case row: UnsafeRow => YsonRowConverter.getOrCreate(t, skipNulls).serializeUnsafeRow(row, consumer)
            case row: InternalRow => YsonRowConverter.getOrCreate(t, skipNulls).serializeInternalRow(row, consumer)
          }
        case MapType(StringType, valueType, _) =>
          consumer.onBeginMap()
          val map: Iterable[(Any, Any)] = value match {
            case m: Map[_, _] => m
            case m: UnsafeMapData => m.keyArray().toSeq[UTF8String](StringType).zip(m.valueArray().toSeq(valueType))
          }
          map.foreach { case (key, mapValue) =>
            consumer.onKeyedItem(key.toString)
            serializeValue(mapValue, valueType, skipNulls = false, consumer)
          }
          consumer.onEndMap()
      }
    }

  }

  def serializeToYson(value: Any, dataType: DataType, skipNulls: Boolean): YTreeNode = {
    val consumer = YTree.builder()
    serializeValue(value, dataType, skipNulls, consumer)
    consumer.build()
  }

  def serialize(value: Any, dataType: DataType, skipNulls: Boolean): Array[Byte] = {
    val output = new ByteArrayOutputStream
    YTreeBinarySerializer.serialize(serializeToYson(value, dataType, skipNulls), output)
    output.toByteArray
  }

  private val serializer: ThreadLocal[mutable.Map[StructType, YsonRowConverter]] = ThreadLocal.withInitial(() => mutable.ListMap.empty)

  def getOrCreate(schema: StructType, skipNulls: Boolean): YsonRowConverter = {
    serializer.get().getOrElseUpdate(schema, new YsonRowConverter(schema, skipNulls))
  }
}
