package ru.yandex.spark.yt

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import org.apache.log4j.Logger
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import ru.yandex.bolts.collection.impl.EmptyMap
import ru.yandex.inside.yt.kosher.impl.ytree.YTreeEntityNodeImpl
import ru.yandex.inside.yt.kosher.impl.ytree.`object`.YTreeSerializer
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTree
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.{YTreeBinarySerializer, YTreeConsumer, YTreeTextSerializer, YsonTags}
import ru.yandex.inside.yt.kosher.ytree.{YTreeBooleanNode, YTreeNode}
import ru.yandex.misc.reflection.ClassX
import ru.yandex.yt.ytclient.proxy.TableWriter

import scala.annotation.tailrec
import scala.collection.mutable

class YsonRowConverter(schema: StructType) extends YTreeSerializer[Row] {
  private val log = Logger.getLogger(getClass)

  private val indexedFields = schema.zipWithIndex
  private val entityNode = new YTreeEntityNodeImpl(new EmptyMap)

  override def getClazz: ClassX[Row] = ClassX.wrap(classOf[Row])

  def serializeValue(value: Any, dataType: DataType, consumer: YTreeConsumer): Unit = {
    if (isNull(value)) {
      consumer.onEntity()
    } else {
      dataType match {
        case IntegerType => consumer.onInteger(value.asInstanceOf[Int].toLong)
        case LongType => consumer.onInteger(value.asInstanceOf[Long])
        case StringType => consumer.onString(value.asInstanceOf[String])
        case BooleanType => consumer.onBoolean(value.asInstanceOf[Boolean])
        case DoubleType => consumer.onDouble(value.asInstanceOf[Double])
        case BinaryType => consumer.onBytes(value.asInstanceOf[Array[Byte]])
        case ArrayType(elementType, _) =>
          consumer.onBeginList()
          value.asInstanceOf[mutable.WrappedArray.ofRef[AnyRef]].foreach { row =>
            consumer.onListItem()
            serializeValue(row, elementType, consumer)
          }
          consumer.onEndList()
        case t: StructType => YsonRowConverter.getOrCreate(t).serialize(value.asInstanceOf[Row], consumer)
        case MapType(StringType, valueType, _) =>
          consumer.onBeginMap()
          value.asInstanceOf[Map[String, Any]].foreach { case (key, mapValue) =>
            consumer.onKeyedItem(key)
            serializeValue(mapValue, valueType, consumer)
          }
          consumer.onEndMap()
      }
    }

  }

  override def serialize(row: Row, consumer: YTreeConsumer): Unit = {
    consumer.onBeginMap()
    indexedFields.foreach { case (field, index) =>
      consumer.onKeyedItem(field.name)
      serializeValue(row.get(index), field.dataType, consumer)
    }
    consumer.onEndMap()
  }

  private def isNull(any: Any): Boolean = {
    any match {
      case null => true
      case None => true
      case _ => false
    }
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
      //      val name = field.metadata.getString("original_name")
      val name = field.name
      val node = map.getOrElse(name, entityNode)
      values(index) = YsonRowConverter.deserializeValue(node, field.dataType)
    }
    new GenericRowWithSchema(values, schema)
  }

  @tailrec
  final def writeRows(writer: TableWriter[Row], rows: Seq[Row]): Unit = {
    import scala.collection.JavaConverters._
    val tableSchema = SchemaConverter.tableSchema(schema)
    if (!writer.write(rows.asJava, tableSchema)) {
      writer.readyEvent().join()
      writeRows(writer, rows)
    }
  }
}

object YsonRowConverter {
  private def toNull(dataType: DataType): Any = dataType match {
    case StringType => null.asInstanceOf[String]
    case LongType => null.asInstanceOf[Long]
    case BooleanType => null.asInstanceOf[Boolean]
    case DoubleType => null.asInstanceOf[Double]
    case BinaryType => null.asInstanceOf[Array[Byte]]
    case ArrayType(_, _) => null.asInstanceOf[Array[Any]]
    case _: StructType => null.asInstanceOf[Row]
    case _: MapType => null.asInstanceOf[Map[String, Any]]
  }

  def deserializeValue(node: YTreeNode, dataType: DataType): Any = {
    if (node.isEntityNode) {
      toNull(dataType)
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
        case ArrayType(elementType, containsNull) =>
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

  private val serializer: ThreadLocal[mutable.Map[StructType, YsonRowConverter]] = ThreadLocal.withInitial(() => mutable.ListMap.empty)

  def getOrCreate(schema: StructType): YsonRowConverter = {
    serializer.get().getOrElseUpdate(schema, new YsonRowConverter(schema))
  }
}
