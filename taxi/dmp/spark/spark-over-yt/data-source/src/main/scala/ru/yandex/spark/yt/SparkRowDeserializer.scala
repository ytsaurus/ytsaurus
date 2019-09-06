package ru.yandex.spark.yt

import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import java.util.function.BiConsumer

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import ru.yandex.bolts.collection.{ListF, MapF}
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.{YTreeTextSerializer, YsonTags}
import ru.yandex.inside.yt.kosher.ytree.YTreeNode
import ru.yandex.yt.ytclient.`object`.{WireRowDeserializer, WireValueDeserializer}
import ru.yandex.yt.ytclient.tables.ColumnValueType

import scala.collection.mutable

class SparkRowDeserializer(val schema: StructType, filters: Array[Filter]) extends WireRowDeserializer[Row] with WireValueDeserializer[Any] {
  private var _values: Array[Any] = _
  private val nulls = schema.map { field =>
    field.dataType match {
      case StringType => null.asInstanceOf[String]
      case LongType => null.asInstanceOf[Long]
      case BooleanType => null.asInstanceOf[Boolean]
      case DoubleType => null.asInstanceOf[Double]
      case BinaryType => null.asInstanceOf[Array[Byte]]
      case ArrayType(_, _) => null.asInstanceOf[Array[Any]]
      case _: StructType => null.asInstanceOf[Row]
      case _: MapType => null.asInstanceOf[Map[String, Any]]
    }
  }

  private val rowSerializers = schema.zipWithIndex.collect {
    case (field, index) if field.dataType.isInstanceOf[StructType] =>
      index -> new YsonRowConverter(field.dataType.asInstanceOf[StructType])
  }.toMap


  private val indexedSchema = schema.fields.map(_.dataType).toIndexedSeq

  private var _index = 0

  override def onNewRow(columnCount: Int): WireValueDeserializer[_] = {
    _values = new Array[Any](schema.length)
    _index = 0
    this
  }

  override def onCompleteRow(): Row = {
    new GenericRow(_values)
  }

  override def setId(id: Int): Unit = {
    _index = id
  }

  override def setType(`type`: ColumnValueType): Unit = {}

  override def setAggregate(aggregate: Boolean): Unit = {}

  override def setTimestamp(timestamp: Long): Unit = {}

  override def build(): Any = null

  private def addValue(value: => Any): Unit = {
    if (_index < _values.length) {
      _values(_index) = value
    }
  }

  override def onEntity(): Unit = addValue(nulls(_index))

  override def onInteger(value: Long): Unit = addValue(value)

  override def onBoolean(value: Boolean): Unit = addValue(value)

  override def onDouble(value: Double): Unit = addValue(value)

  private def collectArray(nodes: ListF[YTreeNode])(f: YTreeNode => Any): Array[Any] = {
    val array = new Array[Any](nodes.length())
    var i = 0
    nodes.forEach((node: YTreeNode) => {
      array(i) = f(node)
      i += 1
    })
    array
  }

  private def collectMap(nodes: MapF[String, YTreeNode])(f: YTreeNode => Any): Map[String, Any] = {
    val map = new mutable.ListMap[String, Any].empty
    nodes.forEach((key: String, value: YTreeNode) => map += key -> f(value))
    map.toMap
  }

  override def onBytes(bytes: Array[Byte]): Unit = {
    addValue {
      indexedSchema(_index) match {
        case BinaryType => bytes
        case StringType => new String(bytes, StandardCharsets.UTF_8)
        case ArrayType(elementType, _) =>
          val input = new ByteArrayInputStream(bytes.dropWhile(_ != YsonTags.BEGIN_LIST))
          val deserialized = YTreeTextSerializer.deserialize(input).asList()
          collectArray(deserialized)(YsonRowConverter.deserializeValue(_, elementType))
        case StructType(_) =>
          val input = new ByteArrayInputStream(bytes.dropWhile(_ != YsonTags.BEGIN_MAP))
          val deserialized = YTreeTextSerializer.deserialize(input)
          rowSerializers(_index).deserialize(deserialized)
        case MapType(StringType, valueType, _) =>
          val input = new ByteArrayInputStream(bytes.dropWhile(_ != YsonTags.BEGIN_MAP))
          val deserialized = YTreeTextSerializer.deserialize(input).asMap()
          collectMap(deserialized)(YsonRowConverter.deserializeValue(_, valueType))
      }
    }

  }
}

object SparkRowDeserializer {
  private val deserializers: ThreadLocal[mutable.Map[StructType, SparkRowDeserializer]] = ThreadLocal.withInitial(() => mutable.ListMap.empty)

  def getOrCreate(schema: StructType, filters: Array[Filter]): SparkRowDeserializer = {
    deserializers.get().getOrElseUpdate(schema, new SparkRowDeserializer(schema, filters))
  }
}
