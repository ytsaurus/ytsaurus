package ru.yandex.spark.yt.serializers

import java.io.ByteArrayInputStream

import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import ru.yandex.bolts.collection.ListF
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.{YTreeTextSerializer, YsonTags}
import ru.yandex.inside.yt.kosher.ytree.YTreeNode
import ru.yandex.yt.ytclient.`object`.{WireRowDeserializer, WireValueDeserializer}
import ru.yandex.yt.ytclient.tables.ColumnValueType

import scala.collection.mutable

class ArrayAnyDeserializer(schema: StructType) extends WireRowDeserializer[Array[Any]] with WireValueDeserializer[Any] {
  private var _values: Array[Any] = _
  private val indexedSchema = schema.fields.map(_.dataType).toIndexedSeq

  private var _index = 0

  override def onNewRow(columnCount: Int): WireValueDeserializer[_] = {
    _values = new Array[Any](schema.length)
    _index = 0
    this
  }

  override def onCompleteRow(): Array[Any] = {
    _values
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

  override def onEntity(): Unit = addValue(null)

  override def onInteger(value: Long): Unit = {
    if (_index < _values.length) {
      indexedSchema(_index) match {
        case LongType => addValue(value)
        case IntegerType => addValue(value.toInt)
        case ShortType => addValue(value.toShort)
      }
    }
  }

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

  override def onBytes(bytes: Array[Byte]): Unit = {
    indexedSchema(_index) match {
      case BinaryType => addValue(bytes)
      case StringType => addValue(bytes)
      case ArrayType(elementType, _) =>
        val input = new ByteArrayInputStream(bytes.dropWhile(_ != YsonTags.BEGIN_LIST))
        val deserialized = YTreeTextSerializer.deserialize(input).asList()
        elementType match {
          case StringType => addValue(collectArray(deserialized)(_.stringValue()))
          case LongType => addValue(collectArray(deserialized)(_.longValue()))
        }
    }
  }
}

object ArrayAnyDeserializer {
  private val deserializers: ThreadLocal[mutable.Map[StructType, ArrayAnyDeserializer]] = ThreadLocal.withInitial(() => mutable.ListMap.empty)

  def getOrCreate(schema: StructType, filters: Array[Filter] = Array.empty): ArrayAnyDeserializer = {
    deserializers.get().getOrElseUpdate(schema, new ArrayAnyDeserializer(schema))
  }
}
