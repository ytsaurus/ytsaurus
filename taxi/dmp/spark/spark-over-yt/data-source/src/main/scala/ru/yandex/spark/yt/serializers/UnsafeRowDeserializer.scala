package ru.yandex.spark.yt.serializers

import org.apache.log4j.Logger
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.YsonDecoderUnsafe
import ru.yandex.yt.ytclient.`object`.{WireRowDeserializer, WireValueDeserializer}
import ru.yandex.yt.ytclient.tables.ColumnValueType
import ru.yandex.spark.yt._

import scala.collection.mutable

class UnsafeRowDeserializer(schema: StructType) extends WireRowDeserializer[UnsafeRow] with WireValueDeserializer[Any] {
  private val log = Logger.getLogger(getClass)

  private val indexedSchema = schema.fields.map(_.dataType).toIndexedSeq
  private val indexedDataTypes = schema.fields.map(f => SchemaConverter.indexedDataType(f.dataType))
  private var writer = new UnsafeRowWriter(schema.fields.length)

  private var bufferSize: Option[Int] = None
  private val bufferSizeStat = new Array[Int](10)
  private var bufferSizeIndex = 0

  private var _index = 0

  override def onNewRow(columnCount: Int): WireValueDeserializer[_] = {
    if (bufferSize.isEmpty && bufferSizeIndex == 10) {
      bufferSize = Some(bufferSizeStat.sum / 10)
      log.debugLazy(s"Calculate buffer size: $bufferSize from stats: ${bufferSizeStat.mkString(", ")}")
    }
    writer = bufferSize.map(new UnsafeRowWriter(schema.fields.length, _))
      .getOrElse(new UnsafeRowWriter(schema.fields.length))
    writer.reset()
    this
  }

  override def onCompleteRow(): UnsafeRow = {
    if (bufferSizeIndex < 10) {
      bufferSizeStat(bufferSizeIndex) = writer.getBuffer.length
      bufferSizeIndex += 1
    }
    writer.getRow
  }

  override def setId(id: Int): Unit = {
    _index = id
  }

  override def setType(`type`: ColumnValueType): Unit = {}

  override def setAggregate(aggregate: Boolean): Unit = {}

  override def setTimestamp(timestamp: Long): Unit = {}

  override def build(): Any = null

  private def addValue(f: => Unit): Unit = {
    if (_index < schema.length) {
      f
    }
  }

  override def onEntity(): Unit = {
    indexedSchema(_index) match {
      case BooleanType | ByteType => writer.setNull1Bytes(_index)
      case ShortType => writer.setNull2Bytes(_index)
      case IntegerType | DateType | FloatType => writer.setNull4Bytes(_index)
      case _ => writer.setNull8Bytes(_index)
    }
  }

  override def onInteger(value: Long): Unit = addValue(writer.write(_index, value))

  override def onBoolean(value: Boolean): Unit = addValue(writer.write(_index, value))

  override def onDouble(value: Double): Unit = addValue(writer.write(_index, value))

  override def onBytes(bytes: Array[Byte]): Unit = {
    indexedSchema(_index) match {
      case BinaryType => addValue(writer.write(_index, bytes))
      case StringType => addValue(writer.write(_index, bytes))
      case _ @ (ArrayType(_, _) | StructType(_) | MapType(_, _, _)) =>
        YsonDecoderUnsafe.decode(bytes, indexedDataTypes(_index), writer, _index)
    }
  }
}

object UnsafeRowDeserializer {
  private val deserializers: ThreadLocal[mutable.Map[StructType, UnsafeRowDeserializer]] = ThreadLocal.withInitial(() => mutable.ListMap.empty)

  def getOrCreate(schema: StructType, filters: Array[Filter] = Array.empty): UnsafeRowDeserializer = {
    deserializers.get().getOrElseUpdate(schema, new UnsafeRowDeserializer(schema))
  }
}
