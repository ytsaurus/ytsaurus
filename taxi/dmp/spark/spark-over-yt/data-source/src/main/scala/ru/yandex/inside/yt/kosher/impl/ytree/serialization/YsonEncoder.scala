package ru.yandex.inside.yt.kosher.impl.ytree.serialization

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets

import com.google.protobuf.CodedOutputStream
import org.apache.spark.sql.types.DataType
import org.apache.spark.unsafe.types.UTF8String
import ru.yandex.misc.ExceptionUtils
import ru.yandex.misc.lang.number.UnsignedLong
import ru.yandex.spark.yt.serializers.YsonRowConverter

class YsonEncoder(stream: ByteArrayOutputStream) {

  import YsonEncoder._

  private val output = CodedOutputStream.newInstance(stream, 8192)
  private var firstItem = false

  private def writeBytes(bytes: Array[Byte]): Unit = translateException {
    output.writeSInt32NoTag(bytes.length)
    output.writeRawBytes(bytes)
  }

  private def writeString(s: UTF8String): Unit = translateException {
    writeBytes(s.getBytes)
  }

  private def writeString(s: String): Unit = translateException {
    writeBytes(s.getBytes(StandardCharsets.UTF_8))
  }

  private def translateException(f: => Unit): Unit = {
    try {
      f
    } catch {
      case e: Exception => throw ExceptionUtils.translate(e)
    }
  }

  def onInteger(value: Long): Unit = translateException {
    output.writeRawByte(INTEGER)
    output.writeSInt64NoTag(value)
  }

  def onUnsignedInteger(value: UnsignedLong): Unit = translateException {
    output.writeRawByte(UNSIGNED_INTEGER)
    output.writeUInt64NoTag(value.longValue)
  }

  def onBoolean(value: Boolean): Unit = translateException {
    output.writeRawByte(if (value) TRUE else FALSE)
  }

  def onDouble(value: Double): Unit = translateException {
    output.writeRawByte(DOUBLE)
    output.writeDoubleNoTag(value)
  }

  def onBytes(bytes: Array[Byte]): Unit = translateException {
    output.writeRawByte(STRING)
    writeBytes(bytes)
  }

  def onString(value: UTF8String): Unit = translateException {
    output.writeRawByte(STRING)
    writeString(value)
  }

  def onEntity(): Unit = translateException {
    output.writeRawByte(YTreeSerializationUtils.ENTITY)
  }

  def onListItem(): Unit = translateException {
    if (firstItem) firstItem = false
    else output.writeRawByte(YTreeSerializationUtils.ITEM_SEPARATOR)
  }

  def onBeginList(): Unit = translateException {
    firstItem = true
    output.writeRawByte(YTreeSerializationUtils.BEGIN_LIST)
  }

  def onEndList(): Unit = translateException {
    firstItem = false
    output.writeRawByte(YTreeSerializationUtils.END_LIST)
  }

  def onBeginAttributes(): Unit = translateException {
    firstItem = true
    output.writeRawByte(YTreeSerializationUtils.BEGIN_ATTRIBUTES)
  }

  def onEndAttributes(): Unit = translateException {
    firstItem = false
    output.writeRawByte(YTreeSerializationUtils.END_ATTRIBUTES)
  }

  def onBeginMap(): Unit = translateException {
    firstItem = true
    output.writeRawByte(YTreeSerializationUtils.BEGIN_MAP)
  }

  def onEndMap(): Unit = translateException {
    firstItem = false
    output.writeRawByte(YTreeSerializationUtils.END_MAP)
  }

  def onKeyedItem(key: UTF8String): Unit = translateException {
    if (firstItem) firstItem = false
    else output.writeRawByte(YTreeSerializationUtils.ITEM_SEPARATOR)
    output.writeRawByte(STRING)
    writeString(key)
    output.writeRawByte(YTreeSerializationUtils.KEY_VALUE_SEPARATOR)
  }

  def onKeyedItem(key: String): Unit = translateException {
    if (firstItem) firstItem = false
    else output.writeRawByte(YTreeSerializationUtils.ITEM_SEPARATOR)
    output.writeRawByte(STRING)
    writeString(key)
    output.writeRawByte(YTreeSerializationUtils.KEY_VALUE_SEPARATOR)
  }

  def close(): Unit = translateException {
    output.flush()
  }
}


object YsonEncoder {
  private val STRING = 0x1
  private val INTEGER = 0x2
  private val DOUBLE = 0x3
  private val FALSE = 0x4
  private val TRUE = 0x5
  private val UNSIGNED_INTEGER = 0x6

  def encode(value: Any, dataType: DataType): Array[Byte] = {
    val output = new ByteArrayOutputStream(200)
    val writer = new YsonEncoder(output)
    write(value, dataType, writer)
    writer.close()
    output.toByteArray
  }

  private def write(value: Any, dataType: DataType, writer: YsonEncoder): Unit = {
    YsonRowConverter.serializeValue(value, dataType, writer)
  }
}
