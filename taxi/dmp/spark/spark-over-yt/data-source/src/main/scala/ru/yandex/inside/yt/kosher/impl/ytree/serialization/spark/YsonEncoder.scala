package ru.yandex.inside.yt.kosher.impl.ytree.serialization.spark

import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets

import com.google.protobuf.CodedOutputStream
import org.apache.spark.sql.types.DataType
import org.apache.spark.unsafe.types.UTF8String
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.YsonTags
import ru.yandex.misc.ExceptionUtils
import ru.yandex.misc.lang.number.UnsignedLong
import ru.yandex.spark.yt.serializers.YsonRowConverter

class YsonEncoder(stream: ByteArrayOutputStream) {
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
    output.writeRawByte(YsonTags.BINARY_INT)
    output.writeSInt64NoTag(value)
  }

  def onUnsignedInteger(value: UnsignedLong): Unit = translateException {
    output.writeRawByte(YsonTags.BINARY_UINT)
    output.writeUInt64NoTag(value.longValue)
  }

  def onBoolean(value: Boolean): Unit = translateException {
    output.writeRawByte(if (value) YsonTags.BINARY_TRUE else YsonTags.BINARY_FALSE)
  }

  def onDouble(value: Double): Unit = translateException {
    output.writeRawByte(YsonTags.BINARY_DOUBLE)
    output.writeDoubleNoTag(value)
  }

  def onBytes(bytes: Array[Byte]): Unit = translateException {
    output.writeRawByte(YsonTags.BINARY_STRING)
    writeBytes(bytes)
  }

  def onString(value: UTF8String): Unit = translateException {
    output.writeRawByte(YsonTags.BINARY_STRING)
    writeString(value)
  }

  def onString(value: String): Unit = translateException {
    output.writeRawByte(YsonTags.BINARY_STRING)
    writeString(value)
  }

  def onEntity(): Unit = translateException {
    output.writeRawByte(YsonTags.ENTITY)
  }

  def onListItem(): Unit = translateException {
    if (firstItem) firstItem = false
    else output.writeRawByte(YsonTags.ITEM_SEPARATOR)
  }

  def onBeginList(): Unit = translateException {
    firstItem = true
    output.writeRawByte(YsonTags.BEGIN_LIST)
  }

  def onEndList(): Unit = translateException {
    firstItem = false
    output.writeRawByte(YsonTags.END_LIST)
  }

  def onBeginAttributes(): Unit = translateException {
    firstItem = true
    output.writeRawByte(YsonTags.BEGIN_ATTRIBUTES)
  }

  def onEndAttributes(): Unit = translateException {
    firstItem = false
    output.writeRawByte(YsonTags.END_ATTRIBUTES)
  }

  def onBeginMap(): Unit = translateException {
    firstItem = true
    output.writeRawByte(YsonTags.BEGIN_MAP)
  }

  def onEndMap(): Unit = translateException {
    firstItem = false
    output.writeRawByte(YsonTags.END_MAP)
  }

  def onKeyedItem(key: UTF8String): Unit = translateException {
    if (firstItem) firstItem = false
    else output.writeRawByte(YsonTags.ITEM_SEPARATOR)
    output.writeRawByte(YsonTags.BINARY_STRING)
    writeString(key)
    output.writeRawByte(YsonTags.KEY_VALUE_SEPARATOR)
  }

  def onKeyedItem(key: String): Unit = translateException {
    if (firstItem) firstItem = false
    else output.writeRawByte(YsonTags.ITEM_SEPARATOR)
    output.writeRawByte(YsonTags.BINARY_STRING)
    writeString(key)
    output.writeRawByte(YsonTags.KEY_VALUE_SEPARATOR)
  }

  def close(): Unit = translateException {
    output.flush()
  }
}


object YsonEncoder {
  def encode(value: Any, dataType: DataType, skipNulls: Boolean): Array[Byte] = {
    val output = new ByteArrayOutputStream(200)
    val writer = new YsonEncoder(output)
    write(value, dataType, skipNulls, writer)
    writer.close()
    output.toByteArray
  }

  private def write(value: Any, dataType: DataType, skipNulls: Boolean, writer: YsonEncoder): Unit = {
    YsonRowConverter.serializeValue(value, dataType, skipNulls, writer)
  }
}
