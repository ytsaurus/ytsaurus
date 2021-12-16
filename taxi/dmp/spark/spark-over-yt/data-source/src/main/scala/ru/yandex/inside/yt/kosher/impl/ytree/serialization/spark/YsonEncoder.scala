package ru.yandex.inside.yt.kosher.impl.ytree.serialization.spark

import com.google.protobuf.CodedOutputStream
import org.apache.spark.sql.types.DataType
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.YsonTags
import ru.yandex.misc.ExceptionUtils
import ru.yandex.spark.yt.serializers.YsonRowConverter
import ru.yandex.yson.YsonConsumer

import java.io.ByteArrayOutputStream

class YsonEncoder(stream: ByteArrayOutputStream) extends YsonConsumer {
  private val output = CodedOutputStream.newInstance(stream, 8192)
  private var firstItem = false

  private def writeBytes(bytes: Array[Byte], offset: Int, length: Int): Unit = translateException {
    output.writeSInt32NoTag(bytes.length)
    output.writeRawBytes(bytes, offset, length)
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

  def onBoolean(value: Boolean): Unit = translateException {
    output.writeRawByte(if (value) YsonTags.BINARY_TRUE else YsonTags.BINARY_FALSE)
  }

  def onDouble(value: Double): Unit = translateException {
    output.writeRawByte(YsonTags.BINARY_DOUBLE)
    output.writeDoubleNoTag(value)
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

  // TODO: support uint64 in complex fields
  override def onUnsignedInteger(l: Long): Unit = ???

  override def onString(bytes: Array[Byte], i: Int, i1: Int): Unit = {
    output.writeRawByte(YsonTags.BINARY_STRING)
    writeBytes(bytes, i, i1)
  }

  override def onKeyedItem(bytes: Array[Byte], i: Int, i1: Int): Unit = {
    if (firstItem) firstItem = false
    else output.writeRawByte(YsonTags.ITEM_SEPARATOR)
    output.writeRawByte(YsonTags.BINARY_STRING)
    writeBytes(bytes, i, i1)
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
