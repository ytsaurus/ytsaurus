package ru.yandex.inside.yt.kosher.impl.ytree.serialization

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{UnsafeArrayWriter, UnsafeRowWriter, UnsafeWriter}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.UTF8String
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.IndexedDataType.{AtomicType, StructFieldMeta}

import scala.annotation.tailrec
import scala.collection.mutable

class YsonDecoderUnsafe(bytes: Array[Byte]) {
  val input = new YsonByteReader(bytes)

  def readToken(allowEof: Boolean): Byte = {
    if (!input.isAtEnd) {
      val b = readByte()
      if (!Character.isWhitespace(b & 0xff)) {
        b
      } else {
        readToken(allowEof)
      }
    } else if (!allowEof) {
      throw new YsonUnexpectedEOF
    } else {
      YsonTags.BINARY_END
    }
  }

  def readByte(): Byte = {
    if (input.isAtEnd) {
      throw new YsonUnexpectedEOF
    } else {
      input.readRawByte
    }
  }

  private def getElementSize(dataType: DataType): Int = dataType match {
    case NullType | StringType | BinaryType | CalendarIntervalType |
         _: DecimalType | _: StructType | _: ArrayType | _: MapType => 8
    case _ => dataType.defaultSize
  }

  private def getElementSize(dataType: IndexedDataType): Int = dataType match {
    case IndexedDataType.AtomicType(dt) => getElementSize(dt)
    case _ => 8
  }

  def writeNull(w: UnsafeWriter, ordinal: Int, dataType: IndexedDataType): Unit = {
    dataType match {
      case IndexedDataType.AtomicType(dt) => dt match {
        case BooleanType | ByteType => w.setNull1Bytes(ordinal)
        case ShortType => w.setNull2Bytes(ordinal)
        case IntegerType | DateType | FloatType => w.setNull4Bytes(ordinal)
        case _ => w.setNull8Bytes(ordinal)
      }
      case _ => w.setNull8Bytes(ordinal)
    }
  }

  def writeRawValue(w: UnsafeWriter, ordinal: Int,
                    value: Any, dataType: IndexedDataType): Unit = {
    if (value == null) {
      writeNull(w, ordinal, dataType)
    } else {
      dataType match {
        case IndexedDataType.AtomicType(dt) => dt match {
          case BooleanType => w.write(ordinal, value.asInstanceOf[Boolean])
          case ByteType => w.write(ordinal, value.asInstanceOf[Byte])
          case ShortType => w.write(ordinal, value.asInstanceOf[Short])
          case IntegerType => w.write(ordinal, value.asInstanceOf[Int])
          case LongType => w.write(ordinal, value.asInstanceOf[Long])
          case FloatType => w.write(ordinal, value.asInstanceOf[Float])
          case DoubleType => w.write(ordinal, value.asInstanceOf[Double])
          case BinaryType => w.write(ordinal, value.asInstanceOf[Array[Byte]])
          case StringType => w.write(ordinal, value.asInstanceOf[UTF8String])
        }
        case IndexedDataType.ArrayType(elementType, _) =>
          val arrayValue = value.asInstanceOf[ArrayData]
          writeList(w, ordinal, arrayValue.array, elementType)
        case s: IndexedDataType.StructType =>
          val structValue = value.asInstanceOf[InternalRow]
          writeRow(w, ordinal, structValue, s)
        case m: IndexedDataType.MapType =>
          val mapValue = value.asInstanceOf[ArrayBasedMapData]
          writeMap(w, ordinal, mapValue.keyArray.array, mapValue.valueArray.array, m)
        case _ => throw new IllegalArgumentException(s"Unexpected dataType $dataType when parsing array")
      }
    }
  }

  private def writeList(w: UnsafeWriter, ordinal: Int,
                        list: Seq[Any],
                        elementType: IndexedDataType): Unit = {
    val elementSize = getElementSize(elementType)
    val arrayWriter = new UnsafeArrayWriter(w, elementSize)
    val previousCursor = w.cursor()
    writeList(arrayWriter, list, elementType)
    w.setOffsetAndSizeFromPreviousCursor(ordinal, previousCursor)
  }

  private def writeList(arrayWriter: UnsafeArrayWriter,
                        list: Seq[Any],
                        elementType: IndexedDataType): Unit = {

    arrayWriter.initialize(list.length)

    list.iterator.zipWithIndex.foreach { case (element, index) =>
      writeRawValue(arrayWriter, index, element, elementType)
    }
  }

  def parseList(w: UnsafeWriter, ordinal: Int,
                endToken: Byte, allowEof: Boolean, elementType: IndexedDataType): Unit = {
    val list = YsonDecoderCommon.parseListRaw(input, endToken, allowEof, elementType)
    writeList(w, ordinal, list, elementType)
  }

  def parseNoneList(endToken: Byte, allowEof: Boolean): Unit = {
    @tailrec
    def read(requireSeparator: Boolean = false): Unit = {
      val token = readToken(allowEof)
      token match {
        case t if t == endToken =>
        case YsonTags.ITEM_SEPARATOR => read()
        case _ =>
          if (requireSeparator) throw new YsonUnexpectedToken(token, "ITEM_SEPARATOR")
          parseNode(None, None, token, allowEof, IndexedDataType.NoneType)
          read(requireSeparator = true)
      }
    }

    read()
  }

  def readVarInt64(w: Option[UnsafeWriter], ordinal: Option[Int]): Unit = {
    val res = input.readSInt64
    w.foreach(_.write(ordinal.get, res))
  }

  def writeRow(w: UnsafeWriter, ordinal: Int,
               row: InternalRow, schema: IndexedDataType.StructType): Unit = {
    val rowWriter = new UnsafeRowWriter(w, schema.sparkDataType.fields.length)
    val previousCursor = w.cursor()
    rowWriter.resetRowWriter()

    schema.map.foreach { case (_, StructFieldMeta(index, dataType, _)) =>
      writeRawValue(rowWriter, index, row.get(index, dataType.sparkDataType), dataType)
    }

    w.setOffsetAndSizeFromPreviousCursor(ordinal, previousCursor)
  }

  def parseStruct(w: UnsafeWriter, ordinal: Int,
                  endToken: Byte, allowEof: Boolean,
                  schema: IndexedDataType.StructType): Unit = {
    val numFields = schema.sparkDataType.fields.length
    val rowWriter = new UnsafeRowWriter(w, numFields)
    val previousCursor = w.cursor()
    rowWriter.resetRowWriter()

    @tailrec
    def read(key: Option[String] = None,
             requireKeyValueSeparator: Boolean = false,
             requireItemSeparator: Boolean = false): Unit = {
      val token = readToken(allowEof)
      token match {
        case t if t == endToken =>
        case YsonTags.KEY_VALUE_SEPARATOR =>
          if (requireItemSeparator) throw new YsonUnexpectedToken(YsonTags.KEY_VALUE_SEPARATOR, "ITEM_SEPARATOR")
          read(key, requireKeyValueSeparator = false, requireItemSeparator)
        case YsonTags.ITEM_SEPARATOR =>
          if (requireKeyValueSeparator) throw new YsonUnexpectedToken(YsonTags.ITEM_SEPARATOR, "KEY_VALUE_SEPARATOR")
          read(key, requireKeyValueSeparator, requireItemSeparator = false)
        case token =>
          if (requireItemSeparator) throw new YsonUnexpectedToken(token, "ITEM_SEPARATOR")
          if (requireKeyValueSeparator) throw new YsonUnexpectedToken(token, "KEY_VALUE_SEPARATOR")
          key match {
            case Some(k) =>
              val field = schema.map.get(k)
              field match {
                case Some(meta) =>
                  parseNode(Some(rowWriter), Some(meta.index), token, allowEof, meta.dataType)
                  meta.setNotNull()
                  read(None, requireItemSeparator = true)
                case None =>
                  parseNode(None, None, token, allowEof, IndexedDataType.NoneType)
                  read(None, requireItemSeparator = false)
              }

            case None =>
              val k = YsonDecoderCommon.parseNode(input, token, allowEof, IndexedDataType.ScalaStringType)
              read(Some(k.asInstanceOf[String]), requireKeyValueSeparator = true)
          }
      }
    }

    read()
    schema.map.foreach {
      case (_, meta) if meta.isNull => rowWriter.setNullAt(meta.index)
      case (_, meta) => meta.setNull()
    }
    w.setOffsetAndSizeFromPreviousCursor(ordinal, previousCursor)
  }

  def closeToken(token: Byte): Option[Byte] = {
    token match {
      case YsonTags.BEGIN_MAP => Some(YsonTags.END_MAP)
      case YsonTags.BEGIN_LIST => Some(YsonTags.END_LIST)
      case _ => None
    }
  }

  def parseMap(w: UnsafeWriter, ordinal: Int,
               endToken: Byte, allowEof: Boolean,
               schema: IndexedDataType.MapType): Unit = {
    val (keyList, valueList) = YsonDecoderCommon.parseMapRaw(input, endToken, allowEof, schema)
    writeMap(w, ordinal, keyList, valueList, schema)
  }

  def writeMap(w: UnsafeWriter, ordinal: Int,
               keyList: Seq[Any],
               valueList: Seq[Any],
               schema: IndexedDataType.MapType): Unit = {
    val keyArrayWriter = new UnsafeArrayWriter(w, getElementSize(StringType))
    val valueArrayWriter = new UnsafeArrayWriter(w, getElementSize(schema.valueType))
    val previousCursor = w.cursor()

    // preserve 8 bytes to write the key array numBytes later.
    valueArrayWriter.grow(8)
    valueArrayWriter.increaseCursor(8)

    // Write the keys and write the numBytes of key array into the first 8 bytes.
    writeList(keyArrayWriter, keyList, AtomicType(StringType))
    Platform.putLong(
      valueArrayWriter.getBuffer,
      previousCursor,
      valueArrayWriter.cursor - previousCursor - 8
    )

    // Write the values.
    writeList(valueArrayWriter, valueList, schema.valueType)
    w.setOffsetAndSizeFromPreviousCursor(ordinal, previousCursor)
  }


  def parseNoneMap(endToken: Byte, allowEof: Boolean): Unit = {
    @tailrec
    def read(key: Option[Int] = None,
             requireKeyValueSeparator: Boolean = false,
             requireItemSeparator: Boolean = false): Unit = {
      val token = readToken(allowEof)
      token match {
        case t if t == endToken =>
        case YsonTags.KEY_VALUE_SEPARATOR =>
          if (requireItemSeparator) throw new YsonUnexpectedToken(YsonTags.KEY_VALUE_SEPARATOR, "ITEM_SEPARATOR")
          read(key, requireKeyValueSeparator = false, requireItemSeparator)
        case YsonTags.ITEM_SEPARATOR =>
          if (requireKeyValueSeparator) throw new YsonUnexpectedToken(YsonTags.ITEM_SEPARATOR, "KEY_VALUE_SEPARATOR")
          read(key, requireKeyValueSeparator, requireItemSeparator = false)
        case token =>
          if (requireItemSeparator) throw new YsonUnexpectedToken(token, "ITEM_SEPARATOR")
          if (requireKeyValueSeparator) throw new YsonUnexpectedToken(token, "KEY_VALUE_SEPARATOR")
          parseNode(None, None, token, allowEof, IndexedDataType.NoneType)
          key match {
            case Some(_) => read(None, requireItemSeparator = true)
            case None => read(Some(1), requireKeyValueSeparator = true)
          }
      }
    }

    read()
  }

  def readVarInt32: Int = {
    input.readSInt32
  }

  def readRawBytes(size: Int): Array[Byte] = {
    input.readRawBytes(size)
  }

  def readBinaryString(w: Option[UnsafeWriter], ordinal: Option[Int]): Unit = {
    val size = readVarInt32
    if (size < 0) throw new YsonFormatException("Negative binary string length")
    val bytes = readRawBytes(size)
    w.foreach(_.write(ordinal.get, bytes))
  }

  def readDouble(w: Option[UnsafeWriter], ordinal: Option[Int]): Unit = {
    val res = input.readDouble
    w.foreach(_.write(ordinal.get, res))
  }

  def parseWithoutAttributes(w: Option[UnsafeWriter], ordinal: Option[Int],
                             first: Byte, allowEof: Boolean, dataType: IndexedDataType): Unit = {
    first match {
      case YsonTags.BINARY_STRING => readBinaryString(w, ordinal)
      case YsonTags.BINARY_INT => readVarInt64(w, ordinal)
      case YsonTags.BINARY_DOUBLE => readDouble(w, ordinal)
      case YsonTags.BINARY_FALSE => w.foreach(_.write(ordinal.get, false))
      case YsonTags.BINARY_TRUE => w.foreach(_.write(ordinal.get, true))
      case YsonTags.BEGIN_LIST =>
        dataType match {
          case IndexedDataType.ArrayType(elementType, _) =>
            parseList(w.get, ordinal.get, YsonTags.END_LIST, allowEof = false, elementType)
          case IndexedDataType.NoneType => parseNoneList(YsonTags.END_LIST, allowEof = false)
          case _ => throw new IllegalArgumentException(s"Unsupported data type: $dataType for yson list")
        }

      case YsonTags.BEGIN_MAP =>
        dataType match {
          case struct: IndexedDataType.StructType => parseStruct(w.get, ordinal.get, YsonTags.END_MAP, allowEof = false, struct)
          case map: IndexedDataType.MapType => parseMap(w.get, ordinal.get, YsonTags.END_MAP, allowEof = false, map)
          case IndexedDataType.NoneType => parseNoneMap(YsonTags.END_MAP, allowEof = false)
          case _ => throw new IllegalArgumentException(s"Unsupported data type: $dataType for yson map")
        }
      case YsonTags.ENTITY =>
        w.foreach(writeNull(_, ordinal.get, dataType))
      //      case '"' =>
      //        consumer.onBytes(readQuotedString)
      //      case _ =>
      //        if (YsonFormatUtil.isUnquotedStringFirstByte(first)) consumer.onBytes(readUnquotedString(first, allowEof))
      //        else if (YsonFormatUtil.isNumericFirstByte(first)) parseNumeric(first, allowEof)
      //        else if (first == '%') consumer.onBoolean(readBooleanValue)
      //        else throw new YsonUnexpectedToken(first, "NODE")
    }
  }

  @tailrec
  final def skip(token: Byte, allowEof: Boolean, skipTo: Seq[Byte]): YsonDecoderUnsafe = {
    token match {
      case b if b != skipTo.head =>
        closeToken(b) match {
          case Some(innerSkipTo) =>
            skip(readToken(allowEof), allowEof, innerSkipTo +: skipTo)
          case None =>
            skip(readToken(allowEof), allowEof, skipTo)
        }

      case _ if skipTo.lengthCompare(1) > 0 =>
        skip(readToken(allowEof), allowEof, skipTo.tail)
      case _ => this
    }
  }

  def skip(token: Byte, allowEof: Boolean, skipTo: Byte): Unit = {
    skip(token, allowEof, Seq(skipTo))
  }

  def parseNode(w: Option[UnsafeWriter], ordinal: Option[Int],
                first: Byte, allowEof: Boolean, dataType: IndexedDataType): Unit = {
    val newFirst = if (first == YsonTags.BEGIN_ATTRIBUTES) {
      skip(readToken(allowEof), allowEof, YsonTags.END_ATTRIBUTES)
      readToken(allowEof)
    } else first
    parseWithoutAttributes(w, ordinal, newFirst, allowEof, dataType)
  }

  def parseNode1(writer: UnsafeWriter, ordinal: Int, dataType: IndexedDataType): Unit = {
    parseNode(Some(writer), Some(ordinal), readToken(allowEof = false), allowEof = true, dataType)
  }
}

object YsonDecoderUnsafe {
  def decode(bytes: Array[Byte], dataType: IndexedDataType, writer: UnsafeRowWriter, ordinal: Int): Unit = {
    val decoder = new YsonDecoderUnsafe(bytes)
    decoder.parseNode1(writer, ordinal, dataType)
  }
}

