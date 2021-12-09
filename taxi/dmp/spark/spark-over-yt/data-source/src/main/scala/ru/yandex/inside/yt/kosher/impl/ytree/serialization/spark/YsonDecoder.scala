package ru.yandex.inside.yt.kosher.impl.ytree.serialization.spark

import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import ru.yandex.inside.yt.kosher.impl.ytree.serialization._
import ru.yandex.misc.lang.number.UnsignedLong
import ru.yandex.yson.YsonError

import scala.annotation.tailrec

class YsonDecoder(bytes: Array[Byte], dataType: IndexedDataType) extends YsonBaseReader
  with MapParser with ListParser with VariantParser {

  val input = new YsonByteReader(bytes)

  @tailrec
  override final def parseToken(allowEof: Boolean): Byte = {
    if (!input.isAtEnd) {
      val b = parseByte()
      if (!Character.isWhitespace(b & 0xff)) {
        b
      } else {
        parseToken(allowEof)
      }
    } else if (!allowEof) {
      throw new YsonError("Unexpected EOF")
    } else {
      YsonTags.BINARY_END
    }
  }

  private def parseByte(): Byte = {
    if (input.isAtEnd) {
      throw new YsonError("Unexpected EOF")
    } else {
      input.readRawByte
    }
  }

  // primitives

  def parseVarInt64: Long = {
    input.readSInt64
  }

  def parseInt64AsBytes: Array[Byte] = {
    input.readRawVarint64AsBytes
  }

  def parseUInt64: Long = {
    input.readRawVarint64
  }

  def parseVarInt32: Int = {
    input.readSInt32
  }

  def parseRawBytes(size: Int): Array[Byte] = {
    input.readRawBytes(size)
  }

  def parseBinary: Array[Byte] = {
    val size = parseVarInt32
    if (size < 0) throw new YsonError("Negative binary string length")
    parseRawBytes(size)
  }

  def parseDouble: Double = {
    input.readDouble
  }

  // binary

  def parseYsonListAsSparkBinary(allowEof: Boolean): Array[Byte] = {
    val pos = input.position
    parseYsonListAsNone(allowEof)
    bytes.slice(pos, input.position)
  }

  def parseYsonMapAsSparkBinary(endToken: Byte, allowEof: Boolean): Array[Byte] = {
    val pos = input.position
    parseYsonMapAsNone(endToken, allowEof)
    bytes.slice(pos, input.position)
  }

  // skip

  @tailrec
  final def skip(token: Byte, allowEof: Boolean, skipTo: Seq[Byte]): YsonDecoder = {
    token match {
      case b if b != skipTo.head =>
        closeToken(b) match {
          case Some(innerSkipTo) =>
            skip(parseToken(allowEof), allowEof, innerSkipTo +: skipTo)
          case None =>
            skip(parseToken(allowEof), allowEof, skipTo)
        }

      case _ if skipTo.lengthCompare(1) > 0 =>
        skip(parseToken(allowEof), allowEof, skipTo.tail)
      case _ => this
    }
  }

  def skip(token: Byte, allowEof: Boolean, skipTo: Byte): Unit = {
    skip(token, allowEof, Seq(skipTo))
  }

  def closeToken(token: Byte): Option[Byte] = {
    token match {
      case YsonTags.BEGIN_MAP => Some(YsonTags.END_MAP)
      case YsonTags.BEGIN_LIST => Some(YsonTags.END_LIST)
      case _ => None
    }
  }

  // parse node

  override def parseNode(first: Byte, allowEof: Boolean, dataType: IndexedDataType): Any = {
    val newFirst = if (first == YsonTags.BEGIN_ATTRIBUTES) {
      parseYsonMapAsNone(YsonTags.END_ATTRIBUTES, allowEof)
      parseToken(allowEof)
    } else first
    parseWithoutAttributes(newFirst, allowEof, dataType)
  }

  def parseNode(): Any = {
    parseNode(parseToken(allowEof = false), allowEof = true, dataType)
  }

  def parseWithoutAttributes(first: Byte, allowEof: Boolean, dataType: IndexedDataType): Any = {
    first match {
      case YsonTags.BINARY_STRING =>
        val s = parseBinary
        dataType match {
          case IndexedDataType.ScalaStringType => new String(s)
          case IndexedDataType.AtomicType(BinaryType) => s
          case _ => UTF8String.fromBytes(s)
        }
      case YsonTags.BINARY_INT =>
        dataType.sparkDataType match {
          case IntegerType => parseVarInt64.toInt
          case BinaryType => first +: parseInt64AsBytes
          case _ => parseVarInt64
        }
      case YsonTags.BINARY_UINT =>
        dataType.sparkDataType match {
          case LongType => parseUInt64
          case IntegerType => parseUInt64.toInt
          case DoubleType => UnsignedLong.valueOf(parseUInt64).doubleValue()
          case BinaryType => first +: parseInt64AsBytes
          case NullType =>
            parseUInt64
            null
        }
      case YsonTags.BINARY_DOUBLE =>
        dataType.sparkDataType match {
          case BinaryType => first +: parseRawBytes(8)
          case _ => parseDouble
        }
      case YsonTags.BINARY_FALSE =>
        dataType.sparkDataType match {
          case BinaryType => Array[Byte](YsonTags.BINARY_FALSE)
          case _ => false
        }
      case YsonTags.BINARY_TRUE =>
        dataType.sparkDataType match {
          case BinaryType => Array[Byte](YsonTags.BINARY_TRUE)
          case _ => true
        }
      case YsonTags.BEGIN_LIST =>
        dataType match {
          case IndexedDataType.ArrayType(elementType, _) => parseYsonListAsSparkList(allowEof = false, elementType)
          case map: IndexedDataType.MapType => parseYsonListAsSparkMap(allowEof = false, map)
          case struct: IndexedDataType.StructType => parseYsonListAsSparkStruct(allowEof = false, struct)
          case tuple: IndexedDataType.TupleType => parseYsonListAsSparkTuple(allowEof = false, tuple)
          case IndexedDataType.AtomicType(BinaryType) => first +: parseYsonListAsSparkBinary(allowEof = false)
          case IndexedDataType.NoneType => parseYsonListAsNone(allowEof = false)
          case variant: IndexedDataType.VariantOverTupleType => parseYsonVariantAsSparkTuple(allowEof = false, variant)
          case variant: IndexedDataType.VariantOverStructType => parseYsonVariantAsSparkStruct(allowEof = false, variant)
          case _ => throw new IllegalArgumentException(s"Unsupported data type: $dataType for yson list")
        }

      case YsonTags.BEGIN_MAP =>
        dataType match {
          case struct: IndexedDataType.StructType => parseYsonMapAsSparkStruct(allowEof = false, struct)
          case map: IndexedDataType.MapType => parseYsonMapAsSparkMap(allowEof = false, map)
          case IndexedDataType.AtomicType(BinaryType) => first +: parseYsonMapAsSparkBinary(YsonTags.END_MAP, allowEof = false)
          case IndexedDataType.NoneType => parseYsonMapAsNone(YsonTags.END_MAP, allowEof = false)
          case _ => throw new IllegalArgumentException(s"Unsupported data type: $dataType for yson map")
        }
      case YsonTags.ENTITY =>
        dataType.sparkDataType match {
          case BinaryType => Array[Byte](YsonTags.ENTITY)
          case _ => null
        }
      //      case '"' =>
      //        consumer.onBytes(readQuotedString)
      //      case _ =>
      //        if (YsonFormatUtil.isUnquotedStringFirstByte(first)) consumer.onBytes(readUnquotedString(first, allowEof))
      //        else if (YsonFormatUtil.isNumericFirstByte(first)) parseNumeric(first, allowEof)
      //        else if (first == '%') consumer.onBoolean(readBooleanValue)
      //        else throw new YsonUnexpectedToken(first, "NODE")
    }
  }
}

object YsonDecoder {
  def decode(bytes: Array[Byte], dataType: IndexedDataType): Any = {
    val decoder = new YsonDecoder(bytes, dataType)
    decoder.parseNode()
  }
}
