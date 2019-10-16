package ru.yandex.inside.yt.kosher.impl.ytree.serialization

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData}
import org.apache.spark.unsafe.types.UTF8String
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.IndexedDataType.StructFieldMeta

import scala.annotation.tailrec

object YsonDecoderCommon {
  @tailrec
  def readToken(input: YsonByteReader, allowEof: Boolean): Byte = {
    if (!input.isAtEnd) {
      val b = readByte(input)
      if (!Character.isWhitespace(b & 0xff)) {
        b
      } else {
        readToken(input, allowEof)
      }
    } else if (!allowEof) {
      throw new YsonUnexpectedEOF
    } else {
      YsonTags.BINARY_END
    }
  }

  def readByte(input: YsonByteReader): Byte = {
    if (input.isAtEnd) {
      throw new YsonUnexpectedEOF
    } else {
      input.readRawByte
    }
  }

  def parseList(input: YsonByteReader, endToken: Byte, allowEof: Boolean, elementType: IndexedDataType): ArrayData = {
    @tailrec
    def read(res: Seq[Any], requireSeparator: Boolean = false): Seq[Any] = {
      val token = readToken(input, allowEof)
      token match {
        case t if t == endToken => res
        case YsonTags.ITEM_SEPARATOR => read(res)
        case _ =>
          if (requireSeparator) throw new YsonUnexpectedToken(token, "ITEM_SEPARATOR")
          val element = parseNode(input, token, allowEof, elementType)
          read(element +: res, requireSeparator = true)
      }
    }

    val res = read(Nil)
    val array = new Array[Any](res.length)
    res.reverseIterator.zipWithIndex.foreach { case (element, index) => array(index) = element }

    ArrayData.toArrayData(array)
  }

  def parseListRaw(input: YsonByteReader, endToken: Byte, allowEof: Boolean, elementType: IndexedDataType): Seq[Any] = {
    @tailrec
    def read(res: Seq[Any], requireSeparator: Boolean = false): Seq[Any] = {
      val token = readToken(input, allowEof)
      token match {
        case t if t == endToken => res
        case YsonTags.ITEM_SEPARATOR => read(res)
        case _ =>
          if (requireSeparator) throw new YsonUnexpectedToken(token, "ITEM_SEPARATOR")
          val element = parseNode(input, token, allowEof, elementType)
          read(element +: res, requireSeparator = true)
      }
    }

    val res = read(Nil)
    res.reverse
  }

  def parseNoneList(input: YsonByteReader, endToken: Byte, allowEof: Boolean): Int = {
    @tailrec
    def read(requireSeparator: Boolean = false): Unit = {
      val token = readToken(input, allowEof)
      token match {
        case t if t == endToken =>
        case YsonTags.ITEM_SEPARATOR => read()
        case _ =>
          if (requireSeparator) throw new YsonUnexpectedToken(token, "ITEM_SEPARATOR")
          parseNode(input, token, allowEof, IndexedDataType.NoneType)
          read(requireSeparator = true)
      }
    }

    read()

    1
  }

  def readVarInt64(input: YsonByteReader): Long = {
    input.readSInt64
  }

  def parseStruct(input: YsonByteReader, endToken: Byte, allowEof: Boolean, schema: IndexedDataType.StructType): InternalRow = {
    @tailrec
    def read(res: Array[Any], key: Option[String] = None,
             requireKeyValueSeparator: Boolean = false,
             requireItemSeparator: Boolean = false): Array[Any] = {
      val token = readToken(input, allowEof)
      token match {
        case t if t == endToken => res
        case YsonTags.KEY_VALUE_SEPARATOR =>
          if (requireItemSeparator) throw new YsonUnexpectedToken(YsonTags.KEY_VALUE_SEPARATOR, "ITEM_SEPARATOR")
          read(res, key, requireKeyValueSeparator = false, requireItemSeparator)
        case YsonTags.ITEM_SEPARATOR =>
          if (requireKeyValueSeparator) throw new YsonUnexpectedToken(YsonTags.ITEM_SEPARATOR, "KEY_VALUE_SEPARATOR")
          read(res, key, requireKeyValueSeparator, requireItemSeparator = false)
        case token =>
          if (requireItemSeparator) throw new YsonUnexpectedToken(token, "ITEM_SEPARATOR")
          if (requireKeyValueSeparator) throw new YsonUnexpectedToken(token, "KEY_VALUE_SEPARATOR")
          key match {
            case Some(k) =>
              val field = schema.map.get(k)
              field match {
                case Some(StructFieldMeta(index, fieldType, _)) =>
                  res(index) = parseNode(input, token, allowEof, fieldType)
                  read(res, None, requireItemSeparator = true)
                case None =>
                  val value = parseNode(input, token, allowEof, IndexedDataType.NoneType)
                  //                  skip(token, allowEof, YsonTags.ITEM_SEPARATOR)
                  read(res, None, requireItemSeparator = false)
              }

            case None =>
              val k = parseNode(input, token, allowEof, IndexedDataType.ScalaStringType)
              read(res, Some(k.asInstanceOf[String]), requireKeyValueSeparator = true)
          }
      }
    }

    new GenericInternalRow(read(new Array[Any](schema.map.size)))
  }

  def closeToken(token: Byte): Option[Byte] = {
    token match {
      case YsonTags.BEGIN_MAP => Some(YsonTags.END_MAP)
      case YsonTags.BEGIN_LIST => Some(YsonTags.END_LIST)
      case _ => None
    }
  }

  def parseMap(input: YsonByteReader, endToken: Byte, allowEof: Boolean, schema: IndexedDataType.MapType): ArrayBasedMapData = {
    @tailrec
    def read(resKeys: Seq[UTF8String], resValues: Seq[Any],
             key: Option[UTF8String] = None,
             requireKeyValueSeparator: Boolean = false,
             requireItemSeparator: Boolean = false): (Seq[UTF8String], Seq[Any]) = {
      val token = readToken(input, allowEof)
      token match {
        case t if t == endToken => (resKeys, resValues)
        case YsonTags.KEY_VALUE_SEPARATOR =>
          if (requireItemSeparator) throw new YsonUnexpectedToken(YsonTags.KEY_VALUE_SEPARATOR, "ITEM_SEPARATOR")
          read(resKeys, resValues, key, requireKeyValueSeparator = false, requireItemSeparator)
        case YsonTags.ITEM_SEPARATOR =>
          if (requireKeyValueSeparator) throw new YsonUnexpectedToken(YsonTags.ITEM_SEPARATOR, "KEY_VALUE_SEPARATOR")
          read(resKeys, resValues, key, requireKeyValueSeparator, requireItemSeparator = false)
        case token =>
          if (requireItemSeparator) throw new YsonUnexpectedToken(token, "ITEM_SEPARATOR")
          if (requireKeyValueSeparator) throw new YsonUnexpectedToken(token, "KEY_VALUE_SEPARATOR")
          key match {
            case Some(k) =>
              val value = parseNode(input, token, allowEof, schema.valueType)
              read(resKeys, value +: resValues, None, requireItemSeparator = true)
            case None =>
              val k = parseNode(input, token, allowEof, IndexedDataType.NoneType).asInstanceOf[UTF8String]
              read(k +: resKeys, resValues, Some(k), requireKeyValueSeparator = true)
          }
      }
    }

    val (keys, values) = read(Nil, Nil)

    new ArrayBasedMapData(ArrayData.toArrayData(keys), ArrayData.toArrayData(values))
  }

  def parseMapRaw(input: YsonByteReader, endToken: Byte, allowEof: Boolean, schema: IndexedDataType.MapType): (Seq[UTF8String], Seq[Any]) = {
    @tailrec
    def read(resKeys: Seq[UTF8String], resValues: Seq[Any],
             key: Option[UTF8String] = None,
             requireKeyValueSeparator: Boolean = false,
             requireItemSeparator: Boolean = false): (Seq[UTF8String], Seq[Any]) = {
      val token = readToken(input, allowEof)
      token match {
        case t if t == endToken => (resKeys, resValues)
        case YsonTags.KEY_VALUE_SEPARATOR =>
          if (requireItemSeparator) throw new YsonUnexpectedToken(YsonTags.KEY_VALUE_SEPARATOR, "ITEM_SEPARATOR")
          read(resKeys, resValues, key, requireKeyValueSeparator = false, requireItemSeparator)
        case YsonTags.ITEM_SEPARATOR =>
          if (requireKeyValueSeparator) throw new YsonUnexpectedToken(YsonTags.ITEM_SEPARATOR, "KEY_VALUE_SEPARATOR")
          read(resKeys, resValues, key, requireKeyValueSeparator, requireItemSeparator = false)
        case token =>
          if (requireItemSeparator) throw new YsonUnexpectedToken(token, "ITEM_SEPARATOR")
          if (requireKeyValueSeparator) throw new YsonUnexpectedToken(token, "KEY_VALUE_SEPARATOR")
          key match {
            case Some(k) =>
              val value = parseNode(input, token, allowEof, schema.valueType)
              read(resKeys, value +: resValues, None, requireItemSeparator = true)
            case None =>
              val k = parseNode(input, token, allowEof, IndexedDataType.NoneType).asInstanceOf[UTF8String]
              read(k +: resKeys, resValues, Some(k), requireKeyValueSeparator = true)
          }
      }
    }

    read(Nil, Nil)
  }


  def parseNoneMap(input: YsonByteReader, endToken: Byte, allowEof: Boolean): Int = {
    @tailrec
    def read(key: Option[Int] = None,
             requireKeyValueSeparator: Boolean = false,
             requireItemSeparator: Boolean = false): Unit = {
      val token = readToken(input, allowEof)
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
          parseNode(input, token, allowEof, IndexedDataType.NoneType)
          key match {
            case Some(_) => read(None, requireItemSeparator = true)
            case None => read(Some(1), requireKeyValueSeparator = true)
          }
      }
    }

    read()

    1
  }

  def readVarInt32(input: YsonByteReader): Int = {
    input.readSInt32
  }

  def readRawBytes(input: YsonByteReader, size: Int): Array[Byte] = {
    input.readRawBytes(size)
  }

  def readBinaryString(input: YsonByteReader): UTF8String = {
    val size = readVarInt32(input)
    if (size < 0) throw new YsonFormatException("Negative binary string length")
    UTF8String.fromBytes(readRawBytes(input, size))
  }

  def readScalaString(input: YsonByteReader): String = {
    val size = readVarInt32(input)
    if (size < 0) throw new YsonFormatException("Negative binary string length")
    new String(readRawBytes(input, size))
  }

  def readDouble(input: YsonByteReader): Double = {
    input.readDouble
  }

  def parseWithoutAttributes(input: YsonByteReader, first: Byte, allowEof: Boolean, dataType: IndexedDataType): Any = {
    first match {
      case YsonTags.BINARY_STRING =>
        dataType match {
          case IndexedDataType.ScalaStringType => readScalaString(input)
          case _ => readBinaryString(input)
        }
      case YsonTags.BINARY_INT => readVarInt64(input)
      //      case YsonTags.BINARY_UINT =>
      //        consumer.onUnsignedInteger(UnsignedLong.valueOf(readVarUint64))
      case YsonTags.BINARY_DOUBLE => readDouble(input)
      case YsonTags.BINARY_FALSE => false
      case YsonTags.BINARY_TRUE => true
      case YsonTags.BEGIN_LIST =>
        dataType match {
          case IndexedDataType.ArrayType(elementType, _) => parseList(input, YsonTags.END_LIST, allowEof = false, elementType)
          case IndexedDataType.NoneType => parseNoneList(input, YsonTags.END_LIST, allowEof = false)
          case _ => throw new IllegalArgumentException(s"Unsupported data type: $dataType for yson list")
        }

      case YsonTags.BEGIN_MAP =>
        dataType match {
          case struct: IndexedDataType.StructType => parseStruct(input, YsonTags.END_MAP, allowEof = false, struct)
          case map: IndexedDataType.MapType => parseMap(input, YsonTags.END_MAP, allowEof = false, map)
          case IndexedDataType.NoneType => parseNoneMap(input, YsonTags.END_MAP, allowEof = false)
          case _ => throw new IllegalArgumentException(s"Unsupported data type: $dataType for yson map")
        }
      case YsonTags.ENTITY => null
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
  final def skip(input: YsonByteReader, token: Byte, allowEof: Boolean, skipTo: Seq[Byte]): Unit = {
    token match {
      case b if b != skipTo.head =>
        closeToken(b) match {
          case Some(innerSkipTo) =>
            skip(input, readToken(input, allowEof), allowEof, innerSkipTo +: skipTo)
          case None =>
            skip(input, readToken(input, allowEof), allowEof, skipTo)
        }

      case _ if skipTo.lengthCompare(1) > 0 =>
        skip(input, readToken(input, allowEof), allowEof, skipTo.tail)
      case _ =>
    }
  }

  def skip(input: YsonByteReader, token: Byte, allowEof: Boolean, skipTo: Byte): Unit = {
    skip(input, token, allowEof, Seq(skipTo))
  }

  def parseNode(input: YsonByteReader, first: Byte, allowEof: Boolean, dataType: IndexedDataType): Any = {
    val newFirst = if (first == YsonTags.BEGIN_ATTRIBUTES) {
      skip(input, readToken(input, allowEof), allowEof, YsonTags.END_ATTRIBUTES)
      readToken(input, allowEof)
    } else first
    parseWithoutAttributes(input, newFirst, allowEof, dataType)
  }
}
