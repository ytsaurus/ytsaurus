package ru.yandex.inside.yt.kosher.impl.ytree.serialization

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData}
import org.apache.spark.sql.types
import org.apache.spark.sql.types.{DataType, NullType, StringType}
import org.apache.spark.unsafe.types.UTF8String
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.IndexedDataType.StructFieldMeta

import scala.annotation.tailrec

class YsonDecoder(bytes: Array[Byte], dataType: IndexedDataType) {
  val input = new YsonByteReader(bytes)

  @tailrec
  final def readToken(allowEof: Boolean): Byte = {
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

  def parseList(endToken: Byte, allowEof: Boolean, elementType: IndexedDataType): ArrayData = {
    @tailrec
    def read(res: Seq[Any], requireSeparator: Boolean = false): Seq[Any] = {
      val token = readToken(allowEof)
      token match {
        case t if t == endToken => res
        case YsonTags.ITEM_SEPARATOR => read(res)
        case _ =>
          if (requireSeparator) throw new YsonUnexpectedToken(token, "ITEM_SEPARATOR")
          val element = parseNode(token, allowEof, elementType)
          read(element +: res, requireSeparator = true)
      }
    }

    val res = read(Nil)
    val array = new Array[Any](res.length)
    res.reverseIterator.zipWithIndex.foreach { case (element, index) => array(index) = element }

    ArrayData.toArrayData(array)
  }

  def parseNoneList(endToken: Byte, allowEof: Boolean): Int = {
    @tailrec
    def read(requireSeparator: Boolean = false): Unit = {
      val token = readToken(allowEof)
      token match {
        case t if t == endToken =>
        case YsonTags.ITEM_SEPARATOR => read()
        case _ =>
          if (requireSeparator) throw new YsonUnexpectedToken(token, "ITEM_SEPARATOR")
          parseNode(token, allowEof, IndexedDataType.NoneType)
          read(requireSeparator = true)
      }
    }

    read()

    1
  }

  def readVarInt64: Long = {
    input.readSInt64
  }

  def parseStruct(endToken: Byte, allowEof: Boolean, schema: IndexedDataType.StructType): InternalRow = {
    @tailrec
    def read(res: Array[Any], key: Option[String] = None,
             requireKeyValueSeparator: Boolean = false,
             requireItemSeparator: Boolean = false): Array[Any] = {
      val token = readToken(allowEof)
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
                  res(index) = parseNode(token, allowEof, fieldType)
                  read(res, None, requireItemSeparator = true)
                case None =>
                  val value = parseNode(token, allowEof, IndexedDataType.NoneType)
                  //                  skip(token, allowEof, YsonTags.ITEM_SEPARATOR)
                  read(res, None, requireItemSeparator = false)
              }

            case None =>
              val k = parseNode(token, allowEof, IndexedDataType.ScalaStringType)
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

  def parseMap(endToken: Byte, allowEof: Boolean, schema: IndexedDataType.MapType): ArrayBasedMapData = {
    @tailrec
    def read(resKeys: Seq[UTF8String], resValues: Seq[Any],
             key: Option[UTF8String] = None,
             requireKeyValueSeparator: Boolean = false,
             requireItemSeparator: Boolean = false): (Seq[UTF8String], Seq[Any]) = {
      val token = readToken(allowEof)
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
              val value = parseNode(token, allowEof, schema.valueType)
              read(resKeys, value +: resValues, None, requireItemSeparator = true)
            case None =>
              val k = parseNode(token, allowEof, IndexedDataType.NoneType).asInstanceOf[UTF8String]
              read(k +: resKeys, resValues, Some(k), requireKeyValueSeparator = true)
          }
      }
    }

    val (keys, values) = read(Nil, Nil)

    new ArrayBasedMapData(ArrayData.toArrayData(keys), ArrayData.toArrayData(values))
  }


  def parseNoneMap(endToken: Byte, allowEof: Boolean): Int = {
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
          parseNode(token, allowEof, IndexedDataType.NoneType)
          key match {
            case Some(_) => read(None, requireItemSeparator = true)
            case None => read(Some(1), requireKeyValueSeparator = true)
          }
      }
    }

    read()

    1
  }

  def readVarInt32: Int = {
    input.readSInt32
  }

  def readRawBytes(size: Int): Array[Byte] = {
    input.readRawBytes(size)
  }

  def readBinaryString: UTF8String = {
    val size = readVarInt32
    if (size < 0) throw new YsonFormatException("Negative binary string length")
    UTF8String.fromBytes(readRawBytes(size))
  }

  def readScalaString: String = {
    val size = readVarInt32
    if (size < 0) throw new YsonFormatException("Negative binary string length")
    new String(readRawBytes(size))
  }

  def readDouble: Double = {
    input.readDouble
  }

  def parseWithoutAttributes(first: Byte, allowEof: Boolean, dataType: IndexedDataType): Any = {
    first match {
      case YsonTags.BINARY_STRING =>
        dataType match {
          case IndexedDataType.ScalaStringType => readScalaString
          case _ => readBinaryString
        }
      case YsonTags.BINARY_INT => readVarInt64
      //      case YsonTags.BINARY_UINT =>
      //        consumer.onUnsignedInteger(UnsignedLong.valueOf(readVarUint64))
      case YsonTags.BINARY_DOUBLE => readDouble
      case YsonTags.BINARY_FALSE => false
      case YsonTags.BINARY_TRUE => true
      case YsonTags.BEGIN_LIST =>
        dataType match {
          case IndexedDataType.ArrayType(elementType, _) => parseList(YsonTags.END_LIST, allowEof = false, elementType)
          case IndexedDataType.NoneType => parseNoneList(YsonTags.END_LIST, allowEof = false)
          case _ => throw new IllegalArgumentException(s"Unsupported data type: $dataType for yson list")
        }

      case YsonTags.BEGIN_MAP =>
        dataType match {
          case struct: IndexedDataType.StructType => parseStruct(YsonTags.END_MAP, allowEof = false, struct)
          case map: IndexedDataType.MapType => parseMap(YsonTags.END_MAP, allowEof = false, map)
          case IndexedDataType.NoneType => parseNoneMap(YsonTags.END_MAP, allowEof = false)
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
  final def skip(token: Byte, allowEof: Boolean, skipTo: Seq[Byte]): YsonDecoder = {
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

  def parseNode(first: Byte, allowEof: Boolean, dataType: IndexedDataType): Any = {
    val newFirst = if (first == YsonTags.BEGIN_ATTRIBUTES) {
      skip(readToken(allowEof), allowEof, YsonTags.END_ATTRIBUTES)
      readToken(allowEof)
    } else first
    parseWithoutAttributes(newFirst, allowEof, dataType)
  }

  def parseNode(): Any = {
    parseNode(readToken(allowEof = false), allowEof = true, dataType)
  }
}

object YsonDecoder {
  def decode(bytes: Array[Byte], dataType: IndexedDataType): Any = {
    val decoder = new YsonDecoder(bytes, dataType)
    decoder.parseNode()
  }
}

sealed abstract class IndexedDataType {
  def sparkDataType: DataType
}

object IndexedDataType {

  case class StructFieldMeta(index: Int, dataType: IndexedDataType, var isNull: Boolean) {
    def setNotNull(): Unit = {
      isNull = false
    }

    def setNull(): Unit = {
      isNull = true
    }
  }

  case class StructType(map: Map[String, StructFieldMeta], sparkDataType: types.StructType) extends IndexedDataType

  case class ArrayType(element: IndexedDataType, sparkDataType: DataType) extends IndexedDataType

  case class MapType(valueType: IndexedDataType, sparkDataType: DataType) extends IndexedDataType

  case class AtomicType(sparkDataType: DataType) extends IndexedDataType

  case object ScalaStringType extends IndexedDataType {
    override def sparkDataType: DataType = StringType
  }

  case object NoneType extends IndexedDataType {
    override def sparkDataType: DataType = NullType
  }

}
