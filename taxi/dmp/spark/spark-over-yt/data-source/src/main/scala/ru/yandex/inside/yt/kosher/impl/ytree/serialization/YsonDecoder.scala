package ru.yandex.inside.yt.kosher.impl.ytree.serialization

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.IndexedDataType.StructFieldMeta
import ru.yandex.misc.lang.number.UnsignedLong

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


  private def readList(endToken: Byte, allowEof: Boolean)(f: (Integer, Byte) => Unit): Unit = {
    @tailrec
    def read(i: Int, requireSeparator: Boolean = false): Unit = {
      val token = readToken(allowEof)
      token match {
        case t if t == endToken => // end reading
        case YsonTags.ITEM_SEPARATOR => read(i)
        case _ =>
          if (requireSeparator) throw new YsonUnexpectedToken(token, "ITEM_SEPARATOR")
          f(i, token)
          read(i + 1, requireSeparator = true)
      }
    }
    read(0)
  }

  def parseSomeList(endToken: Byte, allowEof: Boolean, elementType: IndexedDataType): ArrayData = {
    val res = Array.newBuilder[Any]
    readList(endToken, allowEof) { (_, token) =>
        val element = parseNode(token, allowEof, elementType)
        res += element
    }
    ArrayData.toArrayData(res.result())
  }

  def parseNoneList(endToken: Byte, allowEof: Boolean): Int = {
    readList(endToken, allowEof) { (_, token) =>
      parseNode(token, allowEof, IndexedDataType.NoneType)
    }
    1
  }

  def parseBinaryList(endToken: Byte, allowEof: Boolean): Array[Byte] = {
    val pos = input.position
    readList(endToken, allowEof) { (_, token) =>
      parseNode(token, allowEof, IndexedDataType.AtomicType(BinaryType))
    }
    bytes.slice(pos, input.position)
  }

  def readVarInt64: Long = {
    input.readSInt64
  }

  def readInt64AsBytes: Array[Byte] = {
    input.readRawVarint64AsBytes
  }

  def readUInt64: Long = {
    input.readRawVarint64
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
                  parseNode(token, allowEof, IndexedDataType.NoneType)
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

  private def readTuple(endToken: Byte, allowEof: Boolean, schema: IndexedDataType.TupleType): Array[Any] = {
    val res = new Array[Any](schema.length)
    readList(endToken, allowEof) { (index, token) =>
      if (index < schema.length) {
        val fieldType = schema(index)
        res(index) = parseNode(token, allowEof, fieldType)
      } else {
        parseNode(token, allowEof, IndexedDataType.NoneType)
      }
    }
    res
  }

  def parseTuple(endToken: Byte, allowEof: Boolean, schema: IndexedDataType.TupleType): InternalRow = {
    new GenericInternalRow(readTuple(endToken, allowEof, schema))
  }

  def closeToken(token: Byte): Option[Byte] = {
    token match {
      case YsonTags.BEGIN_MAP => Some(YsonTags.END_MAP)
      case YsonTags.BEGIN_LIST => Some(YsonTags.END_LIST)
      case _ => None
    }
  }

  private def parseMap(endToken: Byte, allowEof: Boolean)
                      (keyParser : Byte => Any)(valueParser : Byte => Any): ArrayBasedMapData = {
    @tailrec
    def read(resKeys: Seq[Any], resValues: Seq[Any],
             key: Option[Any] = None,
             requireKeyValueSeparator: Boolean = false,
             requireItemSeparator: Boolean = false): (Seq[Any], Seq[Any]) = {
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
            case Some(_) =>
              val value = valueParser(token)
              read(resKeys, value +: resValues, None, requireItemSeparator = true)
            case None =>
              val k = keyParser(token)
              read(k +: resKeys, resValues, Some(k), requireKeyValueSeparator = true)
          }
      }
    }

    val (keys, values) =  read(Nil, Nil)
    new ArrayBasedMapData(ArrayData.toArrayData(keys), ArrayData.toArrayData(values))
  }

  def parseSomeMap(endToken: Byte, allowEof: Boolean,
                   schema: IndexedDataType.MapType, storedAsList: Boolean): ArrayBasedMapData = {
    def parseFromList(): ArrayBasedMapData = {
      var resKeys: Seq[Any] = Nil
      var resValues: Seq[Any] = Nil
      val structSchema = StructType(List(
        StructField("_1", schema.keyType.sparkDataType),
        StructField("_2", schema.valueType.sparkDataType)
      ))
      val pairType = IndexedDataType.TupleType(Seq(schema.keyType, schema.valueType), structSchema)
      readList(endToken, allowEof) { (_, token) =>
          val keyValue = readTuple(endToken, allowEof, pairType)
          val key = keyValue(0)
          if (key == null) throw new YsonUnexpectedToken(YsonTags.ENTITY, "NODE")
          val value = keyValue(1)
          resKeys = key +: resKeys
          resValues = value +: resValues
      }
      new ArrayBasedMapData(ArrayData.toArrayData(resKeys), ArrayData.toArrayData(resValues))
    }

    if (storedAsList) {
      parseFromList()
    } else {
      parseMap(endToken, allowEof)(parseNode(_, allowEof, schema.keyType))(parseNode(_, allowEof, schema.valueType))
    }
  }

  def parseNoneMap(endToken: Byte, allowEof: Boolean): Int = {
    parseMap(endToken, allowEof) { token =>
      parseNode(token, allowEof, IndexedDataType.NoneType)
      1
    } { token =>
      parseNode(token, allowEof, IndexedDataType.NoneType)
      None
    }
    1
  }

  def parseBinaryMap(endToken: Byte, allowEof: Boolean): Array[Byte] = {
    val pos = input.position
    parseMap(endToken, allowEof) {
      parseNode(_, allowEof, IndexedDataType.AtomicType(BinaryType))
    } {
      parseNode(_, allowEof, IndexedDataType.AtomicType(BinaryType))
    }
    bytes.slice(pos, input.position)
  }

  def readVarInt32: Int = {
    input.readSInt32
  }

  def readRawBytes(size: Int): Array[Byte] = {
    input.readRawBytes(size)
  }

  def readBinary: Array[Byte] = {
    val size = readVarInt32
    if (size < 0) throw new YsonFormatException("Negative binary string length")
    readRawBytes(size)
  }

  def readDouble: Double = {
    input.readDouble
  }

  def parseWithoutAttributes(first: Byte, allowEof: Boolean, dataType: IndexedDataType): Any = {
    first match {
      case YsonTags.BINARY_STRING =>
        val s = readBinary
        dataType match {
          case IndexedDataType.ScalaStringType => new String(s)
          case IndexedDataType.AtomicType(BinaryType) => s
          case _ =>  UTF8String.fromBytes(s)
        }
      case YsonTags.BINARY_INT =>
        dataType.sparkDataType match {
          case IntegerType => readVarInt64.toInt
          case BinaryType => first +: readInt64AsBytes
          case _ => readVarInt64
        }
      case YsonTags.BINARY_UINT =>
        dataType.sparkDataType match {
          case LongType => readUInt64
          case IntegerType => readUInt64.toInt
          case DoubleType => UnsignedLong.valueOf(readUInt64).doubleValue()
          case BinaryType => first +: readInt64AsBytes
          case NullType => null
        }
      case YsonTags.BINARY_DOUBLE =>
        dataType.sparkDataType match {
          case BinaryType => first +: readRawBytes(8)
          case _ => readDouble
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
          case IndexedDataType.ArrayType(elementType, _) => parseSomeList(YsonTags.END_LIST, allowEof = false, elementType)
          case map: IndexedDataType.MapType => parseSomeMap(YsonTags.END_LIST, allowEof = false, map, storedAsList = true)
          case tuple: IndexedDataType.TupleType => parseTuple(YsonTags.END_LIST, allowEof = false, tuple)
          case IndexedDataType.AtomicType(BinaryType) => first +: parseBinaryList(YsonTags.END_LIST, allowEof = false)
          case IndexedDataType.NoneType => parseNoneList(YsonTags.END_LIST, allowEof = false)
          case _ => throw new IllegalArgumentException(s"Unsupported data type: $dataType for yson list")
        }

      case YsonTags.BEGIN_MAP =>
        dataType match {
          case struct: IndexedDataType.StructType => parseStruct(YsonTags.END_MAP, allowEof = false, struct)
          case map: IndexedDataType.MapType => parseSomeMap(YsonTags.END_MAP, allowEof = false, map, storedAsList = false)
          case IndexedDataType.AtomicType(BinaryType) => first +: parseBinaryMap(YsonTags.END_MAP, allowEof = false)
          case IndexedDataType.NoneType => parseNoneMap(YsonTags.END_MAP, allowEof = false)
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
      parseNoneMap(YsonTags.END_ATTRIBUTES, allowEof)
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
