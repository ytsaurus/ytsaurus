package ru.yandex.inside.yt.kosher.impl.ytree.serialization

import java.io.ByteArrayInputStream

import ru.yandex.inside.yt.kosher.impl.ytree.serialization.YTreeBinarySerializer.CodedInputStream

class YsonByteReader(bytes: Array[Byte]) {
  private var _position: Int = 0
//  private val input = new CodedInputStream(new ByteArrayInputStream(bytes), 65536)

  def isAtEnd: Boolean = _position >= bytes.length

  def readRawByte: Byte = {
    val res = bytes(_position)
    _position += 1

//    val expected = input.readRawByte()
//    assert(res == expected)

    res
  }

  private def readRawByteInner: Byte = {
    val res = bytes(_position)
    _position += 1

    res
  }

  def readSInt64: Long = {
    val res = decodeZigZag64(readRawVarint64)
//    val expected = input.readSInt64()
//    assert(res == expected)
    res
  }

  private def decodeZigZag64(n: Long): Long = {
    (n >>> 1) ^ -(n & 1)
  }

  private def readRawVarint64: Long = {
    if (isAtEnd) throw new IllegalArgumentException("")
    var pos = _position

    def incPos: Int = {
      pos += 1
      pos - 1
    }

    var x = 0L

    def chX(f: => Long): Long = {
      x = f
      x
    }

    val y = bytes(incPos)
    if (y >= 0) {
      _position = pos
      y
    } else if (bytes.length - pos < 9) {
      readRawVarint64SlowPath
    } else {
      if (chX(y ^ (bytes(incPos) << 7)) < 0L) {
        _position = pos
        chX(x ^ ((~0L << 7)))
      } else if (chX(x ^ (bytes(incPos) << 14)) >= 0L) {
        _position = pos
        chX(x ^ ((~0L << 7) ^ (~0L << 14)))
      } else if (chX(x ^ (bytes(incPos) << 21)) < 0L) {
        _position = pos
        chX(x ^ ((~0L << 7) ^ (~0L << 14) ^ (~0L << 21)))
      } else if (chX(x ^ (bytes(incPos).toLong << 28)) >= 0L) {
        _position = pos
        chX(x ^ ((~0L << 7) ^ (~0L << 14) ^ (~0L << 21) ^ (~0L << 28)))
      } else if (chX(x ^ (bytes(incPos).toLong << 35)) < 0L) {
        _position = pos
        chX(x ^ ((~0L << 7) ^ (~0L << 14) ^ (~0L << 21) ^ (~0L << 28) ^ (~0L << 35)))
      } else if (chX(x ^ (bytes(incPos).toLong << 42)) >= 0L) {
        _position = pos
        chX(x ^ ((~0L << 7) ^ (~0L << 14) ^ (~0L << 21) ^ (~0L << 28) ^ (~0L << 35) ^ (~0L << 42)))
      } else if (chX(x ^ (bytes(incPos).toLong << 49)) < 0L) {
        _position = pos
        chX(x ^ ((~0L << 7) ^ (~0L << 14) ^ (~0L << 21) ^ (~0L << 28) ^ (~0L << 35) ^ (~0L << 42) ^ (~0L << 49)))
      } else {
        chX(x ^ ((bytes(incPos).toLong << 56)))
        chX(x ^ ((~0L << 7) ^ (~0L << 14) ^ (~0L << 21) ^ (~0L << 28) ^ (~0L << 35) ^ (~0L << 42) ^ (~0L << 49) ^ (~0L << 56)))
        if (x < 0L && bytes(incPos) < 0L) {
          readRawVarint64SlowPath // Will throw malformed varint
        } else {
          _position = pos
          x
        }
      }
    }
  }


  private def readRawVarint32: Int = {
    var pos = _position

    def incPos: Int = {
      pos += 1
      pos - 1
    }

    if (isAtEnd) throw new IllegalArgumentException("")

    var x = 0

    def chX(f: => Int): Int = {
      x = f
      x
    }

    if (chX(bytes(incPos)) >= 0) {
      _position = pos
      x
    } else if (bytes.length - pos < 9) {
      readRawVarint64SlowPath.toInt
    } else {
      if (chX(x ^ (bytes(incPos) << 7)) < 0L) {
        _position = pos
        chX((x ^ (~0L << 7)).toInt)
      } else if (chX(x ^ (bytes(incPos) << 14)) >= 0L) {
        _position = pos
        chX((x ^ (~0L << 7) ^ (~0L << 14)).toInt)
      } else if (chX(x ^ (bytes(incPos) << 21)) < 0L) {
        _position = pos
        chX((x ^ (~0L << 7) ^ (~0L << 14) ^ (~0L << 21)).toInt)
      } else {
        val y = bytes(incPos)
        chX(x ^ (y << 28))
        chX((x ^ ((~0L << 7) ^ (~0L << 14) ^ (~0L << 21) ^ (~0L << 28))).toInt)
        if (y < 0 &&
          bytes(incPos) < 0 &&
          bytes(incPos) < 0 &&
          bytes(incPos) < 0 &&
          bytes(incPos) < 0 &&
          bytes(incPos) < 0) {
          readRawVarint64SlowPath.toInt // Will throw malformed varint
        } else {
          _position = pos
          x
        }
      }
    }
  }


  private def readRawVarint64SlowPath: Long = {
    var result = 0L
    var shift = 0
    while (shift < 64) {
      val b = readRawByteInner
      result = result | ((b & 0x7F).toLong << shift)
      if ((b & 0x80) == 0) return result

      shift += 7
    }
    throw new IllegalStateException("Malformed varint")
  }

  def readSInt32: Int = {
    val res = decodeZigZag32(readRawVarint32)
//    val expected = input.readSInt32()
//    assert(res == expected)
    res
  }

  def readDouble: Double = {
    val res = java.lang.Double.longBitsToDouble(readRawLittleEndian64)
//    val expected = input.readDouble()
//    assert(res == expected)
    res
  }

  private def readRawLittleEndian64: Long = {
    val a = readRawByteInner
    val b = readRawByteInner
    val c = readRawByteInner
    val d = readRawByteInner
    val e = readRawByteInner
    val f = readRawByteInner
    val g = readRawByteInner
    val h = readRawByteInner
    ((a.toLong & 0xffL)) |
      ((b.toLong & 0xffL) << 8) |
      ((c.toLong & 0xffL) << 16) |
      ((d.toLong & 0xffL) << 24) |
      ((e.toLong & 0xffL) << 32) |
      ((f.toLong & 0xffL) << 40) |
      ((g.toLong & 0xffL) << 48) |
      ((h.toLong & 0xffL) << 56)
  }

  private def decodeZigZag32(n: Int): Int = (n >>> 1) ^ -(n & 1)

  def readRawBytes(size: Int): Array[Byte] = {
    val result = new Array[Byte](size)
    for (i <- 0 until size) {
      result(i) = readRawByteInner
    }
//    val expected = input.readRawBytes(size)
//    assert(result.length == expected.length && result.zip(expected).forall{ case (x, y) => x == y})
    result
  }
}
