package ru.yandex.spark.yt.utils

import com.google.protobuf.ByteString

object DynamicTableInfoDeserializer {
  def deserializePivotKey(ser: ByteString): Seq[String] = {
    val bytes = ser.toByteArray
    assert(bytes.head == 0)
    val valuesCount = bytes(1)

    def parseBigInt(position: Int): (BigInt, Int) = {
      val (res, count, _) = Iterator.iterate((BigInt(0), 0, Option.empty[Byte])) { case (result, count, _) =>
        val byte = bytes(position + count)
        (result | (BigInt(byte & 0x7F) << (7 * count)), count + 1, Some(byte))
      }.dropWhile { case (_, _, byte) => byte.forall(b => (b & 0x80) != 0)}.next()

      (res, position + count)
    }

    def parseValue(position: Int): (BigInt, Int) = {
      assert(bytes(position + 1) == 4)
      parseBigInt(position + 2)
    }

    val (result, _) = (0 until valuesCount).foldLeft((Seq.empty[BigInt], 2)) { case ((res, position), _) =>
      val (value, newPosition) = parseValue(position)
      (value +: res, newPosition)
    }
    result.map(_.toString)
  }
}
