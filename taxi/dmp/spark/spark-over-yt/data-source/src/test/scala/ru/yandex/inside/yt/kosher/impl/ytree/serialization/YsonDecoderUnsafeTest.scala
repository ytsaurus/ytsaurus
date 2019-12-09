package ru.yandex.inside.yt.kosher.impl.ytree.serialization

import java.io.ByteArrayInputStream

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.{FlatSpec, Matchers}
import org.scalatestplus.scalacheck.{ScalaCheckDrivenPropertyChecks, ScalaCheckPropertyChecks}
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.YsonTags._
import ru.yandex.spark.yt.serializers.SchemaConverter

import scala.io.Source

class YsonDecoderUnsafeTest extends FlatSpec with Matchers with ScalaCheckPropertyChecks with ScalaCheckDrivenPropertyChecks {

  behavior of "YsonDecoderUnsafeTest"

  private def decoder(bytes: Array[Byte]): YsonDecoderUnsafe = {
    new YsonDecoderUnsafe(bytes)
  }

  it should "skip" in {
    decoder(Array(1, 2, 3, 4, 5))
      .skip(1.toByte, allowEof = false, Seq(3.toByte))
      .readToken(allowEof = false) shouldEqual 4

    decoder(Array(1, BEGIN_LIST, 3, 4, END_LIST, 3, 5))
      .skip(1.toByte, allowEof = false, Seq(3.toByte))
      .readToken(allowEof = false) shouldEqual 5

    decoder(Array(1, BEGIN_LIST, 3, 4, BEGIN_MAP, 3, 5, BEGIN_LIST, 3, 6, END_LIST, 3, 7, END_MAP, 3, 8, END_LIST, 3, 14))
      .skip(1.toByte, allowEof = false, Seq(3.toByte))
      .readToken(allowEof = false) shouldEqual 14

    decoder(Array(1, BEGIN_LIST, 3, 4, BEGIN_MAP, 3, 5, BEGIN_LIST, 3, 6, END_LIST, 3, 7, END_MAP, 3, 8, END_LIST, 3, 14, END_MAP, 3, 15))
      .skip(BEGIN_MAP, allowEof = false, Seq(3.toByte))
      .readToken(allowEof = false) shouldEqual 15
  }

  it should "decode bytes" in {
    val bytes = readBytes("bytes-struct")

    val schema = StructType(Seq(
      StructField("_id", StringType),
      StructField("created", StringType),
      StructField("updated", StringType),
      StructField("device_id", StringType),
      StructField("phone_id", StringType),
      StructField("yandex_uid", StringType),
      StructField("yandex_uuid", StringType),
      StructField("old_yandex_uuid", StringType),
      StructField("application", StringType),
      StructField("application_version", StringType),
      StructField("token_only", BooleanType),
      StructField("authorized", BooleanType),
      StructField("has_ya_plus", BooleanType),
      StructField("yandex_staff", BooleanType),
      StructField("banners_enabled", ArrayType(StringType)),
      StructField("banners_seen", ArrayType(StringType))
    ))

    val writer = new UnsafeRowWriter(1)
    writer.reset()
    YsonDecoderUnsafe.decode(bytes, SchemaConverter.indexedDataType(schema), writer, 0)
    val res = writer.getRow.get(0, schema).asInstanceOf[InternalRow].toSeq(schema)

    res should contain theSameElementsInOrderAs Seq(
      UTF8String.fromString("0000003c71850288d5d81c1d08d26e0a"),
      UTF8String.fromString("2019-04-16 19:15:41.860000"),
      UTF8String.fromString("2019-04-16 19:16:29.165000"),
      null,
      null,
      null,
      UTF8String.fromString("a4a655e51c5d4772af6aefc60cbed97c"),
      null,
      UTF8String.fromString("android"),
      UTF8String.fromString("3.97.3"),
      null,
      null,
      false,
      null,
      null,
      null
    )
  }

  it should "decode bytes with { in string" in {
    val bytes = readBytes("bytes-brackets")

    val schema = StructType(Seq(
      StructField("_id", StringType),
      StructField("created", StringType),
      StructField("updated", StringType),
      StructField("device_id", StringType),
      StructField("phone_id", StringType),
      StructField("yandex_uid", StringType),
      StructField("yandex_uuid", StringType),
      StructField("old_yandex_uuid", StringType),
      StructField("application", StringType),
      StructField("application_version", StringType),
      StructField("token_only", BooleanType),
      StructField("authorized", BooleanType),
      StructField("has_ya_plus", BooleanType),
      StructField("yandex_staff", BooleanType),
      StructField("banners_enabled", ArrayType(StringType)),
      StructField("banners_seen", ArrayType(StringType))
    ))

    val writer = new UnsafeRowWriter(1)
    writer.reset()
    YsonDecoderUnsafe.decode(bytes, SchemaConverter.indexedDataType(schema), writer, 0)
    val res = writer.getRow.get(0, schema).asInstanceOf[InternalRow].toSeq(schema)

    res should contain theSameElementsInOrderAs Seq(
      UTF8String.fromString("000000a9213b43819264e9aae55f2220"),
      UTF8String.fromString("2018-06-05 11:17:44.471000"),
      UTF8String.fromString("2018-06-05 11:19:32.100000"),
      UTF8String.fromString("3f2c172c56fef9bbd5a8bbe0d69e0de3a009c358"),
      null,
      null,
      UTF8String.fromString("a0eef3a882d6c15da2b7187f909660ca"),
      null,
      UTF8String.fromString("android"),
      UTF8String.fromString("3.61.2"),
      null,
      null,
      null,
      null,
      null,
      null
    )
  }

  it should "decode long from bytes" in {
    val bytes = readBytes("bytes-long")

    val expected = YTreeBinarySerializer.deserialize(new ByteArrayInputStream(bytes)).asMap()
      .getOrThrow("moderator_completed_at").longValue()
    val schema = StructType(Seq(StructField("moderator_completed_at", LongType)))

    val writer = new UnsafeRowWriter(1)
    writer.reset()
    YsonDecoderUnsafe.decode(bytes, SchemaConverter.indexedDataType(schema), writer, 0)
    val res = writer.getRow.get(0, schema).asInstanceOf[InternalRow]

    res.getLong(0) shouldEqual expected
  }

  private def readBytes(fileName: String): Array[Byte] = {
    Source
      .fromInputStream(getClass.getResourceAsStream(fileName))
      .mkString.trim
      .split(",").map(_.trim.toByte)
  }
}
