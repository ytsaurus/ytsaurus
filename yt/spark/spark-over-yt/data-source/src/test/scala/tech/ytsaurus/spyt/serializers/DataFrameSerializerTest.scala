package tech.ytsaurus.spyt.serializers

import org.apache.spark.sql.types._
import org.scalatest.{FlatSpec, Matchers}
import tech.ytsaurus.core.tables.{ColumnValueType, TableSchema}
import tech.ytsaurus.spyt._
import tech.ytsaurus.spyt.test.{LocalSpark, TestUtils, TmpDir}

class DataFrameSerializerTest extends FlatSpec with Matchers with LocalSpark
  with TmpDir with TestUtils {
  private val atomicSchema = TableSchema.builder()
    .setUniqueKeys(false)
    .addValue("a", ColumnValueType.INT64)
    .addValue("b", ColumnValueType.STRING)
    .addValue("c", ColumnValueType.DOUBLE)
    .build()

  private val anySchema = TableSchema.builder()
    .setUniqueKeys(false)
    .addValue("value", ColumnValueType.ANY)
    .build()

  it should "serialize dataframe to byte array" in {
    writeTableFromYson(Seq(
      """{a = 0; b = #; c = 0.0}"""
    ), tmpPath, atomicSchema)

    val res = spark.read.yt(tmpPath)
    val tableBytes = GenericRowSerializer.dfToYTFormat(res)
    val answer = Array(Array(-99, 0, 0, 0, 0, 0, 0, 0, 10, 49, 10, 1, 97, 16, 3, 72, 0, 90, 40, 123, 34, 116, 121,
      112, 101, 95, 110, 97, 109, 101, 34, 61, 34, 111, 112, 116, 105, 111, 110, 97, 108, 34, 59, 34, 105, 116, 101,
      109, 34, 61, 34, 105, 110, 116, 54, 52, 34, 59, 125, 10, 50, 10, 1, 98, 16, 16, 72, 0, 90, 41, 123, 34, 116, 121,
      112, 101, 95, 110, 97, 109, 101, 34, 61, 34, 111, 112, 116, 105, 111, 110, 97, 108, 34, 59, 34, 105, 116, 101,
      109, 34, 61, 34, 115, 116, 114, 105, 110, 103, 34, 59, 125, 10, 50, 10, 1, 99, 16, 5, 72, 0, 90, 41, 123, 34,
      116, 121, 112, 101, 95, 110, 97, 109, 101, 34, 61, 34, 111, 112, 116, 105, 111, 110, 97, 108, 34, 59, 34, 105,
      116, 101, 109, 34, 61, 34, 100, 111, 117, 98, 108, 101, 34, 59, 125, 24, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 3,
      0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0))
    tableBytes should contain theSameElementsAs answer
  }

  it should "serialize dataframe to base64" in {
    writeTableFromYson(Seq(
      """{a = 1; b = "a"; c = 0.3}""",
      """{a = 2; b = "b"; c = 0.5}"""
    ), tmpPath, atomicSchema)

    val res = spark.read.yt(tmpPath)
    val tableBytes = GenericRowSerializer.dfToYTFormatWithBase64(res)
    val answer = Array(
      "nQAAAAAAAAAKMQoBYRADSABaKHsidHlwZV9uYW1lIj0ib3B0aW9uYWwiOyJpdGVtIj0iaW50NjQiO30KMgoBYhAQSABaKXsidHlwZV9uYW1l" +
        "Ij0ib3B0aW9uYWwiOyJpdGVtIj0ic3RyaW5nIjt9CjIKAWMQBUgAWil7InR5cGVfbmFtZSI9Im9wdGlvbmFsIjsiaXRlbSI9ImRvdWJsZSI" +
        "7fRgAAAAAAgAAAAAAAAADAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAABAAAAAAAAAGEAAAAAAAAAMzMzMzMz0z8DAAAAAAAAAAAAAAAAAAAAAgA" +
        "AAAAAAAABAAAAAAAAAGIAAAAAAAAAAAAAAAAA4D8="
    )
    tableBytes should contain theSameElementsAs answer
  }

  it should "serialize lists" in {
    writeTableFromYson(Seq(
      "{value = [[1]; [2; 3]]}",
      "{value = [[4]; #]}"
    ), tmpPath, anySchema)

    val res = spark.read
      .schemaHint("value" -> ArrayType(ArrayType(LongType)))
      .yt(tmpPath)

    val tableBytes = GenericRowSerializer.dfToYTFormatWithBase64(res)
    val answer = Array(
      "OAAAAAAAAAAKNAoFdmFsdWUQEUgAWid7InR5cGVfbmFtZSI9Im9wdGlvbmFsIjsiaXRlbSI9Inlzb24iO30YAAIAAAAAAAAAAQAAAAAAAAA" +
        "AAAAAAAAAAA4AAAAAAAAAW1sCAl07WwIEOwIGXV0AAAEAAAAAAAAAAAAAAAAAAAAIAAAAAAAAAFtbAghdOyNd"
    )
    tableBytes should contain theSameElementsAs answer
  }

  it should "serialize map" in {
    writeTableFromYson(Seq(
      """{value = [[1; "2"]; [3; "4"]]}""",
      """{value = [[5; "6"]; [7; "8"]]}"""
    ), tmpPath, anySchema)

    val res = spark.read
      .schemaHint("value" -> MapType(LongType, StringType))
      .yt(tmpPath)

    val tableBytes = GenericRowSerializer.dfToYTFormatWithBase64(res)
    val answer = Array(
      "OAAAAAAAAAAKNAoFdmFsdWUQEUgAWid7InR5cGVfbmFtZSI9Im9wdGlvbmFsIjsiaXRlbSI9Inlzb24iO30YAAIAAAAAAAAAAQAAAAAAAAAAA" +
        "AAAAAAAABEAAAAAAAAAewECMz0BAjQ7AQIxPQECMn0AAAAAAAAAAQAAAAAAAAAAAAAAAAAAABEAAAAAAAAAewECNz0BAjg7AQI1PQECNn0AAAAAAAAA"
    )
    tableBytes should contain theSameElementsAs answer
  }
}
