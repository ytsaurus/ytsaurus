package tech.ytsaurus.spyt.serializers

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

  it should "serialize dataframe to byte array" in {
    writeTableFromYson(Seq(
      """{a = 0; b = #; c = 0.0}"""
    ), tmpPath, atomicSchema)

    val res = spark.read.yt(tmpPath)
    val tableBytes = GenericRowSerializer.dfToYTFormat(res)
    val answer = Array(1, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
      1, 0, 2, 0, 0, 0, 0, 0, 2, 0, 5, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
    tableBytes shouldBe answer
  }

  it should "serialize dataframe to base64" in {
    writeTableFromYson(Seq(
      """{a = 1; b = "a"; c = 0.3}""",
      """{a = 2; b = "b"; c = 0.5}"""
    ), tmpPath, atomicSchema)

    val res = spark.read.yt(tmpPath)
    val tableBytes = GenericRowSerializer.dfToYTFormatWithBase64(res)
    val answer = "AgAAAAAAAAADAAAAAAAAAAAAAwAAAAAAAQAAAAAAAAABABAAAQAAAGEAAAAAAAAAAgAFAAAAAAAzMzMzMzPTPwMAAAAAAAAAAAAD" +
      "AAAAAAACAAAAAAAAAAEAEAABAAAAYgAAAAAAAAACAAUAAAAAAAAAAAAAAOA/"
    tableBytes shouldBe answer
  }
}
