package ru.yandex.spark.yt.format.batch

import java.io.InputStream

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.scalatest.{FlatSpec, Matchers}
import ru.yandex.spark.yt.test.LocalSpark
import ru.yandex.spark.yt.wrapper.table.YtArrowInputStream

class ArrowBatchReaderTest extends FlatSpec with Matchers with LocalSpark with ReadBatchRows {

  behavior of "ArrowVectorizedReaderTest"

  private val schema = StructType(Seq(
    StructField("a", DoubleType),
    StructField("b", DoubleType),
    StructField("c", DoubleType)
  ))

  it should "read old arrow format (< 0.15.0)" in {
    val stream = new TestInputStream(getClass.getResourceAsStream("arrow_old"))
    val reader = new ArrowBatchReader(stream, 100, schema)
    val expected = readExpected("arrow_old_expected", schema)

    val rows = readFully(reader, schema, Int.MaxValue)
    rows should contain theSameElementsAs expected
  }

  it should "read new arrow format (>= 0.15.0)" in {
    val stream = new TestInputStream(getClass.getResourceAsStream("arrow_new"))
    val reader = new ArrowBatchReader(stream, 100, schema)
    val expected = readExpected("arrow_new_expected", schema)

    val rows = readFully(reader, schema, Int.MaxValue)
    rows should contain theSameElementsAs expected
  }

  private def readExpected(filename: String, schema: StructType): Seq[Row] = {
    val path = getClass.getResource(filename).getPath
    spark.read.schema(schema).csv(s"file://$path").collect()
  }

  private class TestInputStream(is: InputStream) extends YtArrowInputStream {
    override def isNextPage: Boolean = false

    override def read(): Int = is.read()

    override def read(b: Array[Byte]): Int = is.read(b)

    override def read(b: Array[Byte], off: Int, len: Int): Int = is.read(b, off, len)
  }
}
