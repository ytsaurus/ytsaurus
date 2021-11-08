package ru.yandex.spark.yt.format.batch

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructType}
import org.scalatest.{FlatSpec, Matchers}
import ru.yandex.inside.yt.kosher.cypress.YPath
import ru.yandex.spark.yt.test.{LocalSpark, TmpDir}
import ru.yandex.spark.yt.wrapper.YtWrapper
import ru.yandex.spark.yt.wrapper.table.{OptimizeMode, YtArrowInputStream}
import ru.yandex.spark.yt.{SchemaTestUtils, YtWriter}

import java.io.InputStream

class ArrowBatchReaderTest extends FlatSpec with TmpDir with SchemaTestUtils
  with Matchers with LocalSpark with ReadBatchRows {

  behavior of "ArrowBatchReader"

  private val schema = StructType(Seq(
    structField("_0", DoubleType),
    structField("_1", DoubleType),
    structField("_2", DoubleType)
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

  it should "read empty stream" in {
    import spark.implicits._
    val schema = StructType(Seq(structField("a", IntegerType)))
    val data = Seq[Int]()
    val df = data.toDF("a")
    df.write.optimizeFor(OptimizeMode.Scan).yt(tmpPath)

    val stream = YtWrapper.readTableArrowStream(YPath.simple(tmpPath))
    val reader = new ArrowBatchReader(stream, data.length, schema)

    val rows = readFully(reader, schema, Int.MaxValue)
    rows should contain theSameElementsAs data.map(Row(_))
  }

  private def readExpected(filename: String, schema: StructType): Seq[Row] = {
    val path = getClass.getResource(filename).getPath
    spark.read.schema(schema).csv(s"file://$path").collect()
  }

  private class TestInputStream(is: InputStream) extends YtArrowInputStream {
    override def isNextPage: Boolean = false

    override def isEmptyPage: Boolean = false

    override def read(): Int = is.read()

    override def read(b: Array[Byte]): Int = is.read(b)

    override def read(b: Array[Byte], off: Int, len: Int): Int = is.read(b, off, len)
  }
}
