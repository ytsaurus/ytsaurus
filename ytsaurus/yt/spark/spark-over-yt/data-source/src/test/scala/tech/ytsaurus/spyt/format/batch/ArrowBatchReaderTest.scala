package tech.ytsaurus.spyt.format.batch

import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructType}
import org.scalatest.{FlatSpec, Matchers}
import tech.ytsaurus.spyt.test.{LocalSpark, TmpDir}
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.wrapper.table.OptimizeMode
import tech.ytsaurus.spyt.YtWriter
import tech.ytsaurus.core.cypress.YPath
import tech.ytsaurus.spyt.SchemaTestUtils
import tech.ytsaurus.spyt.wrapper.table.YtArrowInputStream

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
    val reader = new ArrowBatchReader(stream, schema)
    val expected = readExpected("arrow_old_expected", schema)

    val rows = readFully(reader, schema, Int.MaxValue)
    rows should contain theSameElementsAs expected
  }

  it should "read new arrow format (>= 0.15.0)" in {
    val stream = new TestInputStream(getClass.getResourceAsStream("arrow_new"))
    val reader = new ArrowBatchReader(stream, schema)
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
    val reader = new ArrowBatchReader(stream, schema)

    val rows = readFully(reader, schema, Int.MaxValue)
    rows should contain theSameElementsAs data.map(Row(_))
  }

  it should "read arrow stream from yt" in {
    import spark.implicits._
    val schema = StructType(Seq(structField("a", IntegerType)))

    def testSlice(data: Seq[Int], batchSize: Int, lowerRowIndex: Int, upperRowIndex: Int): Unit = {
      val stream = YtWrapper.readTableArrowStream(YPath.simple(tmpPath).withRange(lowerRowIndex, upperRowIndex))
      val reader = new ArrowBatchReader(stream, schema)

      val rows = readFully(reader, schema, batchSize)
      val expected = data.slice(lowerRowIndex, upperRowIndex).map(Row(_))

      rows shouldEqual expected
    }

    val chunkCount = 3
    val chunkRowCounts = List(1, 5, 10)

    chunkRowCounts.foreach {
      chunkRowCount =>
        val data = (0 to chunkCount * chunkRowCount).toList
        YtWrapper.removeIfExists(tmpPath)

        (0 to chunkCount).foreach {
          chunkIndex =>
            val chunk = data.slice(chunkIndex * chunkRowCount, (chunkIndex + 1) * chunkRowCount).toDF("a")
            chunk.write.optimizeFor(OptimizeMode.Scan).sortedBy("a").mode(SaveMode.Append).yt(tmpPath)
        }

        testSlice(data, chunkRowCount, 0, 10)
        testSlice(data, chunkRowCount, 18, 2)
        testSlice(data, chunkRowCount, 6, 6)
    }
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
