package tech.ytsaurus.spyt.format

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}
import org.scalatest.{FlatSpec, Matchers}
import tech.ytsaurus.spyt._
import tech.ytsaurus.spyt.format.conf.YtTableSparkSettings.{SortColumns, UniqueKeys}
import tech.ytsaurus.spyt.serializers.SchemaConverter.{SortOption, Sorted, Unordered}
import tech.ytsaurus.spyt.test.{LocalSpark, TmpDir}
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.wrapper.client.YtClientConfiguration
import tech.ytsaurus.client.{ApiServiceTransaction, CompoundClient, TableWriter}
import tech.ytsaurus.core.cypress.YPath
import tech.ytsaurus.core.tables.{ColumnValueType, TableSchema}
import tech.ytsaurus.spyt.format.conf.SparkYtWriteConfiguration

import java.util
import java.util.concurrent.CompletableFuture
import scala.concurrent.duration._
import scala.language.postfixOps

class YtOutputWriterTest extends FlatSpec with TmpDir with LocalSpark with Matchers {
  import YtOutputWriterTest._
  private val schema = StructType(Seq(StructField("a", IntegerType)))

  "YtOutputWriterTest" should "write several batches" in {
    runTestWithSpecificPath(tmpPath)
  }

  it should "write several batches with '#' in path" in {
    runTestWithSpecificPath(tmpPath + "#")
  }

  it should "exception while writing several batches with relative in path" in {
    an[IllegalArgumentException] shouldBe thrownBy {
      prepareWrite("subfolder", Unordered) { transaction => }
    }
  }

  it should "not write several batches if table is sorted" in {
    prepareWrite(tmpPath, Sorted(Seq("a"), uniqueKeys = false)) { transaction =>
      val writer = new MockYtOutputWriter(tmpPath.drop(1), transaction, 2, Sorted(Seq("a"), uniqueKeys = false))
      val rows = Seq(Row(1), Row(2), Row(3), Row(4))

      writeRows(rows, writer, transaction)

      spark.read.yt(tmpPath).collect() should contain theSameElementsAs rows
      YtWrapper.chunkCount(tmpPath) shouldEqual 1
    }
  }

  it should "use YtOutputWriter if used via spark.write.format(yt) for static tables" in {
    import spark.implicits._
    val sampleData = (1 to 1000).map(n => SampleRow(n.longValue() * n, 1.0 + 1.7*n, s"$n-th row"))

    val df = spark.createDataset(sampleData)
    df.write.format("yt").save("yt:/" + tmpPath)

    val yPath = YPath.simple(YtWrapper.formatPath(tmpPath))
    val outputPathAttributes = YtWrapper.attributes(yPath, None, Set.empty[String])

    outputPathAttributes("dynamic").boolValue() shouldBe false

    YtDataCheck.yPathShouldContainExpectedData(yPath, sampleData)(_.getValues.get(0).longValue())
  }

  def runTestWithSpecificPath(path: String): Unit = {
    prepareWrite(path, Unordered) { transaction =>
      val writer = new MockYtOutputWriter(path.drop(1), transaction, 2, Unordered)
      val rows = Seq(Row(1), Row(2), Row(3), Row(4))

      writeRows(rows, writer, transaction)

      spark.read.yt(path).collect() should contain theSameElementsAs rows
      YtWrapper.chunkCount(path) shouldEqual 2
    }
  }

  def prepareWrite(path: String, sortOption: SortOption)
                  (f: ApiServiceTransaction => Unit): Unit = {
    val transaction = YtWrapper.createTransaction(parent = None, timeout = 1 minute)
    val transactionId = transaction.getId.toString

    YtWrapper.createTable(path, TestTableSettings(schema, sortOption = sortOption),
      transaction = Some(transactionId))

    try {
      f(transaction)
    } catch {
      case e: Throwable =>
        try {
          transaction.abort().join()
        } catch {
          case ae: Throwable =>
            e.addSuppressed(ae)
        }
        throw e
    }
  }

  def writeRows(rows: Seq[Row], writer: YtOutputWriter, transaction: ApiServiceTransaction): Unit = {
    try {
      rows.foreach(r => writer.write(new TestInternalRow(r)))
    } finally {
      try {
        writer.close()
      } finally {
        transaction.commit().join()
      }
    }
  }

  class MockYtOutputWriter(path: String, transaction: ApiServiceTransaction, batchSize: Int,
                           sortOption: SortOption)
    extends YtOutputWriter(
      path,
      schema,
      YtClientConfiguration.default("local"),
      SparkYtWriteConfiguration(1, batchSize, batchSize, 5 minutes, typeV3Format = false),
      transaction.getId.toString,
      Map("sort_columns" -> SortColumns.set(sortOption.keys), "unique_keys" -> UniqueKeys.set(sortOption.uniqueKeys))
    ) {
    override protected def initializeWriter(): TableWriter[InternalRow] = {
      val writer = super.initializeWriter()

      new TableWriter[InternalRow] {
        override def getSchema: TableSchema = writer.getSchema

        override def write(rows: util.List[InternalRow], schema: TableSchema): Boolean = writer.write(rows, schema)

        override def readyEvent(): CompletableFuture[Void] = writer.readyEvent()

        override def close(): CompletableFuture[_] = {
          Thread.sleep((5 seconds).toMillis) // to prevent instant closing that shades some bugs
          writer.close()
        }

        override def getTableSchema: CompletableFuture[TableSchema] = writer.getTableSchema

        override def cancel(): Unit = writer.cancel()
      }
    }

    override protected def createYtClient(): CompoundClient = yt

    override protected def initialize(): Unit = {}
  }

  class TestInternalRow(row: Row) extends InternalRow {
    override def numFields: Int = row.length

    override def setNullAt(i: Int): Unit = ???

    override def update(i: Int, value: Any): Unit = ???

    override def copy(): InternalRow = new TestInternalRow(row.copy())

    override def isNullAt(ordinal: Int): Boolean = row.isNullAt(ordinal)

    override def getBoolean(ordinal: Int): Boolean = row.getBoolean(ordinal)

    override def getByte(ordinal: Int): Byte = row.getByte(ordinal)

    override def getShort(ordinal: Int): Short = row.getShort(ordinal)

    override def getInt(ordinal: Int): Int = row.getInt(ordinal)

    override def getLong(ordinal: Int): Long = row.getLong(ordinal)

    override def getFloat(ordinal: Int): Float = row.getFloat(ordinal)

    override def getDouble(ordinal: Int): Double = row.getDouble(ordinal)

    override def getDecimal(ordinal: Int, precision: Int, scale: Int): Decimal = ???

    override def getUTF8String(ordinal: Int): UTF8String = UTF8String.fromString(row.getString(ordinal))

    override def getBinary(ordinal: Int): Array[Byte] = ???

    override def getInterval(ordinal: Int): CalendarInterval = ???

    override def getStruct(ordinal: Int, numFields: Int): InternalRow = ???

    override def getArray(ordinal: Int): ArrayData = ???

    override def getMap(ordinal: Int): MapData = ???

    override def get(ordinal: Int, dataType: DataType): AnyRef = ???
  }

}

object YtOutputWriterTest {
  case class SampleRow(id: Long, ratio: Double, value: String)

  implicit val sampleRowOrdering: Ordering[SampleRow] = Ordering.by(_.id)
}


