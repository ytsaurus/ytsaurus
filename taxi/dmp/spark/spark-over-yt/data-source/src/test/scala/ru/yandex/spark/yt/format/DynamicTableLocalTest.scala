package ru.yandex.spark.yt.format

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkException
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.internal.SQLConf.{FILES_OPEN_COST_IN_BYTES, PARALLEL_PARTITION_DISCOVERY_THRESHOLD}
import org.scalatest.{FlatSpec, Matchers}
import ru.yandex.spark.yt._
import ru.yandex.spark.yt.test.{LocalSpark, TmpDir}
import ru.yandex.spark.yt.wrapper.YtWrapper
import ru.yandex.yt.ytclient.proxy.CompoundClient
import ru.yandex.yt.ytclient.tables.{ColumnSchema, ColumnSortOrder, ColumnValueType, TableSchema}

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Random


class DynamicTableLocalTest extends FlatSpec with Matchers with LocalSpark with TmpDir {

  import spark.implicits._

  private val testSchema = new TableSchema.Builder()
    .setUniqueKeys(false)
    .add(new ColumnSchema("a", ColumnValueType.INT64, ColumnSortOrder.ASCENDING))
    .add(new ColumnSchema("b", ColumnValueType.INT64, ColumnSortOrder.ASCENDING))
    .addValue("c", ColumnValueType.STRING)
    .build()
  private val testData = (1 to 10).map(i => TestRow(i, i * 2, ('a'.toInt + i).toChar.toString))

  "YtFileFormat" should "read dynamic table" in {
    prepareTestTable(tmpPath, testData, Nil)
    val df = spark.read.yt(tmpPath)
    df.selectAs[TestRow].collect() should contain theSameElementsAs testData
  }

  it should "read dynamic table with pivot keys" in {
    prepareTestTable(tmpPath, testData, Seq("[]", "[3]", "[6;12]"))
    val df = spark.read.yt(tmpPath)
    df.selectAs[TestRow].collect() should contain theSameElementsAs testData
  }

  it should "read many dynamic tables" in {
    val tablesCount = 3
    (1 to tablesCount).par.foreach { i =>
      prepareTestTable(s"$tmpPath/$i", testData, Nil)
    }

    Logger.getRootLogger.setLevel(Level.OFF)
    a[SparkException] should be thrownBy {
      withConf(PARALLEL_PARTITION_DISCOVERY_THRESHOLD, "2") {
        spark.read.yt((1 to tablesCount).map(i => s"$tmpPath/$i"): _*).show()
      }
    }
    Logger.getRootLogger.setLevel(Level.WARN)
  }

  it should "read empty table" in {
    prepareTestTable(tmpPath, Seq.empty, Nil)
    val df = spark.read.yt(tmpPath)
    df.selectAs[TestRow].collect().isEmpty shouldBe true
  }

  it should "merge small partitions" in {
    val r = new Random()
    val testData = (1 to 1000).map(i => TestRow(i, i * 2, r.nextString(10)))
    val pivotKeys = "[]" +: (10 until 1000 by 10).map(i => s"[$i]")
    prepareTestTable(tmpPath, testData, pivotKeys)

    val partitions = withConf(FILES_OPEN_COST_IN_BYTES, "1") {
      spark.read.yt(tmpPath).rdd.partitions
    }

    partitions.length shouldEqual defaultParallelism +- 1
  }

  def prepareTestTable(path: String, data: Seq[TestRow], pivotKeys: Seq[String]): Unit = {
    val schema = Encoders.product[TestRow].schema
    YtWrapper.createTable(path, TestTableSettings(schema, isDynamic = true, sortColumns = Seq("a", "b")))
    YtWrapper.mountTable(path)
    YtWrapper.waitState(path, YtWrapper.TabletState.Mounted, 10 seconds)
    YtWrapper.insertRows(path, testSchema, data.map(r => r.productIterator.toList))
    YtWrapper.unmountTable(path)
    YtWrapper.waitState(path, YtWrapper.TabletState.Unmounted, 10 seconds)
    if (pivotKeys.nonEmpty) reshardTable(path, pivotKeys)
    YtWrapper.mountTable(path)
    YtWrapper.waitState(path, YtWrapper.TabletState.Mounted, 10 seconds)
  }

  def reshardTable(path: String, pivotKeys: Seq[String])(implicit yt: CompoundClient): Unit = {
    import scala.language.postfixOps
    import sys.process._

    s"yt --proxy localhost:8000 reshard-table $path ${pivotKeys.mkString(" ")}" !
  }
}
