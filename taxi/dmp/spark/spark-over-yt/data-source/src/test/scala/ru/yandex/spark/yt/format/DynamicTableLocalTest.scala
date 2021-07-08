package ru.yandex.spark.yt.format

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkException
import org.apache.spark.sql.Encoders
import org.scalatest.{FlatSpec, Matchers}
import ru.yandex.spark.yt._
import ru.yandex.spark.yt.test.{LocalSpark, TmpDir}
import ru.yandex.spark.yt.wrapper.YtWrapper
import ru.yandex.yt.ytclient.proxy.CompoundClient
import ru.yandex.yt.ytclient.tables.{ColumnSchema, ColumnSortOrder, ColumnValueType, TableSchema}

import scala.concurrent.duration._
import scala.language.postfixOps


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

  it should "read dynamic table with long pivot keys" in {
    prepareTestTable(tmpPath, testData, Seq("[]", "[3]", "[6;12]"))
    val df = spark.read.yt(tmpPath)
    df.selectAs[TestRow].collect() should contain theSameElementsAs testData
  }

  it should "read many dynamic tables" in {
    val tablesCount = 35
    (1 to tablesCount).par.foreach { i =>
      prepareTestTable(s"$tmpPath/$i", testData, Nil)
    }

    Logger.getRootLogger.setLevel(Level.OFF)
    a[SparkException] should be thrownBy {
      spark.read.yt((1 to tablesCount).map(i => s"$tmpPath/$i"): _*).show()
    }
    Logger.getRootLogger.setLevel(Level.WARN)
  }

  def prepareTestTable(path: String, data: Seq[TestRow], pivotKeys: Seq[String]): Unit = {
    import scala.collection.JavaConverters._
    val schema = Encoders.product[TestRow].schema
    YtWrapper.createTable(path, TestTableSettings(schema, isDynamic = true, sortColumns = Seq("a", "b")))
    YtWrapper.mountTable(path)
    YtWrapper.waitState(path, YtWrapper.TabletState.Mounted, 10 seconds)
    YtWrapper.insertRows(path, testSchema, data.map(r => r.productIterator.toList.asJava).asJava)
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
