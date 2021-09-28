package ru.yandex.spark.yt.format

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkException
import org.apache.spark.sql.internal.SQLConf.{FILES_MAX_PARTITION_BYTES, FILES_OPEN_COST_IN_BYTES, PARALLEL_PARTITION_DISCOVERY_THRESHOLD}
import org.scalatest.{FlatSpec, Matchers}
import ru.yandex.spark.yt._
import ru.yandex.spark.yt.test._

import scala.language.postfixOps
import scala.util.Random

class DynamicTableReadTest extends FlatSpec with Matchers with LocalSpark with TmpDir with TestUtils with DynTableTestUtils {

  import spark.implicits._

  "YtFileFormat" should "read dynamic table" in {
    prepareTestTable(tmpPath, testData, Nil)
    val df = spark.read.yt(tmpPath)
    df.selectAs[TestRow].collect() should contain theSameElementsAs testData
  }

  it should "read dynamic table with pivot keys" in {
    prepareTestTable(tmpPath, testData, Seq(Seq(), Seq(3), Seq(6, 12)))
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
}
