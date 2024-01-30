package tech.ytsaurus.spyt.format

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkException
import org.apache.spark.sql.internal.SQLConf.PARALLEL_PARTITION_DISCOVERY_THRESHOLD
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers}
import tech.ytsaurus.core.cypress.RichYPathParser
import tech.ytsaurus.spyt._
import tech.ytsaurus.spyt.format.conf.SparkYtConfiguration
import tech.ytsaurus.spyt.test._
import tech.ytsaurus.spyt.wrapper.YtWrapper.createTransaction
import tech.ytsaurus.spyt.test.{DynTableTestUtils, TestRow}

import java.time.Instant
import java.time.temporal.{ChronoUnit, TemporalUnit}
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

class DynamicTableReadTest extends FlatSpec with Matchers with LocalSpark with TmpDir with TestUtils
  with TableDrivenPropertyChecks with DynTableTestUtils {

  import tech.ytsaurus.spyt._
  import spark.implicits._

  "YtFileFormat" should "read dynamic table" in {
    prepareTestTable(tmpPath, testData, Nil)
    val tr = createTransaction(None, 5.minutes)
    val df = spark.read.transaction(tr.getId.toString).yt(tmpPath)
    df.selectAs[TestRow].collect() should contain theSameElementsAs testData
  }

  it should "read dynamic table with pivot keys" in {
    prepareTestTable(tmpPath, testData, Seq(Seq(), Seq(3), Seq(6, 12)))
    val df = spark.read.yt(tmpPath)
    df.selectAs[TestRow].collect() should contain theSameElementsAs testData
  }

  it should "read many dynamic tables" in {
    val testModes = Table(
      "enableYtPartitioning",
      "false",
      "true"
    )

    val tablesCount = 3
    val tablePaths = (1 to tablesCount).map(i => s"$tmpPath/$i")
    val startTs = yt.generateTimestamps().join().getValue
    tablePaths.par.foreach(prepareTestTable(_, testData, Seq(Seq(), Seq(3))))
    val expectedResult = testData ++ testData ++ testData

    withConf(PARALLEL_PARTITION_DISCOVERY_THRESHOLD, "2") {
      forAll(testModes) { enableYtPartitioning =>
        withConf(s"spark.yt.${SparkYtConfiguration.Read.YtPartitioningEnabled.name}", enableYtPartitioning) {
          spark.read.yt(tablePaths: _*).selectAs[TestRow].collect() should contain theSameElementsAs expectedResult

          val df = spark.read.option("enable_inconsistent_read", "true").yt(tablePaths: _*)
          df.selectAs[TestRow].collect() should contain theSameElementsAs expectedResult

          val df2 = spark.read.option("timestamp", startTs).yt(tablePaths: _*)
          df2.selectAs[TestRow].collect() should contain theSameElementsAs Seq()
        }
      }
    }
  }

  it should "read empty table" in {
    prepareTestTable(tmpPath, Seq.empty, Nil)
    val df = spark.read.yt(tmpPath)
    df.selectAs[TestRow].collect().isEmpty shouldBe true
  }

  it should "read ordered table" in {
    prepareOrderedTestTable(tmpPath, enableDynamicStoreRead = true)
    val data = (1 to 15).map(i => getTestData(i / 2))
    appendChunksToTestTable(tmpPath, data, sorted = false)
    withConf(s"spark.yt.${SparkYtConfiguration.Read.YtPartitioningEnabled.name}", "false") {
      val df = spark.read.option("enable_inconsistent_read", "true").yt(tmpPath)
      df.selectAs[TestRow].collect() should contain theSameElementsAs data.flatten
    }
  }
}
