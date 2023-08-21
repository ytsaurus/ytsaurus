package tech.ytsaurus.spyt.fs

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkException
import org.apache.spark.sql.execution.InputAdapter
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf._
import org.apache.spark.sql.types._
import org.apache.spark.sql.yson.{UInt64Long, UInt64Type}
import org.apache.spark.sql.{AnalysisException, DataFrameReader, Row, SaveMode}
import org.apache.spark.status.api.v1
import org.apache.spark.test.UtilsWrapper
import org.mockito.scalatest.MockitoSugar
import org.scalatest.prop.TableDrivenPropertyChecks
import org.scalatest.{FlatSpec, Matchers, PrivateMethodTester}
import tech.ytsaurus.spyt._
import tech.ytsaurus.spyt.test.{LocalSpark, TestUtils, TmpDir}
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.wrapper.table.OptimizeMode
import tech.ytsaurus.client.rows.{UnversionedRow, UnversionedValue}
import tech.ytsaurus.core.tables.{ColumnValueType, TableSchema}
import tech.ytsaurus.typeinfo.TiType
import tech.ytsaurus.ysontree.YTree

import java.sql.{Date, Timestamp}
import java.time.LocalDate
import java.util.concurrent.TimeUnit
import scala.concurrent.duration._
import scala.language.postfixOps

class YtSparkSQLTest extends FlatSpec with Matchers with LocalSpark
  with TmpDir with TestUtils with MockitoSugar with TableDrivenPropertyChecks with PrivateMethodTester {
  import spark.implicits._

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

  it should "select rows using views" in {
    writeTableFromYson(Seq(
      """{a = 1; b = "a"; c = 0.3}""",
      """{a = 2; b = "b"; c = 0.5}"""
    ), tmpPath, atomicSchema)

    val table = spark.read.yt(tmpPath)
    table.createOrReplaceTempView("table")
    val res = spark.sql(s"SELECT * FROM table")

    res.columns should contain theSameElementsAs Seq("a", "b", "c")
    res.select("a", "b", "c").collect() should contain theSameElementsAs Seq(
      Row(1, "a", 0.3),
      Row(2, "b", 0.5)
    )
  }

  it should "select rows" in {
    writeTableFromYson(Seq(
      """{a = 1; b = "a"; c = 0.3}""",
      """{a = 2; b = "b"; c = 0.5}"""
    ), tmpPath, atomicSchema)

    val res = spark.sql(s"SELECT * FROM yt.`ytTable:/${tmpPath}`")

    res.columns should contain theSameElementsAs Seq("a", "b", "c")
    res.select("a", "b", "c").collect() should contain theSameElementsAs Seq(
      Row(1, "a", 0.3),
      Row(2, "b", 0.5)
    )
  }

  it should "count io statistics" in {
    val customPath = "ytTable:/" + tmpPath
    val data = Stream.from(1).take(1000)

    val store = UtilsWrapper.appStatusStore(spark)
    val stagesBefore = store.stageList(null)
    val totalInputBefore = stagesBefore.map(_.inputBytes).sum
    val totalOutputBefore = stagesBefore.map(_.outputBytes).sum

    data.toDF().coalesce(1).write.yt(customPath)
    val allRows = spark.sql(s"SELECT * FROM yt.`ytTable:/${tmpPath}`").collect()
    allRows should have size data.length

    val stages = store.stageList(null)
    val totalInput = stages.map(_.inputBytes).sum
    val totalOutput = stages.map(_.outputBytes).sum

    totalInput should be > totalInputBefore
    totalOutput should be > totalOutputBefore

  }
}
