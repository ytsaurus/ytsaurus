package tech.ytsaurus.spyt.format.types

import org.apache.spark.sql.types._
import org.apache.spark.sql.yson.{Datetime, DatetimeType, UInt64Long, UInt64Type}
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import tech.ytsaurus.core.tables.{ColumnValueType, TableSchema}
import tech.ytsaurus.spyt.common.utils.DateTimeTypesConverter._
import tech.ytsaurus.spyt.test.{LocalSpark, TestUtils, TmpDir}
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.{YtReader, YtWriter}
import tech.ytsaurus.typeinfo.TiType
import tech.ytsaurus.spyt.SchemaTestUtils

import java.sql.Date
import java.time.LocalDateTime
import scala.collection.mutable.ListBuffer

class DateTimeTypesTest extends AnyFlatSpec with Matchers with LocalSpark with TmpDir with TestUtils with SchemaTestUtils {
  behavior of "YtDataSource"

  val HOURS_OFFSET: Int = getUtcHoursOffset

  val ids: Seq[Long] = Seq(1L, 2L, 3L)
  val dateArr: Seq[String] = List.apply("1970-04-11", "2019-02-09", "1970-01-01")
  val datetimeArr: Seq[String] = List.apply("1970-04-11T00:00:00Z", "2019-02-09T13:41:11Z", "1970-01-01T00:00:00Z")
  val timestampArr: Seq[String] = List.apply("1970-04-11T00:00:00.000000Z", "2019-02-09T13:41:11.654321Z", "1970-01-01T00:00:00.000000Z")
  val numbers: Seq[Int] = Seq(101, 202, 303)

  it should "datetime types test: write table by yt - read by spark" in {
    val tableSchema: TableSchema = TableSchema.builder()
      .addValue("id", TiType.uint64())
      .addValue("date", TiType.date())
      .addValue("datetime", TiType.datetime())
      .addValue("timestamp", TiType.timestamp())
      .addValue("number", TiType.int32())
      .build()

    val ysonData = ListBuffer[String]()
    for (i <- ids.indices) {
      ysonData +=
        s"""{id = ${ids(i)};
           |date = ${dateToLong(dateArr(i))};
           |datetime = ${datetimeToLong(datetimeArr(i))};
           |timestamp = ${timestampToLong(timestampArr(i))};
           |number = ${numbers(i)}}""".stripMargin
    }
    writeTableFromYson(ysonData, tmpPath, tableSchema)

    val df: DataFrame = spark.read.yt(tmpPath)
    df.schema.fields.map(_.copy(metadata = Metadata.empty)) should contain theSameElementsInOrderAs Seq(
      StructField("id", UInt64Type),
      StructField("date", DateType),
      StructField("datetime", new DatetimeType()),
      StructField("timestamp", TimestampType),
      StructField("number", IntegerType)
    )

    val expectedData = Seq(
      Row(
        UInt64Long(1L),
        Date.valueOf("1970-04-11"),
        Datetime(LocalDateTime.parse("1970-04-11T00:00:00")),
        convertUTCtoLocal("1970-04-11T00:00:00.000000Z", HOURS_OFFSET),
        101
      ),
      Row(
        UInt64Long(2L),
        Date.valueOf("2019-02-09"),
        Datetime(LocalDateTime.parse("2019-02-09T13:41:11")),
        convertUTCtoLocal("2019-02-09T13:41:11.654321Z", HOURS_OFFSET),
        202
      ),
      Row(
        UInt64Long(3L),
        Date.valueOf("1970-01-01"),
        Datetime(LocalDateTime.parse("1970-01-01T00:00:00")),
        convertUTCtoLocal("1970-01-01T00:00:00.000000Z", HOURS_OFFSET),
        303
      )
    )

    df.collect() should contain theSameElementsAs expectedData
  }

  it should "datetime types test: write table by spark - read by yt" in {

    val schemaSpark = StructType(Seq(
      structField("id", LongType, nullable = false),
      structField("date", DateType, nullable = false),
      structField("datetime", new DatetimeType(), nullable = false),
      structField("timestamp", TimestampType, nullable = false),
      structField("number", IntegerType, nullable = false)
    ))

    val writtenBySparkData = ListBuffer[Row]()
    for (i <- ids.indices) {
      writtenBySparkData += Row(
        ids(i),
        Date.valueOf(dateArr(i)),
        Datetime(LocalDateTime.parse(datetimeArr(i).dropRight(1))),
        convertUTCtoLocal(timestampArr(i), HOURS_OFFSET),
        numbers(i)
      )
    }

    val df = spark.createDataFrame(spark.sparkContext.parallelize(writtenBySparkData), schemaSpark)
    df.write.yt(tmpPath)

    val schema = TableSchema.fromYTree(YtWrapper.attribute(tmpPath, "schema"))
    schema.getColumnNames should contain theSameElementsAs Seq("id", "date", "datetime", "timestamp", "number")
    schema.getColumnType(1) shouldEqual ColumnValueType.UINT64
    schema.getColumnType(2) shouldEqual ColumnValueType.UINT64
    schema.getColumnType(3) shouldEqual ColumnValueType.UINT64
    schema.getColumnType(4) shouldEqual ColumnValueType.INT64

    val expectedSchema = TableSchema.builder().setUniqueKeys(false)
      .addValue("id", TiType.optional(TiType.int64()))
      .addValue("date", TiType.optional(TiType.date()))
      .addValue("datetime", TiType.optional(TiType.datetime()))
      .addValue("timestamp", TiType.optional(TiType.timestamp()))
      .addValue("number", TiType.optional(TiType.int32()))
      .build()

    schema shouldEqual expectedSchema

    val data = readTableAsYson(tmpPath).map { yson =>
      val map = yson.asMap()
      (
        map.get("id").longValue(),
        convertDaysToDate(map.get("date").longValue()),
        longToDatetime(map.get("datetime").longValue()),
        longToTimestamp(map.get("timestamp").longValue()),
        map.get("number").intValue()
      )
    }

    val expectedData = ListBuffer[(Long, java.sql.Date, String, String, Int)](
      (1L, Date.valueOf("1970-04-11"), "1970-04-11T00:00:00Z", "1970-04-11T00:00:00.000000Z", 101),
      (2L, Date.valueOf("2019-02-09"), "2019-02-09T13:41:11Z", "2019-02-09T13:41:11.654321Z", 202),
      (3L, Date.valueOf("1970-01-01"), "1970-01-01T00:00:00Z", "1970-01-01T00:00:00.000000Z", 303)
    )

    data should contain theSameElementsAs expectedData
  }
}
