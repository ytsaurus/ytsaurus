package tech.ytsaurus.spyt.format.types

import tech.ytsaurus.spyt.common.utils.DateTimeTypesConverter._
import org.scalatest.funsuite.AnyFunSuite

import java.sql.Timestamp


class DateTimeTypesConverterTest extends AnyFunSuite {

  /** DATE */

  test("dateToLong should convert date to number of days since epoch") {
    assert(dateToLong("1970-01-01") === 0)
    assert(dateToLong("1970-01-02") === 1)
    assert(dateToLong("2008-02-21") === 13930)
  }

  test("longToDate should convert number of days since epoch to date") {
    assert(longToDate(0) === "1970-01-01")
    assert(longToDate(1) === "1970-01-02")
    assert(longToDate(13930) === "2008-02-21")
  }

  test("dateToLong and longToDate should be inverses") {
    val dates = Seq("1970-01-01", "1999-12-31", "2008-02-21")
    dates.foreach { date =>
      assert(longToDate(dateToLong(date)) === date)
    }
  }

  /** DATETIME */

  test("datetimeToLong should convert timestamp to seconds since epoch") {
    assert(datetimeToLong("1970-01-01T00:00:00Z") === 0)
    assert(datetimeToLong("2019-02-09T13:41:11Z") === 1549719671L)
  }

  test("longToDatetime should convert seconds since epoch to timestamp") {
    assert(longToDatetime(0) === "1970-01-01T00:00:00Z")
    assert(longToDatetime(1549719671) === "2019-02-09T13:41:11Z")
  }

  test("datetimeToLong and longToDatetime should be inverses") {
    val timestamps = Seq("1970-01-01T00:00:00Z", "2019-02-09T13:41:11Z")
    timestamps.foreach { datetime =>
      assert(longToDatetime(datetimeToLong(datetime)) === datetime)
    }
  }

  /** TIMESTAMP */

  test("timestampToLong should convert timestamp string to microseconds since epoch") {
    assert(timestampToLong("1970-01-01T00:00:00.000000Z") === 0L)
    assert(timestampToLong("2019-02-09T13:41:11.000000Z") === 1549719671000000L)
    assert(timestampToLong("1970-01-01T00:00:00.123456Z") === 123456)
    assert(timestampToLong("2019-02-09T13:41:11.654321Z") === 1549719671654321L)
  }

  test("longToTimestamp should convert microseconds since epoch to timestamp string") {
    assert(longToTimestamp(0L) === "1970-01-01T00:00:00.000000Z")
    assert(longToTimestamp(1549719671000000L) === "2019-02-09T13:41:11.000000Z")
    // println(longToTimestamp(123456) + "    " + "1970-01-01T00:00:00.123456Z")
    assert(longToTimestamp(123456) === "1970-01-01T00:00:00.123456Z")
    assert(longToTimestamp(1549719671654321L) === "2019-02-09T13:41:11.654321Z")
  }

  test("timestampToLong and longToTimestamp should be inverses") {
    val timestamps = Seq("1970-01-01T00:00:00.000000Z", "2019-02-09T13:41:11.000000Z", "1970-01-01T00:00:00.123456Z", "2019-02-09T13:41:11.654321Z")
    timestamps.foreach { ts =>
      assert(longToTimestamp(timestampToLong(ts)) === ts)
    }
  }

  test("convertUTCtoLocal should convert time from UTC to local") {
    val dateTimeStr = "1970-04-11T00:00:00Z"
    val expected = Timestamp.valueOf("1970-04-11 06:00:00.0")
    val result = convertUTCtoLocal(dateTimeStr, 6)
    assert(result === expected)
  }

  test("convertLocalToUTC should convert time from local to UTC") {
    val timestamp = Timestamp.valueOf("1970-04-11 06:00:00.0")
    val expected = "1970-04-11T00:00:00Z"
    val result = convertLocalToUTC(timestamp, 6)
    assert(result === expected)
  }
}
