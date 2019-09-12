package ru.yandex.spark.yt.utils

import java.time.format.DateTimeFormatter
import java.time.{ZoneId, ZoneOffset, ZonedDateTime}

import org.apache.spark.sql.functions.udf

object DateTimeUtils {
  val format = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  def parseDatetime(dt: String, zoneId: ZoneId = ZoneOffset.UTC): ZonedDateTime = {
    ZonedDateTime.of(
      dt.slice(0, 4).toInt,
      dt.slice(5, 7).toInt,
      dt.slice(8, 10).toInt,
      dt.slice(11, 13).toInt,
      dt.slice(14, 16).toInt,
      dt.slice(17, 19).toInt,
      0,
      zoneId
    )
  }

  val formatDatetime = udf((dt: String) => {
    if (dt == null) {
      null
    } else {
      val dt1 = if (dt.length > 19) dt.take(19) else dt
      format.format(parseDatetime(dt1))
    }
  })

  val emptyStringToNull = udf((str: String) => if (str == null || str.isEmpty) null else str)

  val mskToUtcUdf = udf((dt: String) => if (dt == null) null else mskToUtc(dt))

  def mskToUtc(dt: String): String = {
    DateTimeUtils.format.format(parseDatetime(dt, ZoneId.of("Europe/Moscow")).withZoneSameInstant(ZoneOffset.UTC))
  }

  def time(f: => Unit): Unit = {
    val start = System.currentTimeMillis()
    f
    val end = System.currentTimeMillis()
    println(s"${end - start} ms")
    println(s"${(end - start) / 1000} s")
  }
}
