package ru.yandex.spark.yt.common.utils

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.scalatest.{FlatSpec, Matchers}
import ru.yandex.spark.yt.test.LocalSpark

class DataFrameUtilsTest extends FlatSpec with Matchers with LocalSpark {

  behavior of "DataFrameUtilsTest"

  import DataFrameUtils._

  it should "get top from df" in {
    import spark.implicits._

    val df = Seq(
      ("1", "1", "1", "a"),
      ("1", "1", "2", "b"),
      ("1", "1", "3", "c"),
      ("1", "2", "1", "d"),
      ("1", "2", "2", "e"),
      ("2", "3", "1", "f")
    ).
      toDF("user_phone", "brand", "moscow_event_dttm", "target")

    val res = df.top(Seq("user_phone", "brand"), Seq("moscow_event_dttm"), 1)

    res.columns should contain theSameElementsAs df.columns
    res.select(df.columns.head, df.columns.tail: _*).collect() should contain theSameElementsAs Seq(
      Row("1", "1", "1", "a"),
      Row("1", "2", "1", "d"),
      Row("2", "3", "1", "f")
    )
  }

  it should "join with hot key" in {
    import spark.implicits._

    val df = Seq(
      (None, "1"),
      (None, "2"),
      (None, "3"),
      (None, "4"),
      (Some("1"), "5"),
      (Some("2"), "6")
    ).toDF("key", "valueA")

    val df2 = Seq(
      (Some("1"), "7"),
      (Some("2"), "8")
    ).toDF("key", "valueB")

    val res = df.joinWithHotKey(df2, "key", None, "left_outer")

    val expectedColumns = Seq("key", "valueA", "valueB")
    res.columns should contain theSameElementsAs expectedColumns
    res.select(expectedColumns.map(col): _*).collect() should contain theSameElementsAs Seq(
      Row(null, "1", null),
      Row(null, "2", null),
      Row(null, "3", null),
      Row(null, "4", null),
      Row("1", "5", "7"),
      Row("2", "6", "8")
    )
  }

  it should "collect min by column" in {
    import spark.implicits._

    val df = Seq(
      ("1", Some("1"), "a", "aaa", Some("1"), "aa"),
      ("1", Some("2"), "b", "bbb", Some("2"), "bb"),
      ("1", Some("3"), "c", "ccc", Some("3"), "cc"),
      ("1", Some("4"), "d", "ddd", Some("4"), "dd"),
      ("1", Some("5"), "e", "eee", Some("5"), "ee"),
      ("1", None, "f", "fff", None, "ff"),
      ("2", Some("1"), "a", "aaa", None, "aa")
    ).toDF("key", "min_by_key", "min_by_value1", "min_by_value2", "max_by_key", "max_by_value")

    val res = df.minByColumns(
      groupBy = "key",
      minBy = Seq("min_by_key" -> Seq("min_by_key", "min_by_value1", "min_by_value2")),
      maxBy = Seq("max_by_key" -> Seq("max_by_key", "max_by_value")),
      outputSchema = df.schema
    )

    res.columns should contain theSameElementsAs df.columns
    res.select(df.columns.map(col): _*).collect() should contain theSameElementsAs Seq(
      Row("1", "1", "a", "aaa", "5", "ee"),
      Row("2", "1", "a", "aaa", null, "aa")
    )
  }
}
