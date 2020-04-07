package ru.yandex.spark.yt.common.utils

import org.apache.spark.sql.Row
import org.scalatest.{FlatSpec, Matchers}
import ru.yandex.spark.yt.test.LocalSpark

class TopUdafTest extends FlatSpec with Matchers with LocalSpark {
  import spark.implicits._

  "TopUdaf" should "work" in {
    val df = Seq(
      (1, "a", "A"),
      (1, "b", "B"),
      (1, "c", "C"),
      (2, "b", "B")
    ).toDF("a", "b", "c").repartition(4)
    val top = new TopUdaf(df.schema, Seq("b"))

    val result = df.groupBy('a).agg(top('a, 'b, 'c) as "top").selectExpr("top.*")
    result.collect() should contain theSameElementsAs Seq(
      Row(1, "a", "A"),
      Row(2, "b", "B")
    )
  }

  it should "work in top wrapper" in {
    import TopUdaf._
    val df = Seq(
      (1, "a", "A"),
      (1, "b", "B"),
      (1, "c", "C"),
      (2, "b", "B")
    ).toDF("a", "b", "c").repartition(4)

    val result = df.groupBy('a)
      .agg(
        top(df.schema, Seq("b"), Seq("a", "b", "c")) as "top"
      )
      .selectExpr("top.*")
    result.collect() should contain theSameElementsAs Seq(
      Row(1, "a", "A"),
      Row(2, "b", "B")
    )
  }

  it should "work for several top columns" in {
    import TopUdaf._
    val df = Seq(
      (1, "a", "A", "10"),
      (1, "a", "B", "11"),
      (1, "a", "C", "12"),
      (1, "b", "B", "13"),
      (1, "c", "C", "14"),
      (2, "b", "B", "15")
    ).toDF("a", "b", "c", "d").repartition(4)

    val result = df.groupBy('a)
      .agg(
        top(df.schema, Seq("b", "c"), Seq("a", "b", "c", "d")) as "top"
      )
      .selectExpr("top.*")
    result.collect() should contain theSameElementsAs Seq(
      Row(1, "a", "A", "10"),
      Row(2, "b", "B", "15")
    )
  }
}
