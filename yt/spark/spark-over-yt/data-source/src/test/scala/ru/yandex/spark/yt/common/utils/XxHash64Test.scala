package ru.yandex.spark.yt.common.utils

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.yson.UInt64Long
import org.scalatest.{FlatSpec, Matchers}
import ru.yandex.spark.yt.common.utils.XxHash64ZeroSeed.{registerFunction, xxHash64ZeroSeedUdf}
import ru.yandex.spark.yt.test.{LocalSpark, TmpDir}

class XxHash64Test extends FlatSpec with Matchers with LocalSpark with TmpDir {
  behavior of "XxHash64Test"
  import spark.implicits._

  it should "compute hash like chyt" in {
    // long hash
    val df = Seq(1L, 2L, 10L).toDF("a")
      .withColumn("a", xxHash64ZeroSeedUdf(col("a")))
    df.collect() should contain theSameElementsAs Seq(
      Row(UInt64Long("11468921228449061269")),
      Row(UInt64Long("16917558970995453360")),
      Row(UInt64Long("1755119922650009378"))
    )

    // string hash
    val df2 = Seq("yandex", "spyt").toDF("a")
      .withColumn("a", xxHash64ZeroSeedUdf(col("a")))
    df2.collect() should contain theSameElementsAs Seq(
      Row(UInt64Long("7757972843861013099")),
      Row(UInt64Long("9811361979167774487"))
    )
  }

  it should "run udf in sql query" in {
    registerFunction(spark)

    // int hash
    val df = spark.sql(s"select xxHash64ZeroSeed(*) from (values (0), (1))")
    df.collect() should contain theSameElementsAs Seq(
      Row(UInt64Long("4246796580750024372")),
      Row(UInt64Long("17595444997414146897"))
    )
  }
}