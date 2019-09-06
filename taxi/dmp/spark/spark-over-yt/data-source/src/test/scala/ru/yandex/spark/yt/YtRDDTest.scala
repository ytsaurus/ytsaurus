package ru.yandex.spark.yt

import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.scalatest.{FlatSpec, Matchers}

class YtRDDTest extends FlatSpec with Matchers with LocalSpark {

  behavior of "YtRDDTest"

  import DefaultRpcCredentials._

  val schema = StructType(Seq(
    StructField("event_type", StringType),
    StructField("timestamp", StringType),
    StructField("id", LongType)
  ))

  it should "getPartitions" ignore {
    val rdd = new YtRDD(spark.sparkContext, "//tmp/sashbel", "hume", user, token, 3, schema, Array.empty)
    println(rdd.partitions.toSeq)
  }

  it should "compute" ignore {
    val rdd = new YtRDD(spark.sparkContext, "//tmp/sashbel", "hume", user, token, 3, schema, Array.empty)
    rdd.compute(YtPartition(0, 1, 2), null).foreach(println)
  }

  it should "compute unique_drivers" ignore {
    val df = spark.read.option("proxy", "hume").option("partitions", 1000).yt("//home/taxi-dwh/stg/mdb/unique_drivers/unique_drivers")
    val rdd = new YtRDD(spark.sparkContext, "//home/taxi-dwh/stg/mdb/unique_drivers/unique_drivers", "hume", user, token, 100, df.schema, Array.empty)
    rdd.compute(YtPartition(0, 0, 10), null).foreach(println)
  }

  it should "collect" ignore {
    val rdd = new YtRDD(spark.sparkContext, "//tmp/sashbel", "hume", user, token, 3, schema, Array.empty)
    println(rdd.collect().toSeq)
  }

}
