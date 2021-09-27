package ru.yandex.spark.yt.format

import org.apache.spark.SparkConf
import org.scalatest.{FlatSpec, Matchers}
import ru.yandex.spark.yt.test.LocalSpark
import ru.yandex.spark.yt.wrapper.client.DefaultRpcCredentials
import ru.yandex.spark.yt._

class HahnTest extends FlatSpec with Matchers with LocalSpark {


  "HahnTest" should "work" in {
    spark.read.yt("")
  }

  override def sparkConf: SparkConf = super.sparkConf
    .set("spark.hadoop.yt.proxy", "hahn")
    .set("spark.hadoop.yt.user", DefaultRpcCredentials.user)
    .set("spark.hadoop.yt.token", DefaultRpcCredentials.token)
}
