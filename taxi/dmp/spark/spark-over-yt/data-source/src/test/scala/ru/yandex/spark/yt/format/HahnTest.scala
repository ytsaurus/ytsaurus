package ru.yandex.spark.yt.format

import org.apache.spark.SparkConf
import org.apache.spark.sql.SaveMode
import org.scalatest.FlatSpec
import ru.yandex.spark.yt.test.LocalSpark
import ru.yandex.spark.yt.wrapper.client.DefaultRpcCredentials
import ru.yandex.spark.yt._

class HahnTest extends FlatSpec with LocalSpark {
  override def sparkConf: SparkConf = super.sparkConf
    .set("spark.hadoop.yt.proxy", "hahn")
    .set("spark.hadoop.yt.user", DefaultRpcCredentials.user)
    .set("spark.hadoop.yt.token", DefaultRpcCredentials.token)
    .set("fs.yt.impl", "ru.yandex.spark.yt.fs.YtFileSystem")
    .set("spark.hadoop.fs.yt.impl", "ru.yandex.spark.yt.fs.YtFileSystem")

  import spark.implicits._

  "HahnTest" should "work" in {
    val df = Seq(1, 1, 2, 2, 3, 3, 3).toDF().repartition(10, 'value)
    spark.sparkContext.setCheckpointDir("yt:///home/sashbel/data/checkpoint")
    df.checkpoint(eager = false)
    df.filter('value > 1).cache()

    df.show()
    df.write.yt("//home/sashbel/data/checkpoint_test")
  }
}
