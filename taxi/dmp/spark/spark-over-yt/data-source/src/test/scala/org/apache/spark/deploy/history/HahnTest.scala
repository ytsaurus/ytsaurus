package org.apache.spark.deploy.history

import org.apache.spark.SparkConf
import org.scalatest.FlatSpec
import ru.yandex.spark.yt.test.LocalSpark
import ru.yandex.spark.yt.wrapper.client.DefaultRpcCredentials

import scala.concurrent.duration._
import scala.language.postfixOps

class HahnTest extends FlatSpec with LocalSpark {
  override def sparkConf: SparkConf = super.sparkConf
    .set("spark.hadoop.yt.proxy", "hahn")
    .set("spark.hadoop.yt.user", DefaultRpcCredentials.user)
    .set("spark.hadoop.yt.token", DefaultRpcCredentials.token)
    .set("fs.yt.impl", "ru.yandex.spark.yt.fs.YtFileSystem")
    .set("spark.hadoop.fs.yt.impl", "ru.yandex.spark.yt.fs.YtFileSystem")
    .set("spark.history.fs.logDirectory", "yt:///statbox/cube/spark_home/logs/event_log")
    .set("spark.hadoop.yt.proxyRole", "spark")

  "HahnTest" should "work" in {
//    val df = Seq(1, 1, 2, 2, 3, 3, 3).toDF().repartition(10, 'value)
//    spark.sparkContext.setCheckpointDir("yt:///home/sashbel/data/checkpoint")
//    df.checkpoint(eager = false)
//    df.filter('value > 1).cache()
//
//    df.show()
//    df.write.yt("//home/sashbel/data/checkpoint_test")

    val provider = new FsHistoryProvider(sparkConf)
    provider.checkForLogs()

    Thread.sleep((5 minutes).toMillis)
  }
}
