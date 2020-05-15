package ru.yandex.spark.test

import com.twitter.scalding.Args
import io.circe.syntax._
import org.apache.spark.sql.SparkSession
import ru.yandex.spark.yt.{SparkApp, _}
import ru.yandex.spark.yt.wrapper.YtWrapper
import ru.yandex.yt.ytclient.proxy.YtClient

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Try

object TestConf extends SparkApp {
  override def run(args: Array[String])
                  (implicit spark: SparkSession, yt: YtClient): Unit = {
    spark.read.yt("//sys/spark/examples/example_1").show()

    val confJson = spark.sparkContext.getConf.getAll.filter(_._1 != "spark.hadoop.yt.token").toMap.asJson.spaces4

    val outputPath = Args(args).required("path")
    YtWrapper.createFile(outputPath)
    val os = YtWrapper.writeFile(outputPath, 5 minutes, None)
    Try {
      os.write(confJson.getBytes("utf-8"))
    }
    os.close()
  }
}
