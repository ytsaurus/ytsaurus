package tech.ytsaurus.spyt.test

import com.twitter.scalding.Args
import io.circe.syntax._
import org.apache.spark.sql.SparkSession
import tech.ytsaurus.spyt.{SparkApp, _}
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.client.YTsaurusClient

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.Try
import tech.ytsaurus.spyt._

object TestConf extends SparkApp {
  spark.read.yt("//sys/spark/examples/example_1").show()

  val confJson = spark.sparkContext.getConf.getAll.filter(_._1 != "spark.hadoop.yt.token").toMap.asJson.spaces4

  val outputPath = Args(args).required("path")
  YtWrapper.createFile(outputPath)(yt)
  val os = YtWrapper.writeFile(outputPath, 5 minutes, None)(yt)
  Try {
    os.write(confJson.getBytes("utf-8"))
  }
  os.close()
}
