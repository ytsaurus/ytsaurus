package ru.yandex.spark.e2e.check

import com.twitter.scalding.Args
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import ru.yandex.spark.yt.fs.{YtClientConfigurationConverter, YtFileSystem}
import ru.yandex.spark.yt.yt

import java.net.URI

object CheckApp extends App {
  implicit val spark = SparkSession.builder().getOrCreate()

  val parsedArgs = Args(args)
  val actual = parsedArgs.required("actual")
  val expected = parsedArgs.required("expected")
  val result = parsedArgs.required("result")
  val keyColumns = parsedArgs.required("keys").trim.split(",").map(_.trim).filter(_.nonEmpty)
  val uniqueKeys = parsedArgs.required("uniqueKeys").toBoolean

  val fs: YtFileSystem = new YtFileSystem
  fs.initialize(new URI("yt:///"), spark.sparkContext.hadoopConfiguration)
  val res = CheckUtils.checkDataset(actual, expected, keyColumns, uniqueKeys)(yt, spark)
  res.write(new Path(result))(fs)
}
