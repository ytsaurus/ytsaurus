package ru.yandex.spark.yt.format

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.scalatest.{FlatSpec, Matchers}
import ru.yandex.spark.yt.test.LocalSpark
import org.apache.spark.sql.functions._

class DeleteLogs extends FlatSpec with Matchers with LocalSpark {
    "D" should "work" ignore {
      val path = new Path("ytEventLog:///home/taxi-crm/production/spark/logs/event_log_table/app-20211004180107-1917")
      val fs = FileSystem.get(path.toUri, spark.sparkContext.hadoopConfiguration)

      fs.delete(path, false)
    }

    override def sparkConf: SparkConf = super.sparkConf
      .clone()
      .set("spark.hadoop.fs.ytEventLog.impl", "ru.yandex.spark.yt.fs.eventlog.YtEventLogFileSystem")

}


