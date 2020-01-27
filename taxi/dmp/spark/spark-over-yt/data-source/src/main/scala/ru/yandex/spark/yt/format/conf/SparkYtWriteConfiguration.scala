package ru.yandex.spark.yt.format.conf

import org.apache.spark.sql.SQLContext
import ru.yandex.spark.yt.fs.conf._

import scala.concurrent.duration.Duration

case class SparkYtWriteConfiguration(miniBatchSize: Int,
                                     batchSize: Int,
                                     timeout: Duration)

object SparkYtWriteConfiguration {

  import SparkYtConfiguration._

  def apply(sqlc: SQLContext): SparkYtWriteConfiguration = SparkYtWriteConfiguration(
    sqlc.ytConf(Write.MiniBatchSize),
    sqlc.ytConf(Write.BatchSize),
    sqlc.ytConf(Write.Timeout)
  )
}
