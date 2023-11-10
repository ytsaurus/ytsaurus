package tech.ytsaurus.spyt.format.conf

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.internal.SQLConf
import tech.ytsaurus.spyt.fs.conf._

import scala.concurrent.duration.Duration

case class SparkYtWriteConfiguration(miniBatchSize: Int,
                                     batchSize: Int,
                                     dynBatchSize: Int,
                                     timeout: Duration,
                                     typeV3Format: Boolean)

object SparkYtWriteConfiguration {

  import SparkYtConfiguration._

  def apply(sqlc: SQLContext): SparkYtWriteConfiguration = SparkYtWriteConfiguration(
    sqlc.ytConf(Write.MiniBatchSize),
    sqlc.ytConf(Write.BatchSize),
    sqlc.ytConf(Write.DynBatchSize),
    sqlc.ytConf(Write.Timeout),
    sqlc.ytConf(Write.TypeV3)
  )

  def apply(sqlc: SQLConf): SparkYtWriteConfiguration = SparkYtWriteConfiguration(
    sqlc.ytConf(Write.MiniBatchSize),
    sqlc.ytConf(Write.BatchSize),
    sqlc.ytConf(Write.DynBatchSize),
    sqlc.ytConf(Write.Timeout),
    sqlc.ytConf(Write.TypeV3)
  )
}
