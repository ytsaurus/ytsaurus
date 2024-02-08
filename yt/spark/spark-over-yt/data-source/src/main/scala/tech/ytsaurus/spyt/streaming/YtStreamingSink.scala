package tech.ytsaurus.spyt.streaming

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.{DataFrame, SQLContext}
import tech.ytsaurus.spyt.format.YtDynamicTableWriter
import tech.ytsaurus.spyt.format.conf.SparkYtWriteConfiguration
import tech.ytsaurus.spyt.fs.YtClientConfigurationConverter.ytClientConfiguration
import tech.ytsaurus.spyt.wrapper.client.YtClientProvider

import java.util.UUID

class YtStreamingSink(sqlContext: SQLContext,
                      queuePath: String,
                      parameters: Map[String, String]) extends Sink with Logging {
  private val id: String = s"YtStreamingSource-${UUID.randomUUID()}"
  private val yt = YtClientProvider.ytClient(ytClientConfiguration(sqlContext.sparkSession), id)

  // Batch writing must be either atomic or failed.
  private val wConfig = SparkYtWriteConfiguration(sqlContext).copy(dynBatchSize = Int.MaxValue)

  @volatile private var latestBatchId = -1L

  override def toString: String = "YtStreamingSink"

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    if (batchId <= latestBatchId) {
      logInfo(s"Skipping already committed batch $batchId")
    } else {
      val rows = data.collect()
      val dynamicTableWriter = new YtDynamicTableWriter(queuePath, data.schema, wConfig, parameters)(yt)
      rows.foreach(row => dynamicTableWriter.write(row.toSeq))
      dynamicTableWriter.close()  // Atomic flush all rows.
      log.debug("Wrote " + rows.length + " records")
      latestBatchId = batchId
    }
  }
}
