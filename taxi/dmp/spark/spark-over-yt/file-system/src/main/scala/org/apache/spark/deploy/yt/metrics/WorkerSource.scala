package org.apache.spark.deploy.yt.metrics

import com.codahale.metrics.{Gauge, MetricRegistry}
import org.apache.commons.io.FileUtils
import org.apache.spark.deploy.worker.Worker
import org.apache.spark.metrics.source.Source

import java.io.File
import scala.language.postfixOps

class WorkerSource(val worker: Worker) extends Source {
  private val home = new File(sys.env("HOME"))
  private val tmpfs = new File(home, "tmpfs")

  override val sourceName: String = "worker_custom"

  override val metricRegistry: MetricRegistry = new MetricRegistry()

  private def tmpfsDirSize = FileUtils.sizeOfDirectory(tmpfs)

  private val tmpfsTotal = tmpfs.getFreeSpace
  private val tmpfsLimit = tmpfsTotal - (worker.memoryFree.toLong * 1024 * 1024)

  private def diskUsed = FileUtils.sizeOfDirectory(home)

  metricRegistry.register(MetricRegistry.name("tmpfs_Limit_Used_Bytes"), new Gauge[Long] {
    override def getValue: Long = tmpfsDirSize
  })

  metricRegistry.register(MetricRegistry.name("tmpfs_Limit_Free_Bytes"), new Gauge[Long] {
    override def getValue: Long = tmpfsLimit - tmpfsDirSize
  })

  metricRegistry.register(MetricRegistry.name("tmpfs_Total_Used_Bytes"), new Gauge[Long] {
    override def getValue: Long = tmpfsDirSize + (worker.memoryUsed.toLong * 1024 * 1024)
  })

  metricRegistry.register(MetricRegistry.name("tmpfs_Total_Free_Bytes"), new Gauge[Long] {
    override def getValue: Long = tmpfsTotal - tmpfsDirSize - (worker.memoryUsed.toLong * 1024 * 1024)
  })

  metricRegistry.register(MetricRegistry.name("disk_Used_Bytes"), new Gauge[Long] {
    override def getValue: Long = diskUsed - tmpfsDirSize
  })

  metricRegistry.register(MetricRegistry.name("disk_Free_Bytes"), new Gauge[Long] {
    override def getValue: Long = home.getFreeSpace
  })
}
