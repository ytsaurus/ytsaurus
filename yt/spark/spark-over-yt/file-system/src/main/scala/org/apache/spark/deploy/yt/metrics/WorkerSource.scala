package org.apache.spark.deploy.yt.metrics

import com.codahale.metrics.{Gauge, MetricRegistry}
import io.netty.channel.nio.NioEventLoopGroup
import org.apache.commons.io.FileUtils
import org.apache.spark.deploy.worker.Worker
import org.apache.spark.metrics.source.Source

import java.io.File
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import scala.language.postfixOps

class WorkerSource(val worker: Worker) extends Source {
  private val home = new File(sys.env("HOME"))
  private val tmpfs = new File(home, "tmpfs")

  override val sourceName: String = "worker_custom"

  override val metricRegistry: MetricRegistry = new MetricRegistry()

  private val tmpfsDirSize = new AtomicLong(FileUtils.sizeOfDirectory(tmpfs))
  private val diskUsed = new AtomicLong(FileUtils.sizeOfDirectory(home))

  private val tmpfsTotal = tmpfs.getFreeSpace
  private val tmpfsLimit = tmpfsTotal - (worker.memoryFree.toLong * 1024 * 1024)

  def update(): Unit = {
    tmpfsDirSize.set(FileUtils.sizeOfDirectory(tmpfs))
    diskUsed.set(FileUtils.sizeOfDirectory(home))
  }

  private val eventLoop = new NioEventLoopGroup(1)
  eventLoop.scheduleAtFixedRate(() => update(), 10, 10, TimeUnit.SECONDS)

  metricRegistry.register(MetricRegistry.name("tmpfs_Limit_Used_Bytes"), new Gauge[Long] {
    override def getValue: Long = tmpfsDirSize.get()
  })

  metricRegistry.register(MetricRegistry.name("tmpfs_Limit_Free_Bytes"), new Gauge[Long] {
    override def getValue: Long = tmpfsLimit - tmpfsDirSize.get()
  })

  metricRegistry.register(MetricRegistry.name("tmpfs_Total_Used_Bytes"), new Gauge[Long] {
    override def getValue: Long = tmpfsDirSize.get() + (worker.memoryUsed.toLong * 1024 * 1024)
  })

  metricRegistry.register(MetricRegistry.name("tmpfs_Total_Free_Bytes"), new Gauge[Long] {
    override def getValue: Long = tmpfsTotal - tmpfsDirSize.get() - (worker.memoryUsed.toLong * 1024 * 1024)
  })

  metricRegistry.register(MetricRegistry.name("disk_Used_Bytes"), new Gauge[Long] {
    override def getValue: Long = diskUsed.get() - tmpfsDirSize.get()
  })
}
