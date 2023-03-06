package tech.ytsaurus.spark.metrics

import com.codahale.metrics.{Gauge, MetricRegistry}
import org.slf4j.{Logger, LoggerFactory}

import java.io.File
import java.nio.file.{Files, Path}
import scala.io.Source
import scala.util.control.NonFatal

object AdditionalMetrics {
  private val log: Logger = LoggerFactory.getLogger(getClass)
  // not portable, works in yt environment, could be broken in other cases
  private val deviceName = "^/dev/(\\D+?)\\d+$".r

  private def registerDiskMetrics(registry: MetricRegistry, prefix: String): Unit = {
    log.info(s"Registering disk metrics for $prefix")
    registry.register(s"$prefix.total_disk", new Gauge[Long]() {
      override def getValue: Long = totalDiskSpaceBytes
    })
    registry.register(s"$prefix.available_disk", new Gauge[Long]() {
      override def getValue: Long = availableDiskSpaceBytes
    })
    val ssd = isSsdDisk
    log.info(s"Running on ssd disk: $ssd")
    registry.register(s"$prefix.total_ssd", new Gauge[Long]() {
      override def getValue: Long = if (ssd) totalDiskSpaceBytes else 0
    })
    registry.register(s"$prefix.available_ssd", new Gauge[Long]() {
      override def getValue: Long = if (ssd) availableDiskSpaceBytes else 0
    })
  }

  def register(registry: MetricRegistry, instance: String): Unit = {
    registerDiskMetrics(registry, instance)
  }

  def totalDiskSpaceBytes: Long = currentDir.getTotalSpace

  def availableDiskSpaceBytes: Long = currentDir.getUsableSpace

  def isSsdDisk: Boolean =
    try {
      val currentPath = currentDir.toPath
      val device = deviceName(currentPath)
      log.info(s"Detected device name is $device")
      isSsd(device)
    } catch {
      case NonFatal(ex) =>
        log.error(s"Unable to detect disk type, assuming it is hdd", ex)
        false
    }

  def currentDir: File = new File(".")

  private def deviceName(path: Path): String = {
    val absPath = path.toAbsolutePath
    val fsName = Files.getFileStore(absPath).name()
    fsName match {
      case deviceName(p) => p
      case _ => // would probably fail with exception
        absPath.getRoot.resolve(fsName).toRealPath().getFileName.toString
    }
  }

  private def isSsd(devName: String): Boolean = {
    import scala.collection.JavaConverters._
    val sysinfo = new File("/").toPath.getRoot.resolve("sys").resolve("block")
    val d = Files.newDirectoryStream(sysinfo).asScala
      .filter(d => d.getFileName.startsWith(devName))
      .toSeq
      .maxBy(_.toString.length)
    val path = d.resolve("queue").resolve("rotational")
    val s = Source.fromFile(path.toFile)
    try {
      s.getLines().mkString.trim == "0"
    } finally s.close()
  }
}
