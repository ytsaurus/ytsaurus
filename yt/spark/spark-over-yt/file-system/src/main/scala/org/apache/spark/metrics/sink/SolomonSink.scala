package org.apache.spark.metrics.sink

import com.codahale.metrics.MetricRegistry
import org.apache.spark.SecurityManager
import org.slf4j.LoggerFactory
import tech.ytsaurus.spark.metrics.ReporterConfig
import tech.ytsaurus.spark.metrics.{ReporterConfig, SolomonConfig, SolomonReporter}

import java.util.Properties
import scala.util.{Failure, Success, Try}

private[this] case class SolomonSink(props: Properties, registry: MetricRegistry, securityMgr: SecurityManager)
  extends Sink {

  private val log = LoggerFactory.getLogger(SolomonSink.getClass)
  private val reporter: Try[SolomonReporter] = for {
    solomonConfig <- Try(SolomonConfig.read(props))
    reporterConfig <- Try(ReporterConfig.read(props))
    reporter <- SolomonReporter.tryCreateSolomonReporter(registry, solomonConfig, reporterConfig)
  } yield reporter

  override def start(): Unit = reporter match {
    case Failure(ex) =>
      log.info("No Solomon metrics available", ex)
    case Success(r) =>
      r.start()
  }

  override def stop(): Unit = reporter.foreach { r =>
    log.info(s"Stopping SolomonSink")
    r.stop()
  }

  override def report(): Unit = reporter.foreach { r =>
    log.debug(s"Sending report")
    r.report()
  }
}
