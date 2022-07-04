package org.apache.spark.metrics.sink

import com.codahale.metrics.{MetricFilter, MetricRegistry}
import com.google.common.net.HostAndPort
import org.apache.spark.SecurityManager
import org.apache.spark.metrics.sink.SolomonSink.{readReporterConfig, readSolomonConfig}
import org.slf4j.LoggerFactory
import ru.yandex.spark.metrics.SolomonConfig.JsonEncoding
import ru.yandex.spark.metrics.{ReporterConfig, SolomonConfig, SolomonReporter, SolomonSinkSettings}
import ru.yandex.spark.yt.fs.conf.PropertiesConf

import java.util.Properties
import java.util.concurrent.TimeUnit
import scala.util.{Failure, Success, Try}

private[spark] case class SolomonSink(props: Properties, registry: MetricRegistry, securityMgr: SecurityManager)
  extends Sink {

  private val log = LoggerFactory.getLogger(SolomonSink.getClass)
  private val reporter: Try[SolomonReporter] = for {
    solomonConfig <- Try(readSolomonConfig(props))
    reporterConfig <- Try(readReporterConfig(props))
  } yield SolomonReporter(registry, solomonConfig, reporterConfig)

  override def start(): Unit = reporter match {
    case Failure(ex) =>
      log.warn("No Solomon metrics available", ex)
    case Success(r) =>
      log.info(s"Starting solomon reporter with ${r.reporterConfig.pollPeriodMillis} millis period")
      r.start(r.reporterConfig.pollPeriodMillis, TimeUnit.MILLISECONDS)
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

private[spark] object SolomonSink {
  private val log = LoggerFactory.getLogger(SolomonSink.getClass)

  def readSolomonConfig(props: Properties): SolomonConfig = {
    val host = props.ytConf(SolomonSinkSettings.SolomonHost)
    val port = props.ytConf(SolomonSinkSettings.SolomonPort)
    val token = props.getYtConf(SolomonSinkSettings.SolomonToken)
    val commonLabels = props.ytConf(SolomonSinkSettings.SolomonCommonLabels)
    val metricNameRegex = props.ytConf(SolomonSinkSettings.SolomonMetricNameRegex)
    val metricNameTransform = Some(props.ytConf(SolomonSinkSettings.SolomonMetricNameTransform)).filter(!_.isBlank)
    //noinspection UnstableApiUsage
    val hostAndPort = HostAndPort.fromParts(host, port)
    log.info(s"Solomon host: $host, port: $port commonLabels=$commonLabels")
    SolomonConfig(
      url = s"http://$hostAndPort",
      encoding = JsonEncoding,
      commonLabels,
      token,
      metricNameRegex,
      metricNameTransform
    )
  }

  def readReporterConfig(props: Properties): ReporterConfig =
      ReporterConfig(
        name = props.ytConf(SolomonSinkSettings.ReporterName),
        filter = MetricFilter.ALL,
        rateUnit = TimeUnit.SECONDS,
        durationUnit = TimeUnit.SECONDS,
        pollPeriodMillis = props.ytConf(SolomonSinkSettings.ReporterPollPeriod).toMillis
      )
}
