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

private[spark] case class SolomonSink(props: Properties, registry: MetricRegistry, securityMgr: SecurityManager)
  extends Sink {

  private val solomonConfig: SolomonConfig = readSolomonConfig(props)
  private val reporterConfig: ReporterConfig = readReporterConfig(props)

  private val reporter: SolomonReporter = SolomonReporter(registry, solomonConfig, reporterConfig)

  override def start(): Unit = {
    SolomonSink.log.info(s"Starting SolomonSink with period ${reporterConfig.pollPeriodSeconds} seconds")
    reporter.start(reporterConfig.pollPeriodSeconds, TimeUnit.SECONDS)
  }

  override def stop(): Unit = {
    SolomonSink.log.info(s"Stopping SolomonSink")
    reporter.stop()
  }

  override def report(): Unit = {
    SolomonSink.log.info(s"Report")
    reporter.report()
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
        pollPeriodSeconds = props.ytConf(SolomonSinkSettings.ReporterPollPeriod).toSeconds
      )
}
