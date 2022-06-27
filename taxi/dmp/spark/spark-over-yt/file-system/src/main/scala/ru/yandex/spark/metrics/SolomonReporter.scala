package ru.yandex.spark.metrics

import com.codahale.metrics.{Gauge, Histogram, Meter, MetricRegistry, ScheduledReporter, Timer, Counter => CCounter}
import org.slf4j.LoggerFactory
import ru.yandex.spark.yt.wrapper.LogLazy

import java.time.Instant
import java.util
import scala.util.control.NonFatal

case class SolomonReporter(registry: MetricRegistry, solomonConfig: SolomonConfig, reporterConfig: ReporterConfig)
  extends ScheduledReporter(registry, reporterConfig.name, reporterConfig.filter,
    reporterConfig.rateUnit, reporterConfig.durationUnit) with LogLazy {
  private val log = LoggerFactory.getLogger(getClass)
  private val encoder: MetricEncoder = MetricEncoder(solomonConfig)


  override def report(gauges: util.SortedMap[String, Gauge[_]],
                      counters: util.SortedMap[String, CCounter],
                      histograms: util.SortedMap[String, Histogram],
                      meters: util.SortedMap[String, Meter],
                      timers: util.SortedMap[String, Timer]): Unit = {
    import sttp.client._
    implicit val backend: SttpBackend[Identity, Nothing, NothingT] = HttpURLConnectionBackend()
    try {
      val body = encoder.encode(Instant.now(), gauges, counters, histograms, meters, timers)
      log.debugLazy(s"Reporting metrics to ${solomonConfig.url}: ${new String(body)} (${encoder.contentType}")
      val res = basicRequest.post(uri"${solomonConfig.url}")
        .body(body)
        .contentType(encoder.contentType)
        .response(asStringAlways)
        .send()
      if (res.isSuccess)
        log.debugLazy(s"Successfully sent metrics to Solomon: ${res.body}")
      else if (!res.isSuccess)
        log.warn(s"Fail to send metrics to Solomon: ${res.code} ${res.statusText} ${res.body}")
    } catch {
      case NonFatal(ex) =>
        log.error(s"Fail to send metrics to Solomon at ${solomonConfig.url}: ${ex.getMessage}", ex)
    }
  }
}
