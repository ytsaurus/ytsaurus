package tech.ytsaurus.spark.metrics

import com.codahale.metrics.{Gauge, Histogram, Meter, MetricRegistry, ScheduledReporter, Timer, Counter => CCounter}
import org.slf4j.LoggerFactory
import SolomonReporter.STARTUP_THRESHOLD
import tech.ytsaurus.spyt.wrapper.LogLazy

import java.time.{Duration, Instant}
import java.util
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import scala.util.control.NonFatal

case class SolomonReporter(registry: MetricRegistry, solomonConfig: SolomonConfig, reporterConfig: ReporterConfig)
  extends ScheduledReporter(registry, reporterConfig.name, reporterConfig.filter,
    reporterConfig.rateUnit, reporterConfig.durationUnit) with LogLazy {
  private val log = LoggerFactory.getLogger(getClass)
  private val encoder: MetricEncoder = MetricEncoder(solomonConfig)
  private val initAt: Instant = Instant.now()
  private val started: AtomicBoolean = new AtomicBoolean(false)

  def start(): Unit = {
    log.info(s"Starting solomon reporter with ${reporterConfig.pollPeriodMillis} millis period")
    log.debug(s"solomonConfig=$solomonConfig")
    log.debug(s"reporterConfig=$reporterConfig")
    start(reporterConfig.pollPeriodMillis, TimeUnit.MILLISECONDS)
  }

  override def report(gauges: util.SortedMap[String, Gauge[_]],
                      counters: util.SortedMap[String, CCounter],
                      histograms: util.SortedMap[String, Histogram],
                      meters: util.SortedMap[String, Meter],
                      timers: util.SortedMap[String, Timer]): Unit = {
    import sttp.client._
    implicit val backend: SttpBackend[Identity, Nothing, NothingT] = HttpURLConnectionBackend()
    try {
      if (!started.get()) {
        val sinceStart = Duration.between(initAt, Instant.now())
        if (sinceStart.compareTo(STARTUP_THRESHOLD) > 0)
          started.set(true)
      }
      val body = encoder.encode(Instant.now(), gauges, counters, histograms, meters, timers)
      log.debugLazy(s"Reporting metrics to ${solomonConfig.url}: ${new String(body)} (${encoder.contentType}")
      val res = basicRequest.post(uri"${solomonConfig.url}")
        .body(body)
        .contentType(encoder.contentType)
        .response(asStringAlways)
        .send()
      started.set(true)
      if (res.isSuccess)
        log.debugLazy(s"Successfully sent metrics to Solomon: ${res.body}")
      else if (!res.isSuccess)
        log.warn(s"Failed to send metrics to Solomon: ${res.code} ${res.statusText} ${res.body}")
    } catch {
      case NonFatal(ex) =>
        // ignore first several errors, because agent probably not started yet
        if (!started.get())
          log.debugLazy(s"Fail to send metrics to Solomon at ${solomonConfig.url}: ${ex.getMessage}")
        else
          log.error(s"Failed to send metrics to Solomon at ${solomonConfig.url}: ${ex.getMessage}", ex)
    }
  }
}

object SolomonReporter {
  private val STARTUP_THRESHOLD: Duration = Duration.ofMinutes(1)
}
