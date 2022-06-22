package ru.yandex.spark.metrics

import com.codahale.metrics.{Gauge, Histogram, Meter, Metered, Snapshot, Timer, Counter => CCounter}
import org.slf4j.{Logger, LoggerFactory}
import ru.yandex.spark.metrics.MetricEncoder._

import java.time.Instant
import java.util

private[metrics] trait MetricEncoder {
  def contentType: String
  def solomonConfig: SolomonConfig

  def encode(ts: Instant,
             gauges: util.SortedMap[String, Gauge[_]],
             counters: util.SortedMap[String, CCounter],
             histograms: util.SortedMap[String, Histogram],
             meters: util.SortedMap[String, Meter],
             timers: util.SortedMap[String, Timer]): Array[Byte] = {
    import collection.JavaConverters._

    val metrics = gauges.asScala.flatMap(g => fromGauge(fixName(g._1), g._2)).toSeq ++
      counters.asScala.flatMap(c => fromCounter(fixName(c._1), c._2)) ++
      histograms.asScala.flatMap(h => fromHistogram(fixName(h._1), h._2)) ++
      meters.asScala.flatMap(m => fromMeter(fixName(m._1), m._2)) ++
      timers.asScala.flatMap(t => fromTimer(fixName(t._1), t._2))

    encodeMetrics(MetricMessage(metrics, solomonConfig.commonLabels, ts))
  }

  def encodeMetrics(message: MetricMessage): Array[Byte]
}

private[metrics] object MetricEncoder {
  private val log: Logger = LoggerFactory.getLogger(getClass)

  def apply(solomonConfig: SolomonConfig): MetricEncoder =
    solomonConfig.encoding match {
      case SolomonConfig.JsonEncoding => JsonMetricEncoder(solomonConfig)
      case SolomonConfig.SpackEncoding => throw new IllegalArgumentException("Spack format is not yet supported")
    }

  private def fixName(name: String): String =
    if (name == "project") "project_label"
    else if (name == "cluster") "host_label"
    else if (name == "service") "service_label"
    else name


  private def fromGauge[T](name: String, gauge: Gauge[T]): Option[Metric] =
    gauge.getValue match {
      case v: Integer =>
        Some(IGauge(Map("name" -> name), v.longValue()))
      case v: Long =>
        Some(IGauge(Map("name" -> name), v))
      case v: Float =>
        if (v.isNaN) None
        else Some(DGauge(Map("name" -> name), v.doubleValue()))
      case v: Double =>
        if (v.isNaN) None
        else Some(DGauge(Map("name" -> name), v))
      case null =>
        log.debug(s"Gauge $name is null")
        None
      case v =>
        log.warn(s"Unsupported gauge type: ${v.getClass} for metric $name")
        None
    }

  private def fromCounter(name: String, counter: CCounter): Option[Metric] =
    Some(Counter(Map("name" -> name), counter.getCount))

  private def fromMeter(name: String, meter: Metered): Seq[Metric] =
    Seq(
      Counter(Map("name" -> s"$name.count"), meter.getCount),
      DGauge(Map("name" -> s"$name.mean_rate"), meter.getMeanRate),
      DGauge(Map("name" -> s"$name.rate_1min"), meter.getOneMinuteRate),
      DGauge(Map("name" -> s"$name.rate_5min"), meter.getFiveMinuteRate),
      DGauge(Map("name" -> s"$name.rate_15min"), meter.getFifteenMinuteRate),
    )

  private def fromHistogram(name: String, hist: Histogram): Seq[Metric] =
    Counter(Map("name" -> s"$name.count"), hist.getCount) +: fromSnapshot(name, hist.getSnapshot)

  private def fromSnapshot(name: String, snapshot: Snapshot): Seq[Metric] =
    Seq(
      DGauge(Map("name" -> s"$name.mean"), snapshot.getMean),
      DGauge(Map("name" -> s"$name.max"), snapshot.getMax),
      DGauge(Map("name" -> s"$name.min"), snapshot.getMin),
      DGauge(Map("name" -> s"$name.stddev"), snapshot.getStdDev),
      DGauge(Map("name" -> s"$name.median"), snapshot.getMedian),
      DGauge(Map("name" -> s"$name.p75"), snapshot.get75thPercentile()),
      DGauge(Map("name" -> s"$name.p95"), snapshot.get95thPercentile()),
      DGauge(Map("name" -> s"$name.p99"), snapshot.get99thPercentile()),
    )

  private def fromTimer(name: String, timer: Timer): Seq[Metric] =
    fromMeter(name, timer) ++ fromSnapshot(name, timer.getSnapshot)

  // Rate, Hist, RateHist modes not supported for push metrics
  // DSummary not implemented in Solomon

}