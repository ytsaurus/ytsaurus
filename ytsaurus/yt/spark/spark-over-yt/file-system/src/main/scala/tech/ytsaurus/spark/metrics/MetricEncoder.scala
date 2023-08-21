package tech.ytsaurus.spark.metrics

import com.codahale.metrics.{Gauge, Histogram, Meter, Metered, Snapshot, Timer, Counter => CCounter}
import org.slf4j.{Logger, LoggerFactory}
import MetricEncoder._

import java.time.Instant
import java.util

private[metrics] trait MetricEncoder {
  def contentType: String
  def solomonConfig: SolomonConfig

  log.debug(s"solomonConfig: $solomonConfig")

  def encode(ts: Instant,
             gauges: util.SortedMap[String, Gauge[_]],
             counters: util.SortedMap[String, CCounter],
             histograms: util.SortedMap[String, Histogram],
             meters: util.SortedMap[String, Meter],
             timers: util.SortedMap[String, Timer]): Array[Byte] = {
    import collection.JavaConverters._

    val metrics = gauges.asScala.flatMap(g => fromGauge(g._1, g._2)).toSeq ++
      counters.asScala.flatMap(c => fromCounter(c._1, c._2)) ++
      histograms.asScala.flatMap(h => fromHistogram(h._1, h._2)) ++
      meters.asScala.flatMap(m => fromMeter(m._1, m._2)) ++
      timers.asScala.flatMap(t => fromTimer(t._1, t._2))

    encodeMetrics(MetricMessage(
      metrics.filter(isRequiredMetric)
        .map(_.updateLabel("sensor", n => transformMetricName(fixName(n)))),
      solomonConfig.commonLabels,
      ts
    ))
  }

  private def isRequiredMetric(metric: Metric): Boolean = {
    val name = metric.labels("sensor")
    name.matches(solomonConfig.metricNameRegex)
  }

  private def transformMetricName(name: String): String =
    solomonConfig.metricNameTransform.map(_.replace("$0", name)).getOrElse(name)

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
    else name.replace('.', '_')


  private def fromGauge[T](name: String, gauge: Gauge[T]): Option[Metric] =
    gauge.getValue match {
      case v: Integer =>
        Some(IGauge(Map("sensor" -> name), v.longValue()))
      case v: Long =>
        Some(IGauge(Map("sensor" -> name), v))
      case v: Float =>
        if (v.isNaN) None
        else Some(DGauge(Map("sensor" -> name), v.doubleValue()))
      case v: Double =>
        if (v.isNaN) None
        else Some(DGauge(Map("sensor" -> name), v))
      case null =>
        log.debug(s"Gauge $name is null")
        None
      case v =>
        log.debug(s"Unsupported gauge type: ${v.getClass} for metric $name")
        None
    }

  private def fromCounter(name: String, counter: CCounter): Option[Metric] =
    Some(Counter(Map("sensor" -> name), counter.getCount))

  private def fromMeter(name: String, meter: Metered): Seq[Metric] =
    Seq(
      Counter(Map("sensor" -> s"$name.count"), meter.getCount),
      DGauge(Map("sensor" -> s"$name.mean_rate"), meter.getMeanRate),
      DGauge(Map("sensor" -> s"$name.rate_1min"), meter.getOneMinuteRate),
      DGauge(Map("sensor" -> s"$name.rate_5min"), meter.getFiveMinuteRate),
      DGauge(Map("sensor" -> s"$name.rate_15min"), meter.getFifteenMinuteRate),
    )

  private def fromHistogram(name: String, hist: Histogram): Seq[Metric] =
    Counter(Map("sensor" -> s"$name.count"), hist.getCount) +: fromSnapshot(name, hist.getSnapshot)

  private def fromSnapshot(name: String, snapshot: Snapshot): Seq[Metric] =
    Seq(
      DGauge(Map("sensor" -> s"$name.mean"), snapshot.getMean),
      DGauge(Map("sensor" -> s"$name.max"), snapshot.getMax),
      DGauge(Map("sensor" -> s"$name.min"), snapshot.getMin),
      DGauge(Map("sensor" -> s"$name.stddev"), snapshot.getStdDev),
      DGauge(Map("sensor" -> s"$name.median"), snapshot.getMedian),
      DGauge(Map("sensor" -> s"$name.p75"), snapshot.get75thPercentile()),
      DGauge(Map("sensor" -> s"$name.p95"), snapshot.get95thPercentile()),
      DGauge(Map("sensor" -> s"$name.p99"), snapshot.get99thPercentile()),
    )

  private def fromTimer(name: String, timer: Timer): Seq[Metric] =
    fromMeter(name, timer) ++ fromSnapshot(name, timer.getSnapshot)

  // Rate, Hist, RateHist modes not supported for push metrics
  // DSummary not implemented in Solomon

}