package ru.yandex.spark.metrics

import java.time.Instant


sealed abstract class Metric(val labels: Map[String, String], val `type`: String)

case class DGauge(override val labels: Map[String, String], value: Double) extends Metric(labels, "DGAUGE")
case class IGauge(override val labels: Map[String, String], value: Long) extends Metric(labels, "IGAUGE")
case class Counter(override val labels: Map[String, String], value: Long) extends Metric(labels, "COUNTER")

case class MetricMessage(metrics: Seq[Metric], commonLabels: Map[String, String], ts: Instant)
