package tech.ytsaurus.spark.metrics

import java.time.Instant


sealed abstract class Metric(val labels: Map[String, String], val `type`: String) {
  def updateLabel(name: String, updateValue: String => String): Metric
}

case class DGauge(override val labels: Map[String, String], value: Double) extends Metric(labels, "DGAUGE") {
  override def updateLabel(name: String, updateValue: String => String): DGauge =
    copy(labels = labels.updated(name, updateValue(labels(name))))
}

case class IGauge(override val labels: Map[String, String], value: Long) extends Metric(labels, "IGAUGE") {
  override def updateLabel(name: String, updateValue: String => String): IGauge =
    copy(labels = labels.updated(name, updateValue(labels(name))))
}

case class Counter(override val labels: Map[String, String], value: Long) extends Metric(labels, "COUNTER") {
  override def updateLabel(name: String, updateValue: String => String): Counter =
    copy(labels = labels.updated(name, updateValue(labels(name))))
}

case class MetricMessage(metrics: Seq[Metric], commonLabels: Map[String, String], ts: Instant)
