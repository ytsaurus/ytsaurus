package org.apache.spark.metrics.yt

import com.codahale.metrics.{Counter, MetricRegistry, Timer}
import org.apache.spark.metrics.source.Source

class YtMetricsSource extends Source {
  override val sourceName: String = "yt"

  override val metricRegistry: MetricRegistry = new MetricRegistry

  val writeTime: Timer = metricRegistry.timer(MetricRegistry.name("write"))

  val writeBatchTime: Timer = metricRegistry.timer(MetricRegistry.name("write.batch"))

  val writeBatchTimeSum: Counter = metricRegistry.counter(MetricRegistry.name("write.batch.sum"))

  val writeTimeSum: Counter = metricRegistry.counter(MetricRegistry.name("write.sum"))

  val writeReadyEventTime: Timer = metricRegistry.timer(MetricRegistry.name("write.readyEvent"))

  val writeReadyEventTimeSum: Counter = metricRegistry.counter(MetricRegistry.name("write.readyEvent.sum"))

  val writeCloseTime: Timer = metricRegistry.timer(MetricRegistry.name("write.close"))

  val writeCloseTimeSum: Counter = metricRegistry.counter(MetricRegistry.name("write.close.sum"))

  val writeFutureWait: Timer = metricRegistry.timer(MetricRegistry.name("write.futureWait"))

  val writeFutureWaitSum: Counter = metricRegistry.counter(MetricRegistry.name("write.futureWait.sum"))

  val readTime: Timer = metricRegistry.timer(MetricRegistry.name("read"))

  val readBatchTime: Timer = metricRegistry.timer(MetricRegistry.name("read.batch"))

  val readBatchTimeSum: Counter = metricRegistry.counter(MetricRegistry.name("read.batch.sum"))

  val readTimeSum: Counter = metricRegistry.counter(MetricRegistry.name("read.sum"))

  val readReadyEventTime: Timer = metricRegistry.timer(MetricRegistry.name("read.readyEvent"))

  val readReadyEventTimeSum: Counter = metricRegistry.counter(MetricRegistry.name("read.readyEvent.sum"))

  val readCloseTime: Timer = metricRegistry.timer(MetricRegistry.name("read.close"))

  val readCloseTimeSum: Counter = metricRegistry.counter(MetricRegistry.name("read.close.sum"))
}
