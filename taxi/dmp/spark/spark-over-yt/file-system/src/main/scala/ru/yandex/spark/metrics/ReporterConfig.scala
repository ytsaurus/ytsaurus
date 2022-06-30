package ru.yandex.spark.metrics

import com.codahale.metrics.MetricFilter

import java.util.concurrent.TimeUnit

case class ReporterConfig(
                           name: String,
                           filter: MetricFilter,
                           rateUnit: TimeUnit,
                           durationUnit: TimeUnit,
                           pollPeriodMillis: Long
)
