package tech.ytsaurus.spark.metrics

import com.codahale.metrics.MetricFilter

import java.util.Properties
import java.util.concurrent.TimeUnit
import tech.ytsaurus.spyt.fs.conf.PropertiesConf

case class ReporterConfig(
                           enabled: Boolean,
                           name: String,
                           filter: MetricFilter,
                           rateUnit: TimeUnit,
                           durationUnit: TimeUnit,
                           pollPeriodMillis: Long
)

object ReporterConfig {
  def read(props: Properties): ReporterConfig =
    ReporterConfig(
      enabled = props.ytConf(SolomonSinkSettings.ReporterEnabled),
      name = props.ytConf(SolomonSinkSettings.ReporterName),
      filter = MetricFilter.ALL,
      rateUnit = TimeUnit.SECONDS,
      durationUnit = TimeUnit.SECONDS,
      pollPeriodMillis = props.ytConf(SolomonSinkSettings.ReporterPollPeriod).toMillis
    )
}
