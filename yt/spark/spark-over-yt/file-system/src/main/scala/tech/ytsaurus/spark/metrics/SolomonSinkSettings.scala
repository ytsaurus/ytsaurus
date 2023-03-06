package tech.ytsaurus.spark.metrics

import tech.ytsaurus.spyt.fs.conf.StringMapConfigEntry
import tech.ytsaurus.spyt.fs.conf.{DurationSecondsConfigEntry, IntConfigEntry, StringConfigEntry, StringMapConfigEntry}

import scala.concurrent.duration.DurationInt

case object SolomonSinkSettings {
  case object SolomonHost extends StringConfigEntry("solomon_host", Some("[::1]"))
  case object SolomonPort extends IntConfigEntry("solomon_port",
    Option(System.getenv("SOLOMON_PUSH_PORT")).map(_.toInt))
  case object SolomonToken extends StringConfigEntry("solomon_token", None)
  case object SolomonCommonLabels extends StringMapConfigEntry("common_labels", Some(Map()))
  case object SolomonMetricNameRegex extends StringConfigEntry("accept_metrics", Some(".*"))
  case object SolomonMetricNameTransform extends StringConfigEntry("rename_metrics", Some(""))

  case object ReporterPollPeriod extends DurationSecondsConfigEntry("poll_period",
    Some(500.milliseconds))
  case object ReporterName extends StringConfigEntry("reporter_name", Some("spyt"))
}
