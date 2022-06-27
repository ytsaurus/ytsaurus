package ru.yandex.spark.metrics

import ru.yandex.spark.metrics.SolomonConfig.Encoding

case class SolomonConfig(
  url: String,
  encoding: Encoding,
  commonLabels: Map[String, String],
  token: Option[String],
  metricNameRegex: String,
  metricNameTransform: Option[String]
)

object SolomonConfig {
  sealed trait Encoding {
    def contentType: String
  }

  case object JsonEncoding extends Encoding {
    override def contentType: String = "application/json"
  }

  case object SpackEncoding extends Encoding {
    override def contentType: String = "application/x-solomon-spack"
  }
}
