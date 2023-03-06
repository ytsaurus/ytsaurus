package tech.ytsaurus.spark.metrics

import io.circe.{Encoder, Json}
import io.circe.generic.semiauto.deriveEncoder
import io.circe.syntax.EncoderOps
import JsonMetricEncoder.encoder

import java.time.Instant

case class JsonMetricEncoder(override val solomonConfig: SolomonConfig) extends MetricEncoder {
  override def contentType: String = solomonConfig.encoding.contentType
  override def encodeMetrics(message: MetricMessage): Array[Byte] = message.asJson.noSpaces.getBytes
}

private object JsonMetricEncoder {
  implicit val instantEncoder: Encoder[Instant] = Encoder.encodeLong.contramap(_.getEpochSecond)

  implicit val metricEncoder: Encoder[Metric] = Encoder.instance {
    case m @ IGauge(labels, value) =>
      Json.obj(
        "labels" -> labels.asJson,
        "type" -> m.`type`.asJson,
        "value" -> value.asJson
      )

    case m @ DGauge(labels, value) =>
      Json.obj(
        "labels" -> labels.asJson,
        "type" -> m.`type`.asJson,
        "value" -> value.asJson
      )

    case m @ Counter(labels, value) =>
      Json.obj(
        "labels" -> labels.asJson,
        "type" -> m.`type`.asJson,
        "value" -> value.asJson
      )
  }

  implicit val encoder: Encoder[MetricMessage] = deriveEncoder
}
