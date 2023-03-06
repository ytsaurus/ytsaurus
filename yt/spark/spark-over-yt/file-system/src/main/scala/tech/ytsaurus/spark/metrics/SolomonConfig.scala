package tech.ytsaurus.spark.metrics

import org.slf4j.{Logger, LoggerFactory}
import SolomonConfig.Encoding
import tech.ytsaurus.spyt.fs.conf.PropertiesConf
import tech.ytsaurus.spyt.HostAndPort

import java.util.Properties

case class SolomonConfig(
  url: String,
  encoding: Encoding,
  commonLabels: Map[String, String],
  token: Option[String],
  metricNameRegex: String,
  metricNameTransform: Option[String]
)

object SolomonConfig {
  private val log: Logger = LoggerFactory.getLogger(SolomonConfig.getClass)

  sealed trait Encoding {
    def contentType: String
  }

  case object JsonEncoding extends Encoding {
    override def contentType: String = "application/json"
  }

  case object SpackEncoding extends Encoding {
    override def contentType: String = "application/x-solomon-spack"
  }

  def read(props: Properties): SolomonConfig = {
    val host = props.ytConf(SolomonSinkSettings.SolomonHost)
    val port = props.ytConf(SolomonSinkSettings.SolomonPort)
    val token = props.getYtConf(SolomonSinkSettings.SolomonToken)
    val commonLabels = props.ytConf(SolomonSinkSettings.SolomonCommonLabels)
    val metricNameRegex = props.ytConf(SolomonSinkSettings.SolomonMetricNameRegex)
    val metricNameTransform = Some(props.ytConf(SolomonSinkSettings.SolomonMetricNameTransform)).filter(!_.isBlank)
    //noinspection UnstableApiUsage
    val hostAndPort = HostAndPort(host, port)
    log.info(s"Solomon host: $host, port: $port commonLabels=$commonLabels")
    SolomonConfig(
      url = s"http://$hostAndPort",
      encoding = JsonEncoding,
      commonLabels,
      token,
      metricNameRegex,
      metricNameTransform
    )
  }
}
