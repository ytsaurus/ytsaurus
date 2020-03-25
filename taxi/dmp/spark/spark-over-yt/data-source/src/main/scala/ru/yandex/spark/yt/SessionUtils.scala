package ru.yandex.spark.yt

import java.util.UUID

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import ru.yandex.bolts.collection.{Option => BoltsOption}
import ru.yandex.spark.yt.fs.{YtClientConfigurationConverter, YtClientProvider}
import ru.yandex.spark.yt.utils.YtTableUtils
import ru.yandex.yt.ytclient.proxy.YtClient

object SessionUtils {
  private val log = Logger.getLogger(getClass)

  private def option[T](opt: BoltsOption[T]): Option[T] = {
    if (opt.isPresent) Some(opt.get()) else None
  }

  private def parseRemoteConfig(path: String, yt: YtClient): Map[String, String] = {
    import scala.collection.JavaConverters._
    val remoteConfig = option(YtTableUtils.readDocument(path)(yt).asMap().getO("spark_conf"))
    remoteConfig.map { config =>
      config.asMap().asScala.toMap.mapValues(_.stringValue())
    }.getOrElse(Map.empty)
  }

  def prepareSparkConf(remoteConfigPath: String): SparkConf = {
    val conf = new SparkConf()
    val id = s"tmpYtClient-${UUID.randomUUID()}"
    val yt = YtClientProvider.ytClient(YtClientConfigurationConverter(conf), id)
    try {
      val remoteConfig = parseRemoteConfig(remoteConfigPath, yt)
      conf.setAll(remoteConfig)
    } finally {
      YtClientProvider.close(id)
    }
  }

  def remoteConfigPath: String = "//sys/spark/conf/releases/spark-launch-conf"
}
