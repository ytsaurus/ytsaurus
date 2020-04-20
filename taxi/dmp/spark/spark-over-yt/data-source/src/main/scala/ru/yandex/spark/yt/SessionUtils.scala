package ru.yandex.spark.yt

import java.util.UUID

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import ru.yandex.spark.yt.fs.YtClientConfigurationConverter.ytClientConfiguration
import ru.yandex.spark.yt.fs.YtClientProvider
import ru.yandex.spark.yt.wrapper.YtJavaConverters._
import ru.yandex.spark.yt.wrapper.YtWrapper
import ru.yandex.yt.ytclient.proxy.YtClient

object SessionUtils {
  private val log = Logger.getLogger(getClass)

  private def parseRemoteConfig(path: String, yt: YtClient): Map[String, String] = {
    import scala.collection.JavaConverters._
    val remoteConfig = toOption(YtWrapper.readDocument(path)(yt).asMap().getO("spark_conf"))
    remoteConfig.map { config =>
      config.asMap().asScala.toMap.mapValues(_.stringValue())
    }.getOrElse(Map.empty)
  }

  def prepareSparkConf(): SparkConf = {
    val conf = new SparkConf()
    val sparkClusterVersion = conf.get("spark.yt.cluster.version")
    val id = s"tmpYtClient-${UUID.randomUUID()}"
    val yt = YtClientProvider.ytClient(ytClientConfiguration(conf), id)
    try {
      val remoteGlobalConfig = parseRemoteConfig(remoteGlobalConfigPath, yt)
      val remoteVersionConfig = parseRemoteConfig(remoteVersionConfigPath(sparkClusterVersion), yt)
      conf.setAll(remoteGlobalConfig)
      conf.setAll(remoteVersionConfig)
    } finally {
      YtClientProvider.close(id)
    }
  }

  def buildSparkSession(sparkConf: SparkConf): SparkSession = {
    val spark = SparkSession.builder.config(sparkConf).getOrCreate()
    log.info(s"SPYT Cluster version: ${sparkConf.get("spark.yt.cluster.version")}")
    log.info(s"SPYT library version: ${sparkConf.get("spark.yt.version")}")
    spark
  }

  private def remoteGlobalConfigPath: String = "//sys/spark/conf/global"

  private def remoteVersionConfigPath(sparkClusterVersion: String): String = {
    val snapshot = Set("SNAPSHOT", "beta")
    val subDir = if (snapshot.exists(sparkClusterVersion.contains)) "snapshots" else "releases"
    s"//sys/spark/conf/$subDir/$sparkClusterVersion/spark-launch-conf"
  }
}
