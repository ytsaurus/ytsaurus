package tech.ytsaurus.spyt

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf.FILES_MAX_PARTITION_BYTES
import org.slf4j.LoggerFactory
import tech.ytsaurus.spyt.fs.YtClientConfigurationConverter.ytClientConfiguration
import tech.ytsaurus.spyt.wrapper.config.Utils.{parseRemoteConfig, remoteGlobalConfigPath, remoteVersionConfigPath}
import tech.ytsaurus.spyt.wrapper.YtJavaConverters._
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.wrapper.client.YtClientProvider

import java.util.UUID

object SessionUtils {
  private val log = LoggerFactory.getLogger(getClass)

  implicit class RichSparkConf(conf: SparkConf) {
    def setEnablers(enablers: Map[String, String]): SparkConf = {
      enablers.foldLeft(conf) { case (res, (key, value)) =>
        if (res.contains(key)) res.set(key, (res.get(key).toBoolean && value.toBoolean).toString) else res
      }
    }

    def setAllNoOverride(settings: Map[String, String]): SparkConf = {
      settings.foldLeft(conf) { case (res, (key, value)) =>
        if (!res.contains(key)) res.set(key, value) else res
      }
    }
  }

  private[ytsaurus] def mergeConfs(conf: SparkConf, remoteGlobalConfig: Map[String, String],
                                   remoteVersionConfig: Map[String, String], remoteClusterConfig: Map[String, String],
                                   enablerMap: Map[String, String]): SparkConf = {
    conf
      .setAllNoOverride(remoteClusterConfig)
      .setAllNoOverride(remoteVersionConfig)
      .setAllNoOverride(remoteGlobalConfig)
      .setEnablers(enablerMap)
  }

  def prepareSparkConf(): SparkConf = {
    val conf = new SparkConf()
    val sparkClusterVersion = conf.get("spark.yt.cluster.version")
    val sparkClusterConfPath = conf.getOption("spark.yt.cluster.confPath")
    val id = s"tmpYtClient-${UUID.randomUUID()}"
    val yt = YtClientProvider.ytClient(ytClientConfiguration(conf), id)
    try {
      val remoteGlobalConfig = parseRemoteConfig(remoteGlobalConfigPath, yt)
      val remoteVersionConfig = parseRemoteConfig(remoteVersionConfigPath(sparkClusterVersion), yt)
      val remoteClusterConfig = sparkClusterConfPath.map(parseRemoteConfig(_, yt)).getOrElse(Map.empty[String, String])
      val enablerMap = parseRemoteConfig(remoteVersionConfigPath(sparkClusterVersion), yt, "enablers")
      mergeConfs(conf, remoteGlobalConfig, remoteVersionConfig, remoteClusterConfig, enablerMap)
    } finally {
      YtClientProvider.close(id)
    }
  }

  //TODO: rethink and refactor due to submission to YT scheduler
  def buildSparkSession(sparkConf: SparkConf): SparkSession = {
    val spark = SparkSession.builder.config(sparkConf).getOrCreate()
    log.info(s"SPYT Cluster version: ${sparkConf.get("spark.yt.cluster.version")}")
    log.info(s"SPYT library version: ${sparkConf.get("spark.yt.version")}")
    spark
  }
}
