package ru.yandex.spark.yt.wrapper.config

import ru.yandex.spark.yt.wrapper.YtJavaConverters.RichJavaMap
import ru.yandex.spark.yt.wrapper.YtWrapper
import ru.yandex.yt.ytclient.proxy.CompoundClient

import scala.collection.JavaConverters.mapAsScalaMapConverter

object Utils {
  def parseRemoteConfig(path: String, yt: CompoundClient): Map[String, String] = {
    val remoteConfig = YtWrapper.readDocument(path)(yt).asMap().getOption("spark_conf")
    remoteConfig.map { config =>
      config.asMap().asScala.toMap.mapValues(_.stringValue())
    }.getOrElse(Map.empty)
  }

  def remoteGlobalConfigPath: String = "//home/spark/conf/global"

  def remoteVersionConfigPath(sparkClusterVersion: String): String = {
    val snapshot = Set("SNAPSHOT", "beta", "dev")
    val subDir = if (snapshot.exists(sparkClusterVersion.contains)) "snapshots" else "releases"
    s"//home/spark/conf/$subDir/$sparkClusterVersion/spark-launch-conf"
  }
}
