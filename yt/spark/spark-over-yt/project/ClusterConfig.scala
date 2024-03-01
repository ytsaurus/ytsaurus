package spyt

import sbt._
import spyt.SparkPaths._
import spyt.YtPublishPlugin.autoImport._

import java.io.{FileInputStream, InputStreamReader}
import java.util.Properties

object ClusterConfig {
  def sidecarConfigs(baseConfigDir: File): Seq[File] = {
    (baseConfigDir / "sidecar-config").listFiles()
  }

  def innerSidecarConfigs(baseConfigDir: File): Seq[File] = {
    (baseConfigDir / "inner-sidecar-config").listFiles()
  }

  def launchConfig(version: String, sidecarConfigs: Seq[File]): SparkLaunchConfig = {
    val clusterBasePath = versionPath(spytPath, version)
    val versionConfPath = versionPath(sparkYtConfPath, version)
    val sidecarConfigsClusterPaths = sidecarConfigs.map(file => s"$versionConfPath/${file.getName}")
    SparkLaunchConfig(
      clusterBasePath,
      spark_conf = Map(
        "spark.yt.version" -> version,
        "spark.hadoop.yt.byop.enabled" -> "false",
        "spark.hadoop.yt.read.arrow.enabled" -> "true",
        "spark.hadoop.yt.profiling.enabled" -> "false",
        "spark.hadoop.yt.mtn.enabled" -> "false",
        "spark.hadoop.yt.solomonAgent.enabled" -> "true",
        "spark.hadoop.yt.preferenceIpv6.enabled" -> "true",
        "spark.hadoop.yt.tcpProxy.enabled" -> "false"
      ),
      ytserver_proxy_path = Option(System.getProperty("proxyVersion")).map(version =>
        s"$defaultYtServerProxyPath-$version"
      ),
      file_paths = Seq(
        s"$clusterBasePath/spark.tgz",
        s"$clusterBasePath/spyt-package.zip",
        s"$clusterBasePath/setup-spyt-env.sh",
      ) ++ sidecarConfigsClusterPaths
    )
  }

  def globalConfig(log: sbt.Logger, version: String, baseConfigDir: File): Seq[(String, SparkGlobalConfig)] = {
    val isSnapshot = isSnapshotVersion(version)
    if (!isSnapshot) {
      log.info(s"Prepare configs for ${ytProxies.mkString(", ")}")
      ytProxies.map { proxy =>
        val proxyShort = proxy.split("\\.").head
        val proxyDefaultsFile = baseConfigDir / "spark-defaults" / s"spark-defaults-$proxyShort.conf"
        val proxyDefaults = readSparkDefaults(proxyDefaultsFile)
        val globalConfig = SparkGlobalConfig(proxyDefaults, version)

        (proxy, globalConfig)
      }
    } else Nil
  }

  def artifacts(log: sbt.Logger, version: String, baseConfigDir: File): Seq[YtPublishArtifact] = {
    val isSnapshot = isSnapshotVersion(version)
    val versionConfPath = versionPath(sparkYtConfPath, version)
    val isTtlLimited = isSnapshot && limitTtlEnabled

    val sidecarConfigsFiles = if (innerSidecarConfigEnabled) {
      spyt.ClusterConfig.innerSidecarConfigs(baseConfigDir)
    } else {
      spyt.ClusterConfig.sidecarConfigs(baseConfigDir)
    }
    val launchConfigYson = launchConfig(version, sidecarConfigsFiles)
    val globalConfigYsons = globalConfig(log, version, baseConfigDir)

    val launchConfigPublish = YtPublishDocument(
      launchConfigYson, versionConfPath, None, "spark-launch-conf", isTtlLimited
    )
    val configsPublish = sidecarConfigsFiles.map(
      file => YtPublishFile(file, versionConfPath, None, isTtlLimited = isTtlLimited)
    )
    val globalConfigPublish = globalConfigYsons.map {
      case (proxy, config) => YtPublishDocument(config, sparkYtConfPath, Some(proxy), "global", isTtlLimited)
    }

    configsPublish ++ (launchConfigPublish +: globalConfigPublish)
  }

  private def readSparkDefaults(file: File): Map[String, String] = {
    import scala.collection.JavaConverters._
    val reader = new InputStreamReader(new FileInputStream(file))
    val properties = new Properties()
    try {
      properties.load(reader)
    } finally {
      reader.close()
    }
    properties.stringPropertyNames().asScala.map { name =>
      name -> properties.getProperty(name)
    }.toMap
  }
}
