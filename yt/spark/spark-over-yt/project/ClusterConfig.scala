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

  def launchConfig(version: String, sidecarConfigs: Seq[File]): SparkLaunchConfig = {
    val clusterBasePath = versionPath(sparkYtClusterPath, version)
    val versionConfPath = versionPath(sparkYtConfPath, version)
    val sidecarConfigsClusterPaths = sidecarConfigs.map(file => s"$versionConfPath/${file.getName}")
    SparkLaunchConfig(
      clusterBasePath,
      ytserver_proxy_path = Option(System.getProperty("proxyVersion")).map(version =>
        s"$defaultYtServerProxyPath-$version"
      ),
      file_paths = Seq(
        s"$clusterBasePath/spark.tgz",
        s"$clusterBasePath/spark-yt-launcher.jar"
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

    val sidecarConfigsFiles = sidecarConfigs(baseConfigDir)
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
