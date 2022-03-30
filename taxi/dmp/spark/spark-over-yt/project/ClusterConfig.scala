package spyt

import sbt._
import spyt.SparkPaths._
import spyt.YtPublishPlugin.autoImport._

import java.io.{FileInputStream, InputStreamReader}
import java.util.Properties

object ClusterConfig {
  def artifacts(log: sbt.Logger, version: String, baseConfigDir: File): Seq[YtPublishArtifact] = {
    val isSnapshot = isSnapshotVersion(version)
    val clusterBasePath = versionPath(sparkYtClusterPath, version)
    val versionConfPath = versionPath(sparkYtConfPath, version)

    val sidecarConfigs = (baseConfigDir / "sidecar-config").listFiles()
    val sidecarConfigsClusterPaths = sidecarConfigs.map(file => s"$versionConfPath/${file.getName}")

    val launchConfig = SparkLaunchConfig(
      clusterBasePath,
      ytserver_proxy_path = Option(System.getProperty("proxyVersion")).map(version =>
        s"$defaultYtServerProxyPath-$version"
      ),
      file_paths = Seq(
        s"$clusterBasePath/spark.tgz",
        s"$clusterBasePath/spark-yt-launcher.jar"
      ) ++ sidecarConfigsClusterPaths
    )
    val launchConfigPublish = YtPublishDocument(
      launchConfig,
      versionConfPath,
      None,
      "spark-launch-conf",
      isSnapshot
    )
    val configsPublish = sidecarConfigs.map(file => YtPublishFile(file, versionConfPath, None, isTtlLimited = isSnapshot))

    val globalConfigPublish = if (!isSnapshot) {
      log.info(s"Prepare configs for ${ytProxies.mkString(", ")}")
      ytProxies.map { proxy =>
        val proxyShort = proxy.split("\\.").head
        val proxyDefaultsFile = baseConfigDir / "spark-defaults" / s"spark-defaults-$proxyShort.conf"
        val proxyDefaults = readSparkDefaults(proxyDefaultsFile)
        val globalConfig = SparkGlobalConfig(proxyDefaults, version)

        YtPublishDocument(globalConfig, sparkYtConfPath, Some(proxy), "global", isSnapshot)
      }
    } else Nil

    val linkBasePath = versionBasePath(sparkYtLegacyConfPath, version)
    val links = if (!isSnapshot) {
      Seq(YtPublishLink(versionConfPath, linkBasePath, None, version, isSnapshot))
    } else Nil

    links ++ configsPublish ++ (launchConfigPublish +: globalConfigPublish)
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
