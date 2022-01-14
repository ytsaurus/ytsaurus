package spyt

object SparkPaths {
  val sparkYtBasePath = "//home/spark"
  val sparkYtLegacyBasePath = "//sys/spark"

  val sparkYtBinPath = s"$sparkYtBasePath/bin"
  val sparkYtSparkForkPath = s"$sparkYtBasePath/spark"
  val sparkYtClusterPath = s"$sparkYtBasePath/bin"
  val sparkYtConfPath = s"$sparkYtBasePath/conf"
  val sparkYtDeltaLayerPath = s"$sparkYtBasePath/delta"
  val sparkYtClientPath = s"$sparkYtBasePath/spyt"

  val sparkYtLegacyBinPath = s"$sparkYtLegacyBasePath/bin"
  val sparkYtLegacyConfPath = s"$sparkYtLegacyBasePath/conf"
  val sparkYtLegacyDeltaLayerPath = s"$sparkYtLegacyBasePath/delta"
  val sparkYtLegacyClientPath = s"$sparkYtLegacyBasePath/spyt"

  val ytPortoLayersPath = "//porto_layers"
  val ytPortoDeltaLayersPath = "//porto_layers/delta"
  val ytPortoBaseLayersPath = "//porto_layers/base"

  val defaultYtServerProxyPath = "//sys/bin/ytserver-proxy/ytserver-proxy"

  val sparkYtE2ETestPath = s"$sparkYtBasePath/e2e"

  def isSnapshotVersion(version: String): Boolean = version.contains("SNAPSHOT")

  def versionBasePath(basePath: String, version: String): String = {
    if (isSnapshotVersion(version)) {
      s"$basePath/snapshots"
    } else {
      s"$basePath/releases"
    }
  }

  def versionPath(basePath: String, version: String): String = {
    s"${versionBasePath(basePath, version)}/$version"
  }
}
