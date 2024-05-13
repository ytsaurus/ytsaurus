package spyt

object SparkPaths {
  val sparkYtBasePath = "//home/spark"

  val sparkYtConfPath = s"$sparkYtBasePath/conf"
  val sparkYtDeltaLayerPath = s"$sparkYtBasePath/delta"
  val spytPath = s"$sparkYtBasePath/spyt"

  val ytPortoLayersPath = "//porto_layers"
  val ytPortoDeltaLayersPath = "//porto_layers/delta"
  val ytPortoBaseLayersPath = "//porto_layers/base"

  val defaultYtServerProxyPath = "//sys/bin/ytserver-proxy/ytserver-proxy"

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
