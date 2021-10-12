package spyt

object SparkPaths {
  val sparkYtBasePath = "//home/spark"
  val sparkYtLegacyBasePath = "//sys/spark"

  val sparkYtBinPath = s"$sparkYtBasePath/bin"
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
}
