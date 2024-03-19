package spyt

import tech.ytsaurus.ysontree.{YTreeBuilder, YTreeNode, YTreeTextSerializer}
import tech.ytsaurus.client.YTsaurusClient
import spyt.SparkPaths._

import scala.annotation.tailrec

sealed trait YsonableConfig {
  def toYson: String = {
    YTreeTextSerializer.serialize(toYTree)
  }

  def toYTree: YTreeNode = {
    toYson(new YTreeBuilder()).build()
  }

  def toYson(builder: YTreeBuilder): YTreeBuilder = {
    this.getClass.getDeclaredFields.foldLeft(builder.beginMap()) {
      case (res, nextField) =>
        nextField.setAccessible(true)
        YsonableConfig.toYson(nextField.get(this), res.key(nextField.getName))
    }.endMap()
  }

  def resolveSymlinks(implicit yt: YTsaurusClient): YsonableConfig = this
}

object YsonableConfig {
  @tailrec
  private def toYson(value: Any, builder: YTreeBuilder): YTreeBuilder = {
    value match {
      case Some(value) => toYson(value, builder)
      case None => builder.entity()
      case m: Map[_, _] => m.foldLeft(builder.beginMap()) { case (res, (k: String, v)) => res.key(k).value(v) }.endMap()
      case ss: Seq[_] => ss.foldLeft(builder.beginList()) { case (res, next) => res.value(next) }.endList()
      case c: YsonableConfig => c.toYson(builder)
      case any => builder.value(any)
    }
  }

  def toYson(value: Any): String = {
    YTreeTextSerializer.serialize(toYTree(value))
  }

  def toYTree(value: Any): YTreeNode = {
    val builder = new YTreeBuilder()
    toYson(value, builder)
    builder.build()
  }
}

case class SparkGlobalConfig(spark_conf: Map[String, String],
                             latest_spyt_version: String = "1.76.1",  // COMPAT(alex-shishkin)
                             latest_spark_cluster_version: String = "1.75.4",
                             python_cluster_paths: Map[String, String] = Map(
                               "3.11" -> "/opt/python3.11/bin/python3.11",
                               "3.9" -> "/opt/python3.9/bin/python3.9",
                               "3.8" -> "/opt/python3.8/bin/python3.8",
                               "3.7" -> "/opt/python3.7/bin/python3.7",
                               "3.5" -> "python3.5",
                               "3.4" -> "/opt/python3.4/bin/python3.4",
                               "2.7" -> "python2.7",
                             ),
                             environment: Map[String, String] = Map(
                               "JAVA_HOME" -> "/opt/jdk11",
                               "IS_SPARK_CLUSTER" -> "true",
                               "YT_ALLOW_HTTP_REQUESTS_TO_YT_FROM_JOB" -> "1",
                               "ARROW_ENABLE_NULL_CHECK_FOR_GET" -> "false",
                               "ARROW_ENABLE_UNSAFE_MEMORY_ACCESS" -> "true",
                               "SOLOMON_PUSH_PORT" -> "27099"
                             ),
                             operation_spec: Map[String, YTreeNode] = Map(
                               "job_cpu_monitor" -> YsonableConfig.toYTree(Map("enable_cpu_reclaim" -> "false"))
                             ),
                             worker_num_limit: Int = 1000,
                             ytserver_proxy_path: String = defaultYtServerProxyPath) extends YsonableConfig

case class SparkLaunchConfig(spark_yt_base_path: String,
                             file_paths: Seq[String],
                             spark_conf: Map[String, String],
                             enablers: SpytEnablers = SpytEnablers(),
                             ytserver_proxy_path: Option[String] = None,
                             layer_paths: Seq[String] = SparkLaunchConfig.defaultLayers,
                             environment: Map[String, String] = Map.empty) extends YsonableConfig {
  override def resolveSymlinks(implicit yt: YTsaurusClient): YsonableConfig = {
    import SparkLaunchConfig._
    if (ytserver_proxy_path.isEmpty) {
      copy(ytserver_proxy_path = Some(resolveSymlink(defaultYtServerProxyPath)))
    } else this
  }
}

case class SpytEnablers(enable_byop: Boolean = true,
                        enable_arrow: Boolean = true,
                        enable_mtn: Boolean = true,
                        enable_solomon_agent: Boolean = true,
                        enable_preference_ipv6: Boolean = true,
                        enable_tcp_proxy: Boolean = true) extends YsonableConfig {
  override def toYson(builder: YTreeBuilder): YTreeBuilder = {
    builder.beginMap()
      .key("spark.hadoop.yt.byop.enabled").value(enable_byop)
      .key("spark.hadoop.yt.read.arrow.enabled").value(enable_arrow)
      .key("spark.hadoop.yt.mtn.enabled").value(enable_mtn)
      .key("spark.hadoop.yt.solomonAgent.enabled").value(enable_solomon_agent)
      .key("spark.hadoop.yt.preferenceIpv6.enabled").value(enable_preference_ipv6)
      .key("spark.hadoop.yt.tcpProxy.enabled").value(enable_tcp_proxy)
      .endMap()
  }
}

object SparkLaunchConfig {
  val defaultLayers = Seq(
    s"$sparkYtDeltaLayerPath/layer_with_solomon_agent.tar.gz",
    s"$ytPortoDeltaLayersPath/jdk/layer_with_jdk_lastest.tar.gz",
    s"$sparkYtDeltaLayerPath/python/layer_with_python311_focal_yandexyt0131.tar.gz",
    s"$sparkYtDeltaLayerPath/python/layer_with_python39_focal_yandexyt0131.tar.gz",
    s"$sparkYtDeltaLayerPath/python/layer_with_python38_focal_yandexyt0131.tar.gz",
    s"$sparkYtDeltaLayerPath/python/layer_with_python37_focal_yandexyt0131.tar.gz",
    s"$ytPortoBaseLayersPath/focal/porto_layer_search_ubuntu_focal_app_lastest.tar.gz"
  )

  def resolveSymlink(symlink: String)(implicit yt: YTsaurusClient): String = {
    yt.getNode(s"$symlink&/@target_path").join().stringValue()
  }
}
