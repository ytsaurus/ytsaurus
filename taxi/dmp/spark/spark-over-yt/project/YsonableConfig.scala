package spyt

import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTreeBuilder
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.YTreeTextSerializer
import ru.yandex.inside.yt.kosher.ytree.YTreeNode
import ru.yandex.yt.ytclient.proxy.YtClient

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

  def resolveSymlinks(implicit yt: YtClient): YsonableConfig = this
}

object YsonableConfig {
  @tailrec
  private def toYson(value: Any, builder: YTreeBuilder): YTreeBuilder = {
    value match {
      case Some(value) => toYson(value, builder)
      case None => builder
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
                             latest_spark_cluster_version: String,
                             layer_paths: Seq[String] = Seq(
                               "//sys/spark/delta/layer_with_solomon_agent.tar.gz",
                               "//sys/spark/delta/jdk/layer_with_jdk_lastest.tar.gz",
                               "//sys/spark/delta/python/layer_with_python37_libs_3.tar.gz",
                               "//sys/spark/delta/python/layer_with_python34.tar.gz",
                               "//porto_layers/base/xenial/porto_layer_search_ubuntu_xenial_app_lastest.tar.gz"
                             ),
                             python_cluster_paths: Map[String, String] = Map(
                               "3.7" -> "/opt/python3.7/bin/python3.7",
                               "3.5" -> "python3.5",
                               "3.4" -> "/opt/python3.4/bin/python3.4",
                               "2.7" -> "python2.7"
                             ),
                             environment: Map[String, String] = Map(
                               "JAVA_HOME" -> "/opt/jdk8",
                               "IS_SPARK_CLUSTER" -> "true",
                               "YT_ALLOW_HTTP_REQUESTS_TO_YT_FROM_JOB" -> "1",
                               "ARROW_ENABLE_NULL_CHECK_FOR_GET" -> "false",
                               "ARROW_ENABLE_UNSAFE_MEMORY_ACCESS" -> "true"
                             ),
                             operation_spec: Map[String, YTreeNode] = Map(
                               "job_cpu_monitor" -> YsonableConfig.toYTree(Map("enable_cpu_reclaim" -> "false"))
                             ),
                             worker_num_limit: Int = 1000,
                             ytserver_proxy_path: String = SparkLaunchConfig.defaultYtServerProxyPath) extends YsonableConfig

case class SparkLaunchConfig(spark_yt_base_path: String,
                             file_paths: Seq[String],
                             spark_conf: Map[String, String] = Map.empty,
                             enablers: SpytEnablers = SpytEnablers(),
                             ytserver_proxy_path: Option[String] = None,
                             layer_paths: Seq[String] = Seq(
                               "//sys/spark/delta/layer_with_solomon_agent.tar.gz",
                               "//porto_layers/delta/jdk/layer_with_jdk_lastest.tar.gz",
                               "//sys/spark/delta/python/layer_with_python37_libs_3.tar.gz",
                               "//sys/spark/delta/python/layer_with_python34.tar.gz",
                               "//porto_layers/base/xenial/porto_layer_search_ubuntu_xenial_app_lastest.tar.gz"
                             ),
                             environment: Map[String, String] = Map(
                               "JAVA_HOME" -> "/opt/jdk11"
                             )) extends YsonableConfig {
  override def resolveSymlinks(implicit yt: YtClient): YsonableConfig = {
    import SparkLaunchConfig._
    if (ytserver_proxy_path.isEmpty) {
      copy(ytserver_proxy_path = Some(resolveSymlink(defaultYtServerProxyPath)))
    } else this
  }
}

case class SpytEnablers(enable_byop: Boolean = true,
                        enable_arrow: Boolean = true,
                        enable_mtn: Boolean = false) extends YsonableConfig

object SparkLaunchConfig {
  val defaultYtServerProxyPath = "//sys/bin/ytserver-proxy/ytserver-proxy"

  def resolveSymlink(symlink: String)(implicit yt: YtClient): String = {
    yt.getNode(s"$symlink&/@target_path").join().stringValue()
  }
}
