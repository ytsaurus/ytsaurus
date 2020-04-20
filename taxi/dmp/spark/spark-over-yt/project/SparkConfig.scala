import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTreeBuilder
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.YTreeTextSerializer

sealed trait SparkConfig {
  def toYson: String = {
    val node = this.getClass.getDeclaredFields.foldLeft(new YTreeBuilder().beginMap()) {
      case (res, nextField) =>
        nextField.setAccessible(true)
        SparkConfig.toYson(nextField.get(this), res.key(nextField.getName))
    }.endMap().build()
    YTreeTextSerializer.serialize(node)
  }
}

object SparkConfig {
  def toYson(value: Any, builder: YTreeBuilder): YTreeBuilder = {
    value match {
      case s: String => builder.value(s)
      case m: Map[String, String] => m.foldLeft(builder.beginMap()) { case (res, (k, v)) => res.key(k).value(v) }.endMap()
      case ss: Seq[String] => ss.foldLeft(builder.beginList()) { case (res, next) => res.value(next) }.endList()
      case i: Int => builder.value(i)
    }
  }

  def toYson(value: Any): String = {
    val builder = new YTreeBuilder()
    toYson(value, builder)
    YTreeTextSerializer.serialize(builder.build())
  }
}

case class SparkGlobalConfig(spark_conf: Map[String, String],
                             latest_spark_cluster_version: String,
                             layer_paths: Seq[String] = Seq(
                               "//home/sashbel/delta/jdk/layer_with_jdk_lastest.tar.gz",
                               "//home/sashbel/delta/python/layer_with_python37.tar.gz",
                               "//home/sashbel/delta/python/layer_with_python34.tar.gz",
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
                               "YT_ALLOW_HTTP_REQUESTS_TO_YT_FROM_JOB" -> "1"
                             ),
                             operation_spec: Map[String, String] = Map()) extends SparkConfig

case class SparkLaunchConfig(spark_yt_base_path: String,
                             file_paths: Seq[String],
                             spark_conf: Map[String, String]) extends SparkConfig

object SparkLaunchConfig {
  def apply(spark_yt_base_path: String,
            spark_conf: Map[String, String] = Map.empty): SparkLaunchConfig = {
    new SparkLaunchConfig(
      spark_yt_base_path = spark_yt_base_path,
      file_paths = Seq(
        s"$spark_yt_base_path/spark.tgz",
        s"$spark_yt_base_path/spark-yt-launcher.jar"
      ),
      spark_conf = spark_conf)
  }
}
