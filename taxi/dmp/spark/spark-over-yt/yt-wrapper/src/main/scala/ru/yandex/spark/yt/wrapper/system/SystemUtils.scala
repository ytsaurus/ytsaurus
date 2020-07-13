package ru.yandex.spark.yt.wrapper.system

object SystemUtils {
  def isEnabled(name: String): Boolean = {
    sys.env.get(s"SPARK_YT_${formatName(name)}_ENABLED").exists(_.toBoolean)
  }

  def envGet(name: String): Option[String] = sys.env.get(s"SPARK_YT_${formatName(name)}")

  private def formatName(name: String): String = name.toUpperCase()
}
