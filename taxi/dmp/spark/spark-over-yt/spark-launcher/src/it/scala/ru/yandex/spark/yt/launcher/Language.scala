package ru.yandex.spark.yt.launcher

sealed trait Language

object Language {
  case object Scala extends Language
  case object Python2 extends Language

  abstract class LocalPython(version: String, spytVersion: String) extends Language {
    val virtualenvName = s"python${version}spyt${spytVersion.toLowerCase().replace("-", "")}"
    val virtualenvDirectory = s"/Users/sashbel/Envs/$virtualenvName"
    val binDirectory = s"$virtualenvDirectory/bin"
    val pythonBin = s"$binDirectory/python"
    val sparkLaunchYt = s"$pythonBin $binDirectory/spark-launch-yt"
    val sparkSubmitYt = s"$pythonBin $binDirectory/spark-submit-yt"
    val sparkHome = s"$virtualenvDirectory/lib/python${version}.7/site-packages/pyspark"
  }

  case class LocalPython2(spytVersion: String) extends LocalPython("2", spytVersion)

  case class LocalPython3(spytVersion: String) extends LocalPython("3", spytVersion)
}
