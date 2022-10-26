package ru.yandex.spark.yt.launcher

import java.io.File

import org.scalatest.{Matchers, Suite}
import ru.yandex.spark.yt.launcher.Language.{LocalPython, LocalPython2}

import scala.sys.process.Process

trait TestUtils {
  self: Suite with Matchers =>

  def version: String

  val clusterVersion: String = version.replace("-SNAPSHOT", "~beta1")

  def discoveryPath(clusterName: String): String = {
    s"//home/sashbel/spark-test-${clusterName.replaceAll("_", "-")}"
  }

  def maybeEmpty(predicate: Boolean, str: => String): String = if (predicate) str else ""

  def startCluster(clusterName: String,
                   args: Seq[String] = Nil,
                   localPython: LocalPython = LocalPython2(version)): Unit = {

    val command = s"${localPython.sparkLaunchYt} " +
      "--proxy hume " +
      s"--discovery-path ${discoveryPath(clusterName)} " +
      "--worker-cores 5 " +
      "--worker-num 1 " +
      "--worker-memory 16G " +
      "--tmpfs-limit 10G " +
      s"--spark-cluster-version 2.4.4-$clusterVersion+yandex " +
      s"--operation-alias spark_sashbel_${clusterName} " +
      args.mkString(" ")

    println(command)

    val exitCode = Process(
      command,
      new File("."),
      "PYTHONPATH" -> "",
      "SPARK_HOME" -> localPython.sparkHome
    ).run().exitValue()

    exitCode shouldEqual 0
  }
}
