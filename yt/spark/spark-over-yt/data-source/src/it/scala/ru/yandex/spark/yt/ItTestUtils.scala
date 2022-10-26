package ru.yandex.spark.yt

import org.scalatest.{Matchers, Suite}
import ru.yandex.spark.yt.ProgrammingLanguage.{LocalPython, LocalPython2}

import java.io.File
import scala.language.postfixOps
import scala.sys.process.Process

trait ItTestUtils {
  self: Suite with Matchers =>

  def oldVersion: String

  def newVersion: String

  def discoveryPath(clusterName: String): String = {
    s"//home/sashbel/spark-test-${clusterName.replaceAll("_", "-")}"
  }

  def maybeEmpty(predicate: Boolean, str: => String): String = if (predicate) str else ""

  def startCluster(version: String, clusterName: String, args: Seq[String] = Nil,
                   localPython: LocalPython = LocalPython2(newVersion)): Unit = {
    val command = s"${localPython.sparkLaunchYt} " +
      "--proxy hume " +
      s"--discovery-path ${discoveryPath(clusterName)} " +
      "--worker-cores 5 " +
      "--worker-num 1 " +
      "--worker-memory 16G " +
      "--tmpfs-limit 10G " +
      s"--spark-cluster-version 2.4.4-$version+yandex " +
      s"--operation-alias spark_sashbel_$clusterName " +
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
