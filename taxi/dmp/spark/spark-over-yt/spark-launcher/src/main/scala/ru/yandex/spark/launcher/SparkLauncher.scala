package ru.yandex.spark.launcher

import com.google.common.net.HostAndPort

import scala.language.postfixOps
import scala.sys.process._

object SparkLauncher {
  private val sparkHome = sys.env("SPARK_HOME")
  def startMaster(port: Int): Unit = {
    Process(s"$sparkHome/sbin/start-master.sh", None, "SPARK_MASTER_PORT" -> port.toString).run().exitValue()
  }

  def stopMaster(): Unit = {
    s"$sparkHome/sbin/stop-master.sh" !
  }

  def startSlave(master: HostAndPort, cores: Int, memory: String): Unit = {
    Seq(
      s"$sparkHome/sbin/start-slave.sh",
      s"spark://$master",
      "--cores", cores.toString,
      "--memory", memory
    ) !
  }

  def stopSlave(): Unit = {
    s"$sparkHome/sbin/stop-slave.sh" !
  }

}
