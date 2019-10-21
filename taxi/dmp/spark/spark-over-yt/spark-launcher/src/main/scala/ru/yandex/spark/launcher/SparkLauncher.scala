package ru.yandex.spark.launcher

import java.io.File
import java.net.InetAddress

import com.google.common.net.HostAndPort
import org.apache.log4j.Logger

import scala.annotation.tailrec
import scala.io.Source
import scala.language.postfixOps
import scala.sys.process._

object SparkLauncher {
  private val log = Logger.getLogger(getClass)
  private val sparkHome = sys.env("SPARK_HOME")
  private val masterClass = "org.apache.spark.deploy.master.Master"
  private val workerClass = "org.apache.spark.deploy.worker.Worker"

  def startMaster(ports: Ports): Ports = {
    runSparkClass(masterClass, Nil, Map(
      "host" -> InetAddress.getLocalHost.getHostName,
      "port" -> ports.port.toString,
      "webui-port" -> ports.webUiPort.toString
    ), Nil, Map(), ProcessLogger(log.info(_)))

    val boundPorts = readPorts()
    log.info(s"Ports from file: $boundPorts")

    Ports(
      boundPorts.head,
      boundPorts(1)
    )
  }

  @tailrec
  private def readPorts(): Seq[Int] = {
    val successFlag = new File("ports_success")
    val file = new File("ports")
    if (!successFlag.exists()) {
      Thread.sleep(100)
      readPorts()
    } else {
      val source = Source.fromFile(file)
      try {
        source.getLines().map(_.toInt).toList
      } finally {
        source.close()
      }
    }
  }

  private def runSparkClass(className: String,
                            positionalArgs: Seq[String],
                            args: Map[String, String],
                            pargs: Seq[String],
                            env: Map[String, String],
                            logger: ProcessLogger): Unit = {
    val command = s"$sparkHome/bin/spark-class $className ${positionalArgs.mkString(" ")} " +
      s"${args.map{case (k, v) => s"--$k $v"}.mkString(" ")} ${pargs.mkString(" ")}"

    log.info(s"Run command: $command")

    Process(command, None, env.toSeq:_*)
      .run(logger)
  }

  def stopMaster(): Unit = {
    s"$sparkHome/sbin/stop-master.sh" !
  }

  def startSlave(master: HostAndPort,
                 port: Option[Int], webUiPort: Int,
                 cores: Int, memory: String, ops: Option[String]): Unit = {
    val env = Map(
      "SPARK_WORKER_OPTS" -> ops.map(_.drop(1).dropRight(1)),
    ).flatMap { case (k, v) => v.map(vv => k -> vv.toString) }

    log.info(s"Env: $env")

    runSparkClass(workerClass, Nil, Map(
      "port" -> port.get.toString,
      "webui-port" -> webUiPort.toString,
      "cores" -> cores.toString,
      "memory" -> memory
    ), Seq(s"spark://$master"), env, ProcessLogger(log.info(_)))
  }

  def stopSlave(): Unit = {
    s"$sparkHome/sbin/stop-slave.sh" !
  }

}

case class Ports(port: Int, webUiPort: Int)