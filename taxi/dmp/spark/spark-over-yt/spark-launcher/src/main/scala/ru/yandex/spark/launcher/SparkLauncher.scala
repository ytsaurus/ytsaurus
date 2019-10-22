package ru.yandex.spark.launcher

import java.io.{File, PrintWriter, StringWriter}
import java.net.InetAddress

import org.apache.log4j.Logger
import ru.yandex.spark.discovery.Address

import scala.annotation.tailrec
import scala.io.Source
import scala.language.postfixOps
import scala.sys.process._

object SparkLauncher {
  private val log = Logger.getLogger(getClass)
  private val sparkHome = sys.env("SPARK_HOME")
  private val masterClass = "org.apache.spark.deploy.master.Master"
  private val workerClass = "org.apache.spark.deploy.worker.Worker"

  private var sparkProcess: Process = _

  def startMaster(port: Int, webUiPort: Int): Address = {
    val host = InetAddress.getLocalHost.getHostName
    runSparkThread(masterClass, Map(
      "host" -> host,
      "port" -> port.toString,
      "webui-port" -> webUiPort.toString
    ), Nil, Map())

    val boundPorts = readPorts()
    log.info(s"Ports from file: $boundPorts")

    Address(
      host,
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

  private def runSparkThread(className: String,
                             namedArgs: Map[String, String],
                             positionalArgs: Seq[String],
                             env: Map[String, String]): Unit = {
    val thread = new Thread(() => {
      try {
        val log = Logger.getLogger(SparkLauncher.getClass)
        runSparkClass(className, namedArgs, positionalArgs, env, log)
      } catch {
        case e: Throwable =>
          log.error(s"Spark failed with error: ${e.getMessage}")
          val sw = new StringWriter
          e.printStackTrace(new PrintWriter(sw))
          log.error(sw.toString)
      }
    }, "Spark Thread")
    thread.setDaemon(true)
    thread.start()
  }

  private def runSparkClass(className: String,
                            namedArgs: Map[String, String],
                            positionalArgs: Seq[String],
                            env: Map[String, String],
                            log: Logger): Unit = {
    val command = s"$sparkHome/bin/spark-class $className " +
      s"${namedArgs.map{case (k, v) => s"--$k $v"}.mkString(" ")} " +
      s"${positionalArgs.mkString(" ")}"

    log.info(s"Run command: $command")

    sparkProcess = Process(command, None, env.toSeq:_*)
      .run(ProcessLogger(log.info(_)))

    sparkProcess.exitValue()
  }

  def stopSpark(): Unit = {
    sparkProcess.destroy()
  }

  def startSlave(master: Address,
                 port: Option[Int], webUiPort: Int,
                 cores: Int, memory: String, ops: Option[String]): Unit = {
    val env = Map(
      "SPARK_WORKER_OPTS" -> ops.map(_.drop(1).dropRight(1)),
    ).flatMap { case (k, v) => v.map(vv => k -> vv.toString) }

    log.info(s"Env: $env")

    runSparkThread(workerClass, Map(
      "port" -> port.get.toString,
      "webui-port" -> webUiPort.toString,
      "cores" -> cores.toString,
      "memory" -> memory
    ), Seq(s"spark://${master.hostAndPort}"), env)
  }

  def stopSlave(): Unit = {
    s"$sparkHome/sbin/stop-slave.sh" !
  }
}
