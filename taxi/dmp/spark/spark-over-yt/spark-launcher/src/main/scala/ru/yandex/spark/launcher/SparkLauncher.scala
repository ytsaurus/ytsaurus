package ru.yandex.spark.launcher

import java.io.{File, PrintWriter, StringWriter}
import java.net.InetAddress

import org.apache.log4j.Logger
import ru.yandex.spark.discovery.Address

import scala.annotation.tailrec
import scala.io.Source
import scala.language.postfixOps
import scala.sys.process._

import io.circe._
import io.circe.generic.auto._
import io.circe.parser._
import io.circe.syntax._

object SparkLauncher {
  private val log = Logger.getLogger(getClass)
  private val sparkHome = sys.env("SPARK_HOME")
  private val masterClass = "org.apache.spark.deploy.master.Master"
  private val workerClass = "org.apache.spark.deploy.worker.Worker"

  private var sparkProcess: Process = _

  def startMaster(port: Int, webUiPort: Int, opts: Option[String]): Address = {
    val env = Map(
      "SPARK_MASTER_OPTS" -> opts,
      "SPARK_PRINT_LAUNCH_COMMAND" -> Some("true")
    ).flatMap { case (k, v) => v.map(vv => k -> vv.toString) }

    val host = InetAddress.getLocalHost.getHostName
    runSparkThread(masterClass, Map(
      "host" -> host,
      "port" -> port.toString,
      "webui-port" -> webUiPort.toString
    ), Nil, env)

    val address = readAddress()
    log.info(s"Address from file: $address")

    address
  }

  @tailrec
  private def readAddress(): Address = {
    val successFlag = new File("master_address_success")
    val file = new File("master_address")
    if (!successFlag.exists()) {
      Thread.sleep(100)
      readAddress()
    } else {
      val source = Source.fromFile(file)
      try {
        decode[Address](source.mkString) match {
          case Right(address) => address
          case Left(error) => throw error
        }
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

  def startWorker(master: Address,
                  port: Option[Int], webUiPort: Int,
                  cores: Int, memory: String, opts: Option[String]): Unit = {
    val env = Map(
      "SPARK_WORKER_OPTS" -> opts,
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
