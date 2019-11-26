package ru.yandex.spark.launcher

import java.io.{File, PrintWriter, StringWriter}
import java.net.InetAddress

import com.google.common.net.HostAndPort
import io.circe.generic.auto._
import io.circe.parser._
import org.apache.log4j.Logger
import ru.yandex.spark.discovery.{Address, CypressDiscoveryService, DiscoveryService}
import ru.yandex.spark.yt.utils.YtClientConfiguration

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.io.Source
import scala.language.postfixOps
import scala.sys.process._

trait SparkLauncher {
  self =>

  private val log = Logger.getLogger(getClass)
  private val sparkHome = sys.env("SPARK_HOME")
  private val masterClass = "org.apache.spark.deploy.master.Master"
  private val workerClass = "org.apache.spark.deploy.worker.Worker"
  private val historyServerClass = "org.apache.spark.deploy.history.HistoryServer"
  private var sparkThread: Thread = _

  def startMaster(port: Int, webUiPort: Int, opts: Option[String]): Address = {
    val env = Map(
      "SPARK_MASTER_OPTS" -> opts,
      "SPARK_PRINT_LAUNCH_COMMAND" -> Some(true)
    ).flatMap { case (k, v) => v.map(vv => k -> vv.toString) }

    val host = InetAddress.getLocalHost.getHostName
    sparkThread = runSparkThread(masterClass, Map.empty, Map(
      "host" -> host,
      "port" -> port.toString,
      "webui-port" -> webUiPort.toString
    ), Nil, env)

    readAddress("master", 5 minutes)
  }

  def startWorker(master: Address,
                  port: Option[Int], webUiPort: Int,
                  cores: Int, memory: String, opts: Option[String]): Unit = {
    val env = Map(
      "SPARK_WORKER_OPTS" -> opts,
    ).flatMap { case (k, v) => v.map(vv => k -> vv.toString) }

    log.info(s"Env: $env")

    sparkThread = runSparkThread(workerClass, Map.empty, Map(
      "port" -> port.get.toString,
      "webui-port" -> webUiPort.toString,
      "cores" -> cores.toString,
      "memory" -> memory
    ), Seq(s"spark://${master.hostAndPort}"), env)
  }

  def startHistoryServer(port: Int, path: String, opts: Option[String]): Address = {
    sparkThread = runSparkThread(historyServerClass, Map(
      "spark.history.fs.logDirectory" -> path,
      "spark.history.ui.port" -> port.toString
    ), Map.empty, Nil, Map.empty)

    readAddress("history", 5 minutes)
  }

  private def readAddress(name: String, timeout: Duration): Address = {
    val successFlag = new File(s"${name}_address_success")
    val file = new File(s"${name}_address")
    if (!DiscoveryService.waitFor(successFlag.exists(), timeout)) {
      throw new RuntimeException("Service address is not found")
    }
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

  private def runSparkThread(className: String,
                             systemProperties: Map[String, String],
                             namedArgs: Map[String, String],
                             positionalArgs: Seq[String],
                             env: Map[String, String]): Thread = {
    val thread = new Thread(() => {
      var process: Process = null
      try {
        val log = Logger.getLogger(self.getClass)
        process = runSparkClass(className, systemProperties, namedArgs, positionalArgs, env, log)
        log.warn(s"Spark exit value: ${process.exitValue()}")
      } catch {
        case e: Throwable =>
          log.error(s"Spark failed with error: ${e.getMessage}")
          val sw = new StringWriter
          e.printStackTrace(new PrintWriter(sw))
          log.error(sw.toString)
          process.destroy()
      }
    }, "Spark Thread")
    thread.setDaemon(true)
    thread.start()
    thread
  }

  private def runSparkClass(className: String,
                            systemProperties: Map[String, String],
                            namedArgs: Map[String, String],
                            positionalArgs: Seq[String],
                            env: Map[String, String],
                            log: Logger): Process = {
    val command = s"$sparkHome/bin/spark-class " +
      s"${systemProperties.map{case (k, v) => s"-D$k=$v"}.mkString(" ")} " +
      s"$className " +
      s"${namedArgs.map { case (k, v) => s"--$k $v" }.mkString(" ")} " +
      s"${positionalArgs.mkString(" ")}"

    log.info(s"Run command: $command")

    Process(command, None, env.toSeq: _*).run(ProcessLogger(log.info(_)))
  }

  def sparkThreadIsAlive: Boolean = sparkThread.isAlive

  @tailrec
  final def checkPeriodically(p: => Boolean): Unit = {
    if (p) {
      Thread.sleep((10 seconds).toMillis)
      checkPeriodically(p)
    }
  }

  def run(ytConfig: YtClientConfiguration, discoveryPath: String)(f: DiscoveryService => Unit): Unit = {
    val discoveryService = new CypressDiscoveryService(ytConfig, discoveryPath)

    try {
      f(discoveryService)
    } finally {
      log.info("Closing discovery service")
      discoveryService.close()
      Option(sparkThread).foreach(_.interrupt())
    }
  }
}
