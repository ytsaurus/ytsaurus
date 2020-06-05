package ru.yandex.spark.launcher

import java.io.{File, PrintWriter, StringWriter}
import java.net.InetAddress

import io.circe.generic.auto._
import io.circe.parser._
import org.apache.log4j.Logger
import ru.yandex.spark.discovery.{Address, CypressDiscoveryService, DiscoveryService}
import ru.yandex.spark.launcher.Service.{BasicService, MasterService, WorkerService}
import ru.yandex.spark.yt.wrapper.YtWrapper
import ru.yandex.spark.yt.wrapper.client.YtClientConfiguration

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.io.Source
import scala.language.postfixOps
import scala.sys.process._

trait SparkLauncher {
  self: VanillaLauncher =>

  private val log = Logger.getLogger(getClass)
  private val masterClass = "org.apache.spark.deploy.master.Master"
  private val workerClass = "org.apache.spark.deploy.worker.Worker"
  private val historyServerClass = "org.apache.spark.deploy.history.HistoryServer"

  def startMaster: MasterService = {
    val host = InetAddress.getLocalHost.getHostName
    val thread = runSparkThread(masterClass, namedArgs = Map("host" -> host))
    val address = readAddress("master", 5 minutes)
    MasterService("Master", address, thread)
  }

  def startWorker(master: Address, cores: Int, memory: String): WorkerService = {
    val thread = runSparkThread(
      workerClass,
      namedArgs = Map(
        "cores" -> cores.toString,
        "memory" -> memory,
        "host" -> InetAddress.getLocalHost.getHostName
      ),
      positionalArgs = Seq(s"spark://${master.hostAndPort}")
    )

    WorkerService("Worker", thread)
  }

  def startHistoryServer(path: String): BasicService = {
    val thread = runSparkThread(
      historyServerClass,
      systemProperties = Map(
        "spark.history.fs.logDirectory" -> path
      )
    )
    val address = readAddress("history", 5 minutes)

    BasicService("Spark History Server", address.hostAndPort, thread)
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
                             systemProperties: Map[String, String] = Map.empty,
                             namedArgs: Map[String, String] = Map.empty,
                             positionalArgs: Seq[String] = Nil): Thread = {
    val thread = new Thread(() => {
      var process: Process = null
      try {
        val log = Logger.getLogger(self.getClass)
        process = runSparkClass(className, systemProperties, namedArgs, positionalArgs, log)
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
                            log: Logger): Process = {
    val fullSystemProperties = systemProperties ++ sparkSystemProperties

    val sparkHome = new File(env("SPARK_HOME", "./spark")).getAbsolutePath
    val command = s"$sparkHome/bin/spark-class " +
      s"${fullSystemProperties.map { case (k, v) => s"-D$k=$v" }.mkString(" ")} " +
      s"$className " +
      s"${namedArgs.map { case (k, v) => s"--$k $v" }.mkString(" ")} " +
      s"${positionalArgs.mkString(" ")}"

    log.info(s"Run command: $command")

    val javaHome = env("JAVA_HOME", "/opt/jdk8")
    Process(
      command,
      new File("."),
      "JAVA_HOME" -> javaHome,
      "SPARK_HOME" -> sparkHome
    ).run(ProcessLogger(log.info(_)))
  }

  @tailrec
  final def checkPeriodically(p: => Boolean): Unit = {
    if (p) {
      Thread.sleep((10 seconds).toMillis)
      checkPeriodically(p)
    }
  }

  def withDiscovery(ytConfig: YtClientConfiguration, discoveryPath: String)(f: DiscoveryService => Unit): Unit = {
    val client = YtWrapper.createRpcClient(ytConfig)
    try {
      val discoveryService = new CypressDiscoveryService(discoveryPath)(client.yt)
      f(discoveryService)
    } finally {
      log.info("Close yt client")
      client.close()
    }
  }

  def withService[T, S <: Service](service: S)(f: S => T): T = {
    try f(service) finally service.stop()
  }

  def withOptionService[T, S <: Service](service: Option[S])(f: Option[S] => T): T = {
    try f(service) finally service.foreach(_.stop())
  }
}
