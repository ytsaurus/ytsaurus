package ru.yandex.spark.launcher

import io.circe.generic.auto._
import io.circe.parser._
import org.slf4j.{Logger, LoggerFactory}
import ru.yandex.spark.launcher.Service.{BasicService, MasterService}
import ru.yandex.spark.yt.wrapper.YtWrapper
import ru.yandex.spark.yt.wrapper.client.YtClientConfiguration
import ru.yandex.spark.yt.wrapper.discovery.{Address, CypressDiscoveryService, DiscoveryService}

import java.io.File
import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.io.Source
import scala.language.postfixOps
import scala.sys.process._
import scala.util.{Failure, Success, Try}

trait SparkLauncher {
  self: VanillaLauncher =>

  private val log = LoggerFactory.getLogger(getClass)
  private val masterClass = "org.apache.spark.deploy.master.Master"
  private val workerClass = "org.apache.spark.deploy.worker.Worker"
  private val historyServerClass = "org.apache.spark.deploy.history.HistoryServer"

  def startMaster: MasterService = {
    log.info("Start Spark master")
    val thread = runSparkThread(masterClass, namedArgs = Map("host" -> Utils.ytHostnameOrIpAddress))
    val address = readAddressOrDie("master", 5 minutes, thread)
    MasterService("Master", address, thread)
  }

  def startWorker(master: Address, cores: Int, memory: String): BasicService = {
    val thread = runSparkThread(
      workerClass,
      namedArgs = Map(
        "cores" -> cores.toString,
        "memory" -> memory,
        "host" -> Utils.ytHostnameOrIpAddress
      ),
      positionalArgs = Seq(s"spark://${master.hostAndPort}")
    )
    val address = readAddressOrDie("worker", 5 minutes, thread)

    BasicService("Worker", address.hostAndPort, thread)
  }

  def startHistoryServer(path: String): BasicService = {
    val thread = runSparkThread(
      historyServerClass,
      systemProperties = Map(
        "spark.history.fs.logDirectory" -> path
      )
    )
    val address = readAddressOrDie("history", 5 minutes, thread)
    BasicService("Spark History Server", address.hostAndPort, thread)
  }

  def readAddressOrDie(name: String, timeout: Duration, thread: Thread): Address = {
    Try(readAddress(name, timeout)) match {
      case Success(address) => address
      case Failure(exception) =>
        thread.interrupt()
        throw exception
    }
  }

  private def readAddress(name: String, timeout: Duration): Address = {
    val successFlag = new File(s"${name}_address_success")
    val file = new File(s"${name}_address")
    if (!DiscoveryService.waitFor(successFlag.exists(), timeout, s"spark component address in file $file")) {
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
        val log = LoggerFactory.getLogger(self.getClass)
        process = runSparkClass(className, systemProperties, namedArgs, positionalArgs, log)
        log.warn(s"Spark exit value: ${process.exitValue()}")
      } catch {
        case e: Throwable =>
          log.error(s"Spark failed with error: ${e.getMessage}")
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

    val javaHome = env("JAVA_HOME", "/opt/jdk11")
    val sparkLocalDirs = env("SPARK_LOCAL_DIRS", "./tmpfs")
    Process(
      command,
      new File("."),
      "JAVA_HOME" -> javaHome,
      "SPARK_HOME" -> sparkHome,
      "SPARK_LOCAL_DIRS" -> sparkLocalDirs,
      // when using MTN, Spark should use ip address and not hostname, because hostname is not in DNS
      "SPARK_LOCAL_HOSTNAME" -> Utils.ytHostnameOrIpAddress
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
    val client = YtWrapper.createRpcClient("discovery", ytConfig)
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
