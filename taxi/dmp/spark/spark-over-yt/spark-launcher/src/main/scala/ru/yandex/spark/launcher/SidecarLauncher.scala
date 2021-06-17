package ru.yandex.spark.launcher

import com.google.common.net.HostAndPort
import com.twitter.scalding.Args
import org.slf4j.LoggerFactory
import ru.yandex.spark.launcher.Service.BasicService
import ru.yandex.spark.yt.wrapper.Utils.parseDuration
import ru.yandex.spark.yt.wrapper.client.YtClientConfiguration
import ru.yandex.spark.yt.wrapper.discovery.DiscoveryService

import java.io.File
import java.nio.file.{Path, Paths}
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.sys.process.Process

trait SidecarLauncher {
  self: VanillaLauncher =>

  private val log = LoggerFactory.getLogger(getClass)

  private def waitForServiceStart(config: SidecarConfig, process: Thread, timeout: Duration): Unit = {
    val address = HostAndPort.fromParts(config.host, config.port)
    val monitoringAddress = HostAndPort.fromParts(config.host, config.monitoringPort)
    DiscoveryService.waitFor(DiscoveryService.isAlive(address, 0) || !process.isAlive, timeout,
      s"${config.serviceName} on port ${config.port}")
    DiscoveryService.waitFor(DiscoveryService.isAlive(monitoringAddress, 0) || !process.isAlive, timeout,
      s"${config.serviceName} on port ${config.monitoringPort}")
  }

  def startService[T <: SidecarConfig](config: T,
                                       prepareConfigFile: (String, T) => String,
                                       prepareBinaryFile: Path => Path = identity,
                                       processWorkingDir: Option[File] = None,
                                       serviceEnv: Map[String, String] = Map.empty): BasicService = {
    log.info(s"Start ${config.serviceName}")

    val binaryAbsolutePath = prepareBinaryFile(Paths.get(path(config.binaryPath)))
    val configFiles = config.configPaths.map { configTemplatePath =>
      log.info(s"Prepare config from template $configTemplatePath")
      createFromTemplate(new File(path(configTemplatePath))) { content =>
        val templateContent = replaceHome(content)
          .replaceAll("\\$YT_OPERATION_ALIAS", config.operationAlias)
          .replaceAll("\\$YT_JOB_COOKIE", config.ytJobCookie)

        prepareConfigFile(templateContent, config)
      }
    }

    val thread = new Thread(() => {
      val process = Process.apply(
        s"$binaryAbsolutePath --config ${configFiles.head.getAbsolutePath}",
        cwd = processWorkingDir,
        serviceEnv.toSeq: _*
      ).run()

      try {
        val exitCode = process.exitValue()
        log.info(s"Rpc proxy exit code is $exitCode")
      } catch {
        case e: Throwable =>
          process.destroy()
          throw e
      }
    })
    thread.setDaemon(true)
    thread.start()
    waitForServiceStart(config, thread, config.timeout)
    if (thread.isAlive) {
      log.info(s"${config.serviceName} started on port ${config.port}, monitoring port ${config.monitoringPort}")
    }
    BasicService(config.serviceName, config.port, thread)
  }
}

trait SidecarConfig {
  def serviceName: String

  def binaryPath: String

  def configPaths: Seq[String]

  def host: String

  def port: Int

  def monitoringPort: Int

  def operationAlias: String

  def ytJobCookie: String

  def ytConf: YtClientConfiguration

  def timeout: Duration
}

trait SidecarConfigUtils {
  protected def argBaseName: String

  private def envArgBaseName: String = s"SPARK_YT_${toEnv(argBaseName)}"

  private def toEnv(name: String): String = name.replace("-", "_").toUpperCase()


  protected def envName(name: String): String = s"${envArgBaseName}_${toEnv(name)}"

  protected def arg(name: String)(implicit args: Args): String = {
    args.optional(s"$argBaseName-$name").getOrElse(sys.env(envName(name)))
  }

  protected def optionArg(name: String)(implicit args: Args): Option[String] = {
    args.optional(s"$argBaseName-$name").orElse(sys.env.get(envName(name)))
  }

  protected def ytConf(implicit args: Args): YtClientConfiguration = {
    YtClientConfiguration(args.optional)
  }

  protected def timeout(implicit args: Args): Duration = {
    args.optional("timeout").map(parseDuration).getOrElse(5 minutes)
  }
}
