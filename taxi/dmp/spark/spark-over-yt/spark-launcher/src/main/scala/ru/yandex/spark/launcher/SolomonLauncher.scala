package ru.yandex.spark.launcher

import com.twitter.scalding.Args
import org.slf4j.LoggerFactory
import ru.yandex.spark.launcher.Service.BasicService
import ru.yandex.spark.yt.wrapper.client.YtClientConfiguration

import java.io.File
import scala.concurrent.duration.Duration

trait SolomonLauncher extends SidecarLauncher {
  self: VanillaLauncher =>

  private val log = LoggerFactory.getLogger(getClass)

  private def prepareConfigFile(templateContent: String, config: SidecarConfig): String = {
    config match {
      case c: SolomonConfig =>
        log.info(s"Spark ui port: ${c.sparkUiPort}")
        templateContent
          .replaceAll("\\$SPARK_COMPONENT", c.sparkComponent)
          .replaceAll("\\$SOLOMON_CONFIG_FILE", c.solomonConfigFile)
          .replaceAll("\\$SPARK_UI_PORT", c.sparkUiPort.toString)
    }
  }

  def startSolomonAgent(args: Array[String],
                        sparkComponent: String,
                        sparkUiPort: Int): BasicService = {
    val config = SolomonConfig(sparkSystemProperties, args, sparkComponent, sparkUiPort)
    startService(config, prepareConfigFile, processWorkingDir = Some(new File(path(config.configDirectory))))
  }
}

case class SolomonConfig(binaryPath: String,
                         configPaths: Seq[String],
                         configDirectory: String,
                         solomonConfigFile: String,
                         port: Int,
                         monitoringPort: Int,
                         operationAlias: String,
                         ytJobCookie: String,
                         ytConf: YtClientConfiguration,
                         timeout: Duration,
                         sparkComponent: String,
                         sparkUiPort: Int) extends SidecarConfig {
  override def host: String = "::"

  override def serviceName: String = "Solomon Agent"
}

object SolomonConfig extends SidecarConfigUtils {

  override protected def argBaseName: String = "solomon"

  def apply(sparkConf: Map[String, String],
            args: Array[String],
            sparkComponent: String,
            sparkUiPort: Int): SolomonConfig = {
    SolomonConfig(sparkConf, Args(args), sparkComponent, sparkUiPort)
  }

  def apply(sparkConf: Map[String, String], args: Args, sparkComponent: String, sparkUiPort: Int): SolomonConfig = {
    implicit val a = args
    val agentConfig = "solomon-agent.template.conf"
    val serviceConfig = s"solomon-service-$sparkComponent.template.conf"
    SolomonConfig(
      binaryPath = optionArg("binary-path").getOrElse("/usr/local/bin/solomon-agent"),
      configPaths = optionArg("config-paths")
        .map(_.split(",").toSeq)
        .getOrElse(Seq(s"$$HOME/$agentConfig", s"$$HOME/$serviceConfig")),
      configDirectory = optionArg("config-dir").getOrElse("$HOME"),
      solomonConfigFile = optionArg("service-config-file").getOrElse(serviceConfig.replace(".template", "")),
      port = optionArg("port").map(_.toInt).getOrElse(27100),
      monitoringPort = optionArg("monitoring-port").map(_.toInt).getOrElse(27101),
      operationAlias = args.optional("operation-alias").getOrElse(sys.env("YT_OPERATION_ALIAS")),
      ytJobCookie = args.optional("job-cookie").getOrElse(sys.env("YT_JOB_COOKIE")),
      ytConf = ytConf,
      timeout = timeout,
      sparkComponent = sparkComponent,
      sparkUiPort = sparkUiPort
    )
  }
}
