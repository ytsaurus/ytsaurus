package org.apache.spark.deploy.history

import org.apache.spark.deploy.history.HistoryServer.{createSecurityManager, initSecurity}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.History
import org.apache.spark.ui.JettyUtils
import org.apache.spark.util.{ShutdownHookManager, Utils}
import org.apache.spark.{SecurityManager, SparkConf}
import tech.ytsaurus.spark.launcher.AddressUtils

import javax.servlet.http.HttpServletRequest

class YtHistoryServer(conf: SparkConf,
                      provider: ApplicationHistoryProvider,
                      securityManager: SecurityManager,
                      port: Int) extends HistoryServer(conf, provider, securityManager, port) {

  override def initialize(): Unit = {
    val ytLogPage = new YtLogPage(conf)
    attachPage(ytLogPage)

    val servletParams: JettyUtils.ServletParams[String] = (request: HttpServletRequest) => ytLogPage.renderLog(request)

    // This workaround via reflection is needed because jetty is shaded in Spark,
    // and we can't compile this class in SPYT using shaded classes
    val createServletHandlerMethod = JettyUtils.getClass.getDeclaredMethod("createServletHandler",
      classOf[String], classOf[JettyUtils.ServletParams[_]], classOf[SparkConf], classOf[String])
    val servletHandler = createServletHandlerMethod.invoke(JettyUtils, "/workerLog", servletParams, conf, "")

    // And this too
    val attachHandlerMethod = this.getClass.getMethod(
      "attachHandler",
      Class.forName("org.sparkproject.jetty.servlet.ServletContextHandler")
    )
    attachHandlerMethod.invoke(this, servletHandler)

    super.initialize()
  }

}

object YtHistoryServer extends Logging {
  private val conf = new SparkConf

  def main(argStrings: Array[String]): Unit = {
    // almost exact copy of org.apache.spark.deploy.history.HistoryServer main method with slight changes
    Utils.initDaemon(log)
    new HistoryServerArguments(conf, argStrings)
    initSecurity()
    val securityManager = createSecurityManager(conf)

    val providerName = conf.get(History.PROVIDER)
      .getOrElse(classOf[FsHistoryProvider].getName)
    val provider = Utils.classForName[ApplicationHistoryProvider](providerName)
      .getConstructor(classOf[SparkConf])
      .newInstance(conf)

    val port = conf.get(History.HISTORY_SERVER_UI_PORT)

    val server = new YtHistoryServer(conf, provider, securityManager, port)
    server.bind()
    provider.start()
    AddressUtils.writeAddressToFile("history", server.publicHostName, server.boundPort, None, None)

    ShutdownHookManager.addShutdownHook { () => server.stop() }

    // Wait until the end of the world... or if the HistoryServer process is manually stopped
    while(true) { Thread.sleep(Int.MaxValue) }
  }
}