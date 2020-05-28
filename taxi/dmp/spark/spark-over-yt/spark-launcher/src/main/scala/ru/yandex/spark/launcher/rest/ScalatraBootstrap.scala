package ru.yandex.spark.launcher.rest

import javax.servlet.ServletContext
import org.scalatra.LifeCycle

class ScalatraBootstrap extends LifeCycle {
  override def init(context: ServletContext) {
    val sparkMasterEndpoint = context.getInitParameter(masterEndpointParam)
    val sparkByopEnabled = context.getInitParameter(byopEnabledParam).toBoolean
    val sparkByopPort = if (sparkByopEnabled) Some(context.getInitParameter(byopPortParam).toInt) else None
    context.mount(new MasterWrapperServlet(sparkMasterEndpoint, sparkByopPort), "/*")
  }
}
