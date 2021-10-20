package ru.yandex.spark.launcher.rest

import com.google.common.net.HostAndPort
import org.eclipse.jetty.server.{Connector, Server, ServerConnector}
import org.eclipse.jetty.servlet.DefaultServlet
import org.eclipse.jetty.util.thread.QueuedThreadPool
import org.eclipse.jetty.webapp.WebAppContext
import org.scalatra.servlet.ScalatraListener

class MasterWrapperServer(server: Server) {
  def joinThread(): Thread = {
    val thread = new Thread(() => {
      try {
        server.join()
      } catch {
        case e: Throwable =>
          server.stop()
          throw e
      }
    })
    thread.setDaemon(true)
    thread.start()
    thread
  }
}

object MasterWrapperServer {
  private def createServer(port: Int): Server = {
    val threadPool = new QueuedThreadPool()
    threadPool.setDaemon(true)

    val server = new Server(threadPool)
    val connector = new ServerConnector(server)
    connector.setPort(port)
    server.setConnectors(Array[Connector](connector))
    server
  }

  def start(port: Int, masterEndpoint: HostAndPort, byopPort: Option[Int]): MasterWrapperServer = {
    val server = createServer(port)
    val context = new WebAppContext()

    context.setContextPath("/")
    context.setResourceBase("src/main/webapp")
    context.setInitParameter(ScalatraListener.LifeCycleKey, classOf[ScalatraBootstrap].getCanonicalName)
    context.setInitParameter(masterEndpointParam, masterEndpoint.toString)
    context.setInitParameter(byopEnabledParam, byopPort.nonEmpty.toString)
    byopPort.foreach(bp => context.setInitParameter(byopPortParam, bp.toString))
    context.addEventListener(new ScalatraListener)
    context.addServlet(classOf[DefaultServlet], "/")

    server.setHandler(context)

    server.start()
    new MasterWrapperServer(server)
  }
}
