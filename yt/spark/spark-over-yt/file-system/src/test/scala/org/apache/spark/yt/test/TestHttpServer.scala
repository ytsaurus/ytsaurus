package org.apache.spark.yt.test

import org.apache.spark.yt.test.TestHttpServer.{Request, Response}
import org.slf4j.{Logger, LoggerFactory}
import org.sparkproject.jetty.server.handler.AbstractHandler
import org.sparkproject.jetty.server.{Server, Request => JettyRequest}

import java.net.BindException
import java.nio.charset.Charset
import javax.servlet.http.{HttpServletRequest, HttpServletResponse}
import scala.annotation.tailrec
import scala.concurrent.duration.{Duration, _}
import scala.concurrent.{Await, Promise}
import scala.util.Random
import scala.util.control.NonFatal

trait TestHttpServer extends AutoCloseable {
  def start(): Unit
  def stop(): Unit
  def port: Int

  def expect(check: Request => Boolean): TestHttpServer
  def assert(check: Request => Unit): TestHttpServer = expect(check.andThen(_ => true))
  def respond(response: Response): TestHttpServer
  def awaitResult(duration: Duration = 5.seconds): Response

  override def close(): Unit = stop()
}

object TestHttpServer {
  val log: Logger = LoggerFactory.getLogger(TestHttpServer.getClass)

  case class Response(body: Array[Byte], contentType: String, httpStatusCode: Int = 200, httpStatusLine: String = "OK")
  case class Request(body: Array[Byte], contentType: String, cookies: Map[String, String] = Map())

  val OK: Response = Response("OK".getBytes(Charset.defaultCharset()), contentType = "text/plain")
  val RANDOM_PORT_FROM = 20000
  val RANDOM_PORT_TO = 30000

  def apply(): TestHttpServer =
    apply(() => Random.nextInt(RANDOM_PORT_TO - RANDOM_PORT_FROM) + RANDOM_PORT_FROM)

  def apply(randomPort: () => Int): TestHttpServer = new TestHttpServer {
    import org.sparkproject.jetty.server.ServerConnector

    private var server: Option[Server] = None
    private var assignedPort: Int = -1

    private var respond: Response = OK
    private var expected: (Request => Boolean) = r => true

    private var promise: Promise[Response] = Promise()

    @tailrec
    def start(): Unit = {
      assignedPort = randomPort()
      try {
        val srv = new Server()
        val connector = new ServerConnector(srv)
        log.info(s"Starting test server on port $assignedPort")
        connector.setPort(assignedPort)
        srv.setConnectors(Array(connector))
        srv.setStopAtShutdown(true)
        srv.setStopTimeout(5000)
        srv.setHandler(new AbstractHandler() {
          override def handle(target: String, baseRequest: JettyRequest, request: HttpServletRequest,
                              response: HttpServletResponse): Unit = {
            baseRequest.setHandled(true)
            val req = Request(
              body = request.getInputStream.readAllBytes(),
              contentType = request.getContentType,
              cookies = Option(request.getCookies).map(_.map(c => c.getName -> c.getValue).toMap).getOrElse(Map())
            )
            log.info(s"Got request: $req body: ${new String(req.body, Charset.defaultCharset())}")
            try {
              expected(req)
              promise.success(respond)
            }
            catch {
              case NonFatal(ex) =>
                promise.failure(ex)
            }
          }
        })
        log.info(s"Started test server on $assignedPort")
        srv.start()
        server = Some(srv)
      } catch {
        case ex: BindException =>
          log.info(s"Failed to start test server on port $assignedPort")
          log.debug(s"Failed to start test server on port $assignedPort", ex)
          start()
      }
    }

    override def stop(): Unit = server.foreach(_.stop())

    override def expect(check: Request => Boolean): TestHttpServer = {
      expected = check
      this
    }

    override def respond(response: Response): TestHttpServer = {
      respond = response
      this
    }

    override def awaitResult(duration: Duration): Response =
      try {
        Await.result(promise.future, duration)
      }
      finally promise = Promise()

    override def port: Int = assignedPort
  }
}