package ru.yandex.spark.launcher.rest

import com.codahale.metrics.MetricRegistry
import org.eclipse.jetty.server.{Connector, Server, ServerConnector}
import org.eclipse.jetty.servlet.ServletHolder
import org.eclipse.jetty.util.log.Slf4jLog
import org.eclipse.jetty.util.thread.QueuedThreadPool
import org.eclipse.jetty.webapp.WebAppContext
import org.slf4j.LoggerFactory

import java.io.Closeable
import javax.servlet.http.{HttpServlet, HttpServletRequest, HttpServletResponse}


object AutoScalerMetricsServer {
  private val log = LoggerFactory.getLogger(AutoScalerMetricsServer.getClass)

  private def createServer(port: Int): Server = {
    val threadPool = new QueuedThreadPool()
    threadPool.setDaemon(true)
    threadPool.setName("AutoScalerMetricsServer")
    val server = new Server(threadPool)
    val connector = new ServerConnector(server)
    connector.setPort(port)
    server.setConnectors(Array[Connector](connector))
    server
  }

  def start(port: Int, registry: MetricRegistry): Closeable = {
    val server = createServer(port)
    val context = new WebAppContext()
    context.addServlet(new ServletHolder(new PrometheusServlet(registry)), "/")
    context.setContextPath("/")
    context.setResourceBase("src/main/webapp")
    context.setLogger(new Slf4jLog())
    server.setHandler(context)
    server.start()
    log.info(s"Started AutoScalerMetricsServer on port $port")
    () => {
      log.info(s"Stopping AutoScalerMetricsServer")
      server.stop()
    }
  }
}

/**
 * Taken from org.apache.spark.metrics.sink.PrometheusServler
 */
class PrometheusServlet(registry: MetricRegistry) extends HttpServlet {
  private val log = LoggerFactory.getLogger(classOf[PrometheusServlet])

  override def init(): Unit = {
    super.init()
    log.info("Initialized PrometheusServlet")
  }

  override def doGet(req: HttpServletRequest, resp: HttpServletResponse): Unit = {
    try {
      resp.setContentType("text/plain;charset=utf-8")
      resp.setStatus(HttpServletResponse.SC_OK)
      val metrics = getMetricsSnapshot(req)
      log.info(s"metrics requested:\n===\n$metrics\n===\n")
      resp.getWriter.print(metrics)
    } catch {
      case e: IllegalArgumentException =>
        resp.sendError(HttpServletResponse.SC_BAD_REQUEST, e.getMessage)
      case e: Exception =>
        log.warn(s"GET ${req.getRequestURI} failed: $e", e)
        throw e
    }
  }

  def getMetricsSnapshot(_req: HttpServletRequest): String = {
    import scala.collection.JavaConverters._

    val guagesLabel = """{type="gauges"}"""
    val countersLabel = """{type="counters"}"""
    val metersLabel = countersLabel
    val histogramslabels = """{type="histograms"}"""
    val timersLabels = """{type="timers"}"""

    val sb = new StringBuilder()
    registry.getGauges.asScala.foreach { case (k, v) =>
      if (!v.getValue.isInstanceOf[String]) {
        sb.append(s"${normalizeKey(k)}Value$guagesLabel ${v.getValue}\n")
      }
    }
    registry.getCounters.asScala.foreach { case (k, v) =>
      sb.append(s"${normalizeKey(k)}Count$countersLabel ${v.getCount}\n")
    }
    registry.getHistograms.asScala.foreach { case (k, h) =>
      val snapshot = h.getSnapshot
      val prefix = normalizeKey(k)
      sb.append(s"${prefix}Count$histogramslabels ${h.getCount}\n")
      sb.append(s"${prefix}Max$histogramslabels ${snapshot.getMax}\n")
      sb.append(s"${prefix}Mean$histogramslabels ${snapshot.getMean}\n")
      sb.append(s"${prefix}Min$histogramslabels ${snapshot.getMin}\n")
      sb.append(s"${prefix}50thPercentile$histogramslabels ${snapshot.getMedian}\n")
      sb.append(s"${prefix}75thPercentile$histogramslabels ${snapshot.get75thPercentile}\n")
      sb.append(s"${prefix}95thPercentile$histogramslabels ${snapshot.get95thPercentile}\n")
      sb.append(s"${prefix}98thPercentile$histogramslabels ${snapshot.get98thPercentile}\n")
      sb.append(s"${prefix}99thPercentile$histogramslabels ${snapshot.get99thPercentile}\n")
      sb.append(s"${prefix}999thPercentile$histogramslabels ${snapshot.get999thPercentile}\n")
      sb.append(s"${prefix}StdDev$histogramslabels ${snapshot.getStdDev}\n")
    }
    registry.getMeters.entrySet.iterator.asScala.foreach { kv =>
      val prefix = normalizeKey(kv.getKey)
      val meter = kv.getValue
      sb.append(s"${prefix}Count$metersLabel ${meter.getCount}\n")
      sb.append(s"${prefix}MeanRate$metersLabel ${meter.getMeanRate}\n")
      sb.append(s"${prefix}OneMinuteRate$metersLabel ${meter.getOneMinuteRate}\n")
      sb.append(s"${prefix}FiveMinuteRate$metersLabel ${meter.getFiveMinuteRate}\n")
      sb.append(s"${prefix}FifteenMinuteRate$metersLabel ${meter.getFifteenMinuteRate}\n")
    }
    registry.getTimers.entrySet.iterator.asScala.foreach { kv =>
      val prefix = normalizeKey(kv.getKey)
      val timer = kv.getValue
      val snapshot = timer.getSnapshot
      sb.append(s"${prefix}Count$timersLabels ${timer.getCount}\n")
      sb.append(s"${prefix}Max$timersLabels ${snapshot.getMax}\n")
      sb.append(s"${prefix}Mean$timersLabels ${snapshot.getMax}\n")
      sb.append(s"${prefix}Min$timersLabels ${snapshot.getMin}\n")
      sb.append(s"${prefix}50thPercentile$timersLabels ${snapshot.getMedian}\n")
      sb.append(s"${prefix}75thPercentile$timersLabels ${snapshot.get75thPercentile}\n")
      sb.append(s"${prefix}95thPercentile$timersLabels ${snapshot.get95thPercentile}\n")
      sb.append(s"${prefix}98thPercentile$timersLabels ${snapshot.get98thPercentile}\n")
      sb.append(s"${prefix}99thPercentile$timersLabels ${snapshot.get99thPercentile}\n")
      sb.append(s"${prefix}999thPercentile$timersLabels ${snapshot.get999thPercentile}\n")
      sb.append(s"${prefix}StdDev$timersLabels ${snapshot.getStdDev}\n")
      sb.append(s"${prefix}FifteenMinuteRate$timersLabels ${timer.getFifteenMinuteRate}\n")
      sb.append(s"${prefix}FiveMinuteRate$timersLabels ${timer.getFiveMinuteRate}\n")
      sb.append(s"${prefix}OneMinuteRate$timersLabels ${timer.getOneMinuteRate}\n")
      sb.append(s"${prefix}MeanRate$timersLabels ${timer.getMeanRate}\n")
    }
    sb.toString()
  }

  private def normalizeKey(key: String): String = {
    s"metrics_${key.replaceAll("[^a-zA-Z0-9]", "_")}_"
  }
}