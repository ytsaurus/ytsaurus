package org.apache.spark.deploy.history

import org.apache.spark.SparkConf
import org.apache.spark.deploy.history.WorkerLogReader.getLogMeta
import org.apache.spark.internal.Logging
import org.apache.spark.ui.{UIUtils, WebUIPage}
import tech.ytsaurus.spyt.fs.YtClientConfigurationConverter
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.wrapper.client.YtClientConfiguration
import tech.ytsaurus.spyt.wrapper.model.WorkerLogBlock
import tech.ytsaurus.spyt.wrapper.model.WorkerLogSchema.getMetaPath
import tech.ytsaurus.client.CompoundClient
import tech.ytsaurus.spyt.wrapper.client.YtClientProvider

import java.util.UUID
import javax.servlet.http.HttpServletRequest
import scala.xml.{Node, Unparsed}

class YtLogPage(conf: SparkConf) extends WebUIPage("workerLogPage") with Logging {
  private val supportedLogTypes = Set("stderr", "stdout")
  private val defaultRows = 200

  private val id: String = UUID.randomUUID().toString
  protected var _ytConf: YtClientConfiguration = YtClientConfigurationConverter.ytClientConfiguration(conf)
  protected lazy val yt: CompoundClient = YtClientProvider.ytClient(_ytConf, id)

  def renderLog(request: HttpServletRequest): String = {
    val (app, exec, logType, _, _, _, logText, startRow, endRow, logLength) = getLogQuery(request)
    val pre = s"==== Rows $startRow-$endRow of $logLength of $app/$exec/$logType ====\n"
    pre + logText
  }

  override def render(request: HttpServletRequest): Seq[Node] = {
    val (_, _, logType, rowLength, params, pageName, logText, startRow, endRow, logLength) = getLogQuery(request)
    val curLogLength = endRow - startRow
    val range =
      <span id="log-data">
        Showing {curLogLength} rows: {startRow.toString} - {endRow.toString} of {logLength}
      </span>

    val moreButton =
      <button type="button" onclick={"loadMore()"} class="log-more-btn btn btn-default">
        Load More
      </button>

    val newButton =
      <button type="button" onclick={"loadNew()"} class="log-new-btn btn btn-default">
        Load New
      </button>

    val alert =
      <div class="no-new-alert alert alert-info" style="display: none;">
        End of Log
      </div>

    val logParams = "?%s&logType=%s".format(params, logType)
    val jsOnload = "window.onload = " +
      s"initLogPage('$logParams', $curLogLength, $startRow, $endRow, $logLength, $rowLength);"

    val content =
      <div>
        {range}
        <div class="log-content" style="height:80vh; overflow:auto; padding:5px;">
          <div>{moreButton}</div>
          <pre>{logText}</pre>
          {alert}
          <div>{newButton}</div>
        </div>
        <script>{Unparsed(jsOnload)}</script>
      </div>

    UIUtils.basicSparkPage(request, content, logType + " log page for " + pageName)
  }

  private def getLogQuery(request: HttpServletRequest):
    (String, String, String, Int, String, String, String, Long, Long, Long) = {
    val appId = Option(request.getParameter("appId"))
    val executorId = Option(request.getParameter("executorId"))
    val driverId = Option(request.getParameter("driverId"))
    val logType = request.getParameter("logType")
    val offset = Option(request.getParameter("offset")).map(_.toLong)
    val rowLength = Option(request.getParameter("rowLength")).map(_.toInt)
      .getOrElse(defaultRows)

    val (app, exec, params, pageName) = (appId, executorId, driverId) match {
      case (Some(a), Some(e), None) =>
        (a, e, s"appId=$a&executorId=$e", s"$a/$e")
      case (None, None, Some(d)) =>
        (d, "", s"driverId=$d", d)
      case _ =>
        throw new Exception("Request must specify either application or driver identifiers")
    }

    val (logText, startRow, endRow, logLength) = getLog(app, exec, logType, offset, rowLength)
    (app, exec, logType, rowLength, params, pageName, logText, startRow, endRow, logLength)
  }

  /** Get the part of the log files given the offset and desired length of rows */
  private def getLog(
                      appId: String,
                      execId: String,
                      logType: String,
                      offsetOption: Option[Long],
                      rowLength: Int
                    ): (String, Long, Long, Long) = {

    if (!supportedLogTypes.contains(logType)) {
      return ("Error: Log type must be one of " + supportedLogTypes.mkString(", "), 0, 0, 0)
    }
    logInfo(s"configs ${conf.getAll.mkString(";")}")
    val ytLogDirectory = conf.get("spark.workerLog.tablePath")
    val meta = getLogMeta(getMetaPath(ytLogDirectory), appId, execId, logType)(yt)

    if (meta.isEmpty) {
      return (s"Error: invalid log directory, logs not found", 0, 0, 0)
    }

    try {
      val tablePath = s"$ytLogDirectory/${meta.get.tableName}"
      val totalLength = meta.get.length
      logDebug(s"Logs for $appId, $execId, $logType found $tablePath}")

      val offset = offsetOption.getOrElse(totalLength - rowLength)
      val startIndex = {
        if (offset < 0) {
          0L
        } else if (offset > totalLength) {
          totalLength
        } else {
          offset
        }
      }
      val endIndex = math.min(startIndex + rowLength, totalLength)
      logDebug(s"Getting log from $startIndex to $endIndex")
      if (!YtWrapper.exists(tablePath)(yt)) {
        return (s"Error: required table $tablePath not found", 0, 0, 0)
      }
      val blocks = WorkerLogReader.read(tablePath, appId, execId, logType, startIndex, endIndex)(yt)
      val logText = blocks.map(formatBlock).mkString("\n")
      logDebug(s"Got log of length ${logText.length} rows")
      (logText, startIndex, endIndex, totalLength)
    } catch {
      case e: Exception =>
        logError(s"Error getting $logType logs for table $appId/$execId", e)
        ("Error getting logs due to exception: " + e.getMessage, 0, 0, 0)
    }
  }

  private def formatBlock(block: WorkerLogBlock): String = {
    val inner = block.inner
    val data = Seq(
      Some(inner.dateTime), Some(inner.loggerName), inner.level,
      inner.sourceHost, inner.file, inner.lineNumber,
      inner.thread, Some(inner.message), inner.exceptionClass,
      inner.exceptionMessage, inner.stack).flatten
    data.mkString(": ")
  }
}
