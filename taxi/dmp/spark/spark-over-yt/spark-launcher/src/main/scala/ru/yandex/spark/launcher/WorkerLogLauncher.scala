package ru.yandex.spark.launcher

import com.twitter.scalding.Args
import org.slf4j.LoggerFactory
import ru.yandex.spark.launcher.WorkerLogLauncher.WorkerLogConfig
import ru.yandex.spark.yt.wrapper.Utils.parseDuration
import ru.yandex.spark.yt.wrapper.YtWrapper
import ru.yandex.yt.ytclient.proxy.CompoundClient

import java.io.{File, RandomAccessFile}
import java.nio.file.attribute.FileTime
import java.nio.file.{Files, Path}
import java.time.ZoneOffset
import scala.collection.mutable
import scala.concurrent.duration.{Duration, DurationInt}
import scala.language.postfixOps

object WorkerLogLauncher extends VanillaLauncher {
  private val log = LoggerFactory.getLogger(getClass)

  case class WorkerLogConfig(enableService: Boolean,
                             enableJsonParsing: Boolean,
                             scanDirectory: String,
                             tablesPath: String,
                             updateInterval: Duration,
                             bufferSize: Int,
                             ytTableRowLimit: Int)

  object WorkerLogConfig {
    val minimalInterval: Duration = 1 minute
    def create(sparkConf: Map[String, String], args: Array[String]): WorkerLogConfig = {
      create(sparkConf, Args(args))
    }

    def create(sparkConf: Map[String, String], args: Args): WorkerLogConfig = {
      val userUpdateInterval = args.optional("wlog-update-interval")
        .orElse(sparkConf.get("spark.workerLog.updateInterval"))
        .map(parseDuration).getOrElse(10 minutes)

      val updateInterval = if (userUpdateInterval < minimalInterval) {
        log.warn(s"Update interval that less than $minimalInterval doesn't allowed, $minimalInterval will be used")
        minimalInterval
      } else {
        userUpdateInterval
      }

      WorkerLogConfig(
        enableService = args.optional("wlog-service-enabled")
                .orElse(sparkConf.get("spark.workerLog.enableService"))
                .exists(_.toBoolean),
        enableJsonParsing = args.optional("wlog-enable-json-parsing")
                .orElse(sparkConf.get("spark.workerLog.enableJsonParsing"))
                .exists(_.toBoolean),
        scanDirectory = args.optional("wlog-file-log-path")
                .orElse(sparkConf.get("spark.workerLog.fileLogPath"))
                .orElse(sys.env.get("SPARK_WORKER_DIR"))
                .getOrElse(s"${new File(env("SPARK_HOME", "./spark")).getAbsolutePath}/work"),
        tablesPath = args.optional("wlog-table-path")
                .orElse(sparkConf.get("spark.workerLog.tablePath")).get,
        updateInterval = updateInterval,
        bufferSize = args.optional("wlog-buffer-size")
          .orElse(sparkConf.get("spark.workerLog.bufferSize"))
          .map(_.toInt).getOrElse(10),
        ytTableRowLimit = args.optional("wlog-yttable-row-limit")
          .orElse(sparkConf.get("spark.hadoop.yt.dynTable.rowSize"))
          .map(_.toInt).getOrElse(16777216)
      )
    }
  }

  def start(workerLogConfig: WorkerLogConfig, client: CompoundClient): Thread = {
    val thread = new Thread(new LogServiceRunnable(workerLogConfig)(client), "WorkerLogRunnable")
    thread.start()
    thread
  }
}

class LogServiceRunnable(workerLogConfig: WorkerLogConfig)(implicit yt: CompoundClient) extends Runnable {
  val fileMeta: mutable.Map[String, (Long, Long)] = mutable.HashMap[String, (Long, Long)]().withDefaultValue((0L, 0L))
  val finishedPaths: mutable.Set[String] = mutable.HashSet[String]()
  private val log = LoggerFactory.getLogger(getClass)

  override def run(): Unit = {
    try {
      log.info(s"WorkerLogConfig $workerLogConfig")
      YtWrapper.createDir(workerLogConfig.tablesPath, None, ignoreExisting = true)
      while (!Thread.interrupted()) {
        try {
          uploadLogs()
        } catch {
          case e: Exception =>
            log.error(s"Error while uploading logs: ${e.getMessage}")
        }
        Thread.sleep(workerLogConfig.updateInterval.toMillis)
      }
    } catch {
      case e: InterruptedException =>
        log.info("WorkerLogService was interrupted")
        throw e
    }
  }

  private def readUntilEnd(reader: RandomAccessFile, ignoreLast: Boolean): Stream[(Long, String)] = {
    val stream = Stream
      .continually((Option(reader.readLine()), reader.getFilePointer))
      .takeWhile{ case (line, _) => line.nonEmpty }
      .map { case (line, seek) => (seek, line.get) }
    if (ignoreLast) stream.dropRight(1)
    else stream
  }

  private def checkFileFinished(file: File): Boolean = {
    new File(s"${file.getParentFile.getAbsolutePath}/${file.getName}_finished").exists()
  }

  private val writer = new WorkerLogWriter(workerLogConfig)

  private def processStdoutStderrData(directory: File, appDriver: String, execId: String = ""): Unit = {
    directory.list()
      .filter(x => x == "stdout" || x == "stderr")
      .foreach { logFileName =>
        val logPath = s"${directory.getAbsolutePath}/$logFileName"
        val logFile = new File(logPath)
        log.debug(s"Found log file $logPath")
        val creationTime = Files.getAttribute(Path.of(logPath), "creationTime")
          .asInstanceOf[FileTime].toInstant.atOffset(ZoneOffset.UTC).toLocalDateTime
        if (finishedPaths.contains(logPath)) {
          Nil
        } else {
          val ignoreLast = if (checkFileFinished(logFile)) {
            finishedPaths.add(logPath)
            false
          } else {
            true
          }
          val reader = new RandomAccessFile(logFile, "r")
          try {
            val (currentSeek, currentLine) = fileMeta(logPath)
            reader.seek(currentSeek)
            val ans = readUntilEnd(reader, ignoreLast)
            val (lastSeek, length) = ans.foldLeft((currentSeek, 0)) {
              case ((_, len), (seek, line)) =>
                val block = if (workerLogConfig.enableJsonParsing) {
                  WorkerLogBlock.fromJson(line, logFileName, appDriver, execId, currentLine + len, creationTime)
                } else {
                  WorkerLogBlock.fromMessage(line, logFileName, appDriver, execId, currentLine + len, creationTime)
                }
                val written = writer.write(block)
                (seek, len + written)
            }
            fileMeta(logPath) = (lastSeek, currentLine + length)
          } finally reader.close()
        }
      }
  }

  def uploadLogs(): Unit = {
    log.info("Started scanning logs")
    val appsFolder = new File(workerLogConfig.scanDirectory)
    appsFolder.list().foreach { dirName =>
      val fullPath = s"${appsFolder.getAbsolutePath}/$dirName"
      val file = new File(fullPath)
      if (file.getName.startsWith("app-")) {
        file.list().foreach { executorId =>
          val executorPath = s"${file.getAbsolutePath}/$executorId"
          processStdoutStderrData(new File(executorPath), dirName, executorId)
        }
      } else if (file.getName.startsWith("driver-")) {
        processStdoutStderrData(file, dirName)
      } else {
        Nil
      }
    }
    writer.flush()

    log.info("Finished scanning logs")
  }
}