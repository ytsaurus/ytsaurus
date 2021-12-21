package ru.yandex.spark.launcher

import com.twitter.scalding.Args
import org.slf4j.LoggerFactory
import ru.yandex.spark.launcher.WorkerLogLauncher.WorkerLogConfig
import ru.yandex.spark.launcher.WorkerLogLauncher.WorkerLogConfig.getSparkHomeDir
import ru.yandex.spark.yt.wrapper.Utils.parseDuration
import ru.yandex.spark.yt.wrapper.model.WorkerLogBlock
import ru.yandex.spark.yt.wrapper.model.WorkerLogSchema.{getMetaPath, metaSchema}
import ru.yandex.spark.yt.wrapper.{LogLazy, YtWrapper}
import ru.yandex.yt.ytclient.proxy.CompoundClient

import java.io.{File, RandomAccessFile}
import java.nio.file.attribute.FileTime
import java.nio.file.{Files, Path, StandardCopyOption}
import java.time.ZoneOffset
import scala.collection.mutable
import scala.concurrent.duration.{Duration, DurationInt}
import scala.language.postfixOps

object WorkerLogLauncher extends VanillaLauncher {
  private val log = LoggerFactory.getLogger(getClass)

  case class WorkerLogConfig(enableService: Boolean,
                             enableJson: Boolean,
                             scanDirectory: String,
                             tablesPath: String,
                             updateInterval: Duration,
                             bufferSize: Int,
                             ytTableRowLimit: Int,
                             tableTTL: Duration
                            )

  object WorkerLogConfig {
    val minimalInterval: Duration = 1 minute
    def create(sparkConf: Map[String, String], args: Array[String]): Option[WorkerLogConfig] = {
      create(sparkConf, Args(args))
    }

    def getSparkHomeDir: String = {
      new File(env("SPARK_HOME", "/slot/sandbox/tmpfs/spark")).getAbsolutePath
    }

    def getSparkWorkDir: String = {
      sys.env.getOrElse("SPARK_WORKER_DIR", s"$getSparkHomeDir/work")
    }

    def create(sparkConf: Map[String, String], args: Args): Option[WorkerLogConfig] = {
      val userUpdateInterval = args.optional("wlog-update-interval")
        .orElse(sparkConf.get("spark.workerLog.updateInterval"))
        .map(parseDuration).getOrElse(10 minutes)
      val tablesPathOpt = args.optional("wlog-table-path")
        .orElse(sparkConf.get("spark.workerLog.tablePath"))
      val enableService = args.optional("wlog-service-enabled")
        .orElse(sparkConf.get("spark.workerLog.enableService"))
        .exists(_.toBoolean)

      if (!enableService) {
        None
      } else if (tablesPathOpt.isEmpty) {
        log.warn(s"Path to worker log yt directory is not defined, WorkerLogService couldn't be started")
        None
      } else {
        val updateInterval = if (userUpdateInterval < minimalInterval) {
          log.warn(s"Update interval that less than $minimalInterval doesn't allowed, $minimalInterval will be used")
          minimalInterval
        } else {
          userUpdateInterval
        }

        Some(WorkerLogConfig(
          enableService = enableService,
          enableJson = args.optional("wlog-enable-json")
            .orElse(sparkConf.get("spark.workerLog.enableJson"))
            .exists(_.toBoolean),
          scanDirectory = args.optional("wlog-file-log-path")
            .orElse(sparkConf.get("spark.workerLog.fileLogPath"))
            .getOrElse(getSparkWorkDir),
          tablesPath = tablesPathOpt.get,
          updateInterval = updateInterval,
          bufferSize = args.optional("wlog-buffer-size")
            .orElse(sparkConf.get("spark.workerLog.bufferSize"))
            .map(_.toInt).getOrElse(100),
          ytTableRowLimit = args.optional("wlog-yttable-row-limit")
            .orElse(sparkConf.get("spark.hadoop.yt.dynTable.rowSize"))
            .map(_.toInt).getOrElse(16777216),
          tableTTL = args.optional("wlog-table-ttl")
            .orElse(sparkConf.get("spark.workerLog.tableTTL"))
            .map(parseDuration).getOrElse(7 days)
        ))
      }
    }
  }

  def start(workerLogConfig: WorkerLogConfig, client: CompoundClient): Thread = {
    if (workerLogConfig.enableJson) {
      val path = Files.move(
        Path.of(getSparkHomeDir, "conf", "log4j.workerLogJson.properties"),
        Path.of(getSparkHomeDir, "conf", "log4j.properties"),
        StandardCopyOption.REPLACE_EXISTING
      )
      if (path == null) {
        throw new RuntimeException("Couldn't replace log4j properties")
      }
    }
    val thread = new Thread(new LogServiceRunnable(workerLogConfig)(client), "WorkerLogRunnable")
    thread.start()
    thread
  }
}

class LogServiceRunnable(workerLogConfig: WorkerLogConfig)(implicit yt: CompoundClient) extends Runnable with LogLazy {
  val fileMeta: mutable.Map[String, (Long, Long)] = mutable.HashMap[String, (Long, Long)]().withDefaultValue((0L, 0L))
  val finishedPaths: mutable.Set[String] = mutable.HashSet[String]()
  private val log = LoggerFactory.getLogger(getClass)

  private[launcher] def init(): Unit = {
    YtWrapper.createDir(workerLogConfig.tablesPath, None, ignoreExisting = true)
    YtWrapper.createDynTableAndMount(getMetaPath(workerLogConfig.tablesPath), metaSchema)
  }

  override def run(): Unit = {
    try {
      log.info(s"WorkerLog configuration: $workerLogConfig")
      init()
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
        log.traceLazy(s"Found log file $logPath")
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
            writer.setCreationTime(appDriver, execId, logFileName, creationTime)
            val ans = readUntilEnd(reader, ignoreLast)
            val (lastSeek, length) = ans.foldLeft((currentSeek, 0)) {
              case ((_, len), (seek, line)) =>
                val block = if (workerLogConfig.enableJson) {
                  WorkerLogBlock.fromJson(line, logFileName, appDriver, execId, currentLine + len, creationTime)
                } else {
                  WorkerLogBlock.fromMessage(line, logFileName, appDriver, execId, currentLine + len, creationTime)
                }
                val written = writer.write(block)
                (seek, len + written)
            }
            if (!fileMeta.contains(logPath) && length == 0) {
              writer.newEmptyFile(appDriver, execId, logFileName)
            }
            fileMeta(logPath) = (lastSeek, currentLine + length)
          } finally reader.close()
        }
      }
  }

  def uploadLogs(): Unit = {
    log.debugLazy("Started scanning logs")
    val appsFolder = new File(workerLogConfig.scanDirectory)
    if (appsFolder.exists()) {
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
    } else {
      log.debugLazy("No logs folder")
    }

    log.debugLazy("Finished scanning logs")
  }
}