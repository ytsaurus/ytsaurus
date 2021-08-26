package ru.yandex.spark.launcher

import net.logstash.log4j.JSONEventLayoutV1
import org.apache.commons.io.FileUtils
import org.apache.log4j.spi.{LoggingEvent, RootLogger}
import org.apache.log4j.{FileAppender, Level}
import org.scalatest.{FlatSpec, Matchers}
import ru.yandex.inside.yt.kosher.ytree.YTreeNode
import ru.yandex.spark.launcher.WorkerLogLauncher.WorkerLogConfig
import ru.yandex.spark.yt.test.{LocalYtClient, TmpDir}
import ru.yandex.spark.yt.wrapper.YtWrapper
import ru.yandex.spark.yt.wrapper.model.WorkerLogSchema.schema

import java.io.{File, FileWriter}
import java.nio.file.attribute.FileTime
import java.nio.file.{Files, Path, Paths}
import java.time.{LocalDateTime, ZoneOffset}
import java.util.UUID
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

class WorkerLogLauncherTest extends FlatSpec with LocalYtClient with Matchers with TmpDir {
  behavior of "WorkerLogLauncherTest"

  override def testDir: String = "/tmp/test" // should start with single slash

  private val tablesDir: String = tmpPath // should start with single slash

  private val testLogDir: String = s"/tmp/spytTest"
  private val tmpLogDir: String = s"$testLogDir/test-${UUID.randomUUID()}"
  private val config = WorkerLogConfig(
    enableService = true,
    enableJson = true,
    scanDirectory = tmpLogDir,
    tablesPath = tablesDir,
    updateInterval = 20 seconds,
    bufferSize = 1000,
    ytTableRowLimit = 500
  )

  private def createLocalDir(path: String): Unit = {
    FileUtils.forceMkdir(new File(path))
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    createLocalDir(testLogDir)
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    YtWrapper.createDir(tablesDir, None, ignoreExisting = true)
    FileUtils.deleteQuietly(new File(tmpLogDir))
    createLocalDir(tmpLogDir)
  }

  override def afterEach(): Unit = {
    FileUtils.deleteQuietly(new File(tmpLogDir))
    super.afterEach()
  }

  override def afterAll(): Unit = {
    FileUtils.forceDelete(new File(testLogDir))
    super.afterAll()
  }

  private def getAppFolderName(id: String): String = s"app-$id"

  private def getDriverFolderName(id: String): String = s"driver-$id"

  private def getAppFolderPath(id: String): String = s"$tmpLogDir/${getAppFolderName(id)}"

  private def getDriverFolderPath(id: String): String = s"$tmpLogDir/${getDriverFolderName(id)}"

  private def getExecutorFolderPath(appId: String, exId: String): String = s"${getAppFolderPath(appId)}/$exId"

  private def createApp(id: String): Unit = createLocalDir(getAppFolderPath(id))

  private def createDriver(id: String): Unit = createLocalDir(getDriverFolderPath(id))

  private def createExecutor(appId: String, exId: String): Unit = createLocalDir(getExecutorFolderPath(appId, exId))

  private val STDOUT = "stdout"
  private val STDERR = "stderr"

  // pipe = STDOUT or STDERR
  private def getDriverFileLog(driverId: String, pipe: String): String =
    s"${getDriverFolderPath(driverId)}/$pipe"

  private def getExecutorFileLog(appId: String, exId: String, pipe: String): String =
    s"${getExecutorFolderPath(appId, exId)}/$pipe"

  private case class TestEventHolder(timestamp: Long,
                                     level: Option[String],
                                     message: String,
                                     throwable: Option[String] = None) {
    def toEvent: LoggingEvent = {
      new LoggingEvent(
        "class",
        new RootLogger(Level.ALL),
        timestamp,
        level.map(Level.toLevel)
          .getOrElse(throw new RuntimeException("Cannot write event with null level")),
        message,
        throwable.map(new Throwable(_)).orNull
      )
    }
  }

  private object TestEventHolder {
    def apply(node: YTreeNode): TestEventHolder = {
      val logBlock = WorkerLogBlock(node)

      TestEventHolder(
        LocalDateTime.parse(logBlock.inner.dateTime, WorkerLogBlock.formatter)
          .toInstant(ZoneOffset.UTC).toEpochMilli,
        logBlock.inner.level,
        logBlock.inner.message,
        logBlock.inner.exceptionMessage
      )
    }
  }

  private val simpleInfoEvent = TestEventHolder(1, Some(Level.INFO.toString), "messageInfo")
  private val simpleInfoEvent2 = TestEventHolder(657283675, Some(Level.INFO.toString), "messageInfo2")

  private val simpleErrorEvent = TestEventHolder(
    234, Some(Level.ERROR.toString), "messageError", Some("throwable")
  )
  private val simpleErrorEvent2 = TestEventHolder(
    99999999, Some(Level.ERROR.toString), "messageError2", Some("throwable2")
  )

  private type LogArray = Array[Seq[TestEventHolder]]
  private def getAllLogs: LogArray = {
    val logTables = YtWrapper.listDir(tablesDir)
    val allLogs = logTables.map { table =>
      YtWrapper.selectRows(s"${config.tablesPath}/$table", schema).map(TestEventHolder(_))
    }
    allLogs
  }

  private def writeToLog(filePath: String, events: Seq[TestEventHolder]): Unit = {
    val stream = new FileAppender(new JSONEventLayoutV1, filePath)
    try {
      events.foreach(e => stream.append(e.toEvent))
    } finally {
      stream.close()
    }
  }

  private def rawWriteToLog(filePath: String, content: String): Unit = {
    val fw = new FileWriter(filePath, true)
    try {
      fw.write(content)
    } finally {
      fw.close()
    }
  }

  private def finalizeDriverLog(driverId: String, pipe: String): Unit = {
    Files.createFile(Paths.get(s"${getDriverFileLog(driverId, pipe)}_finished"))
  }

  private def finalizeExecutorLog(appId: String, exId: String, pipe: String): Unit = {
    Files.createFile(Paths.get(s"${getExecutorFileLog(appId, exId, pipe)}_finished"))
  }

  it should "not upload any logs on start" in {
    getAllLogs.isEmpty shouldBe true

    val app = "example"
    createApp(app)
    createExecutor(app, "example")
    createDriver("example")

    val workerLogService = new LogServiceRunnable(config)
    workerLogService.uploadLogs()

    getAllLogs.isEmpty shouldBe true
  }

  it should "read and write driver log" in {
    val driver = "example"
    createDriver(driver)

    val workerLogService = new LogServiceRunnable(config)

    writeToLog(getDriverFileLog(driver, STDOUT), List(simpleInfoEvent, simpleInfoEvent2))
    writeToLog(getDriverFileLog(driver, STDERR), List(simpleErrorEvent, simpleErrorEvent2))

    finalizeDriverLog(driver, STDOUT)
    finalizeDriverLog(driver, STDERR)

    workerLogService.uploadLogs()

    val allLogs = getAllLogs
    allLogs should contain theSameElementsAs Seq(
      Seq(simpleErrorEvent, simpleInfoEvent),
      Seq(simpleInfoEvent2),
      Seq(simpleErrorEvent2)
    )
  }

  it should "read and write application log" in {
    val app = "example"
    createApp(app)

    val executor1 = "exampleE1"
    createExecutor(app, executor1)

    val executor2 = "exampleE2"
    createExecutor(app, executor2)

    val workerLogService = new LogServiceRunnable(config)

    writeToLog(getExecutorFileLog(app, executor1, STDOUT), List(simpleInfoEvent, simpleInfoEvent2))
    writeToLog(getExecutorFileLog(app, executor2, STDERR), List(simpleErrorEvent))
    finalizeExecutorLog(app, executor1, STDOUT)
    finalizeExecutorLog(app, executor2, STDERR)
    workerLogService.uploadLogs()
    val allLogs = getAllLogs

    allLogs should contain theSameElementsAs Seq(
      Seq(simpleInfoEvent, simpleErrorEvent),
      Seq(simpleInfoEvent2)
    )
  }

  it should "read and write over limit logs" in {
    val driver = "example"
    createDriver(driver)

    val limitInfoEvent1 = simpleInfoEvent.copy(message = "L" * config.ytTableRowLimit)
    val limitInfoEvent2 = simpleInfoEvent.copy(message = "OG")
    val workerLogService = new LogServiceRunnable(config)

    writeToLog(getDriverFileLog(driver, STDOUT), List(simpleInfoEvent.copy(message = limitInfoEvent1.message + limitInfoEvent2.message)))

    finalizeDriverLog(driver, STDOUT)

    workerLogService.uploadLogs()

    val allLogs = getAllLogs
    allLogs should contain theSameElementsAs Seq(
      Seq(limitInfoEvent1, limitInfoEvent2)
    )
  }

  it should "read and write json and non-json logs" in {
    val nonJsonLog = "{Test}"

    val driver = "example"
    createDriver(driver)

    val workerLogService = new LogServiceRunnable(config)

    writeToLog(getDriverFileLog(driver, STDOUT), List(simpleInfoEvent))
    rawWriteToLog(getDriverFileLog(driver, STDOUT), s"$nonJsonLog\n")
    writeToLog(getDriverFileLog(driver, STDOUT), List(simpleInfoEvent2))
    finalizeDriverLog(driver, STDOUT)
    workerLogService.uploadLogs()

    val creationTime = Files.getAttribute(Path.of(getDriverFileLog(driver, STDOUT)), "creationTime")
      .asInstanceOf[FileTime].toMillis

    val allLogs = getAllLogs

    allLogs should contain theSameElementsAs Seq(
      Seq(TestEventHolder(creationTime, None, nonJsonLog)),
      Seq(simpleInfoEvent),
      Seq(simpleInfoEvent2)
    )
  }

  it should "write duplicate lines" in {
    val app = "example"
    createApp(app)

    val executor = "exampleE"
    createExecutor(app, executor)

    val workerLogService = new LogServiceRunnable(config)

    writeToLog(getExecutorFileLog(app, executor, STDOUT), List(simpleInfoEvent, simpleInfoEvent))
    finalizeExecutorLog(app, executor, STDOUT)
    workerLogService.uploadLogs()
    val allLogs = getAllLogs

    allLogs should contain theSameElementsAs Seq(
      Seq(simpleInfoEvent, simpleInfoEvent)
    )
  }

  it should "not parse json when option disabled" in {
    val nonJsonLog = "{Test}"
    val simpleLogEventJson = new JSONEventLayoutV1().format(simpleInfoEvent.toEvent).strip()

    val app = "example"
    createApp(app)

    val executor = "exampleE"
    createExecutor(app, executor)

    val workerLogService = new LogServiceRunnable(config.copy(enableJson = false))
    writeToLog(getExecutorFileLog(app, executor, STDERR), List(simpleInfoEvent))
    rawWriteToLog(getExecutorFileLog(app, executor, STDERR), s"$nonJsonLog\n")
    finalizeExecutorLog(app, executor, STDERR)
    workerLogService.uploadLogs()

    val creationTime = Files.getAttribute(Path.of(getExecutorFileLog(app, executor, STDERR)), "creationTime")
      .asInstanceOf[FileTime].toMillis

    val allLogs = getAllLogs

    allLogs should contain theSameElementsAs Seq(
      Seq(TestEventHolder(creationTime, None, simpleLogEventJson), TestEventHolder(creationTime, None, nonJsonLog))
    )
  }

  it should "not upload changes after setting finished state" in {
    val driver = "example"
    createDriver(driver)

    val workerLogService = new LogServiceRunnable(config)

    writeToLog(getDriverFileLog(driver, STDOUT), List(simpleInfoEvent))
    writeToLog(getDriverFileLog(driver, STDERR), List(simpleErrorEvent))
    finalizeDriverLog(driver, STDOUT)
    finalizeDriverLog(driver, STDERR)
    workerLogService.uploadLogs()
    val allLogsBefore = getAllLogs

    writeToLog(getDriverFileLog(driver, STDOUT), List(simpleInfoEvent2))
    writeToLog(getDriverFileLog(driver, STDERR), List(simpleErrorEvent2))
    workerLogService.uploadLogs()
    val allLogsAfter = getAllLogs

    allLogsBefore shouldBe allLogsAfter
  }

  private def writeMakeAttemptsAndRead(attemptCnt: Int): (LogArray, LogArray, LogArray) = {
    val driver = "example"
    createDriver(driver)

    val workerLogService = new LogServiceRunnable(config)

    writeToLog(getDriverFileLog(driver, STDOUT), List(simpleInfoEvent))
    workerLogService.uploadLogs()
    val allLogsBefore = getAllLogs

    val (sL1, sL2) = new JSONEventLayoutV1().format(simpleInfoEvent2.toEvent).splitAt(2)
    rawWriteToLog(getDriverFileLog(driver, STDOUT), sL1)
    (0 until attemptCnt).foreach(_ => workerLogService.uploadLogs())
    val allLogsAfterReadAttempts = getAllLogs

    rawWriteToLog(getDriverFileLog(driver, STDOUT), sL2)
    finalizeDriverLog(driver, STDOUT)
    workerLogService.uploadLogs()
    val allLogsAfterWriteSecondPart = getAllLogs

    (allLogsBefore, allLogsAfterReadAttempts, allLogsAfterWriteSecondPart)
  }

  it should "read successfully after error attempt" in {
    val (_, _, allLogsAfterWriteSecondPart) = writeMakeAttemptsAndRead(1)

    allLogsAfterWriteSecondPart should contain theSameElementsAs Seq(
      Seq(simpleInfoEvent),
      Seq(simpleInfoEvent2)
    )
  }

  it should "support multiple write, upload and read" in {
    val driver = "example"
    createDriver(driver)

    val eventCnt = 100
    val groupCnt = 10
    val events = (0 until eventCnt).map(x => TestEventHolder(x, Some(Level.INFO.toString), ""))
    val logSnaps = new ArrayBuffer[LogArray](groupCnt)

    val workerLogService = new LogServiceRunnable(config)

    val thread = new Thread(() => {
      events.grouped(groupCnt).foreach {
        event =>
          writeToLog(getDriverFileLog(driver, STDOUT), event)
          workerLogService.uploadLogs()
          logSnaps += getAllLogs
      }
      finalizeDriverLog(driver, STDOUT)
      workerLogService.uploadLogs()
      logSnaps(groupCnt - 1) = getAllLogs
    })

    thread.start()
    thread.join()

    logSnaps.foreach {
      logSnap =>
        logSnap.foreach {
          seq =>
            seq.zipWithIndex.forall {
              case (event, i) => event.timestamp == i
            } shouldBe true
        }
    }

    logSnaps(groupCnt - 1) shouldBe List(events)

    def getLengthOfHead(a: LogArray): Int = a.headOption.map(_.length).getOrElse(0)
    (0 until groupCnt - 1).foreach {
      i =>
        getLengthOfHead(logSnaps(i)) should be <= getLengthOfHead(logSnaps(i + 1))
    }
  }
}
