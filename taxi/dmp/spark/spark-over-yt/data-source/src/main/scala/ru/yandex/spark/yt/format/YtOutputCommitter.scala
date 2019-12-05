package ru.yandex.spark.yt.format

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{JobContext, TaskAttemptContext}
import org.apache.log4j.Logger
import org.apache.spark.internal.io.FileCommitProtocol
import ru.yandex.inside.yt.kosher.Yt
import ru.yandex.spark.yt._
import ru.yandex.spark.yt.conf.YtTableSparkSettings
import ru.yandex.spark.yt.utils._
import ru.yandex.yt.ytclient.proxy.YtClient

class YtOutputCommitter(jobId: String,
                        outputPath: String,
                        dynamicPartitionOverwrite: Boolean) extends FileCommitProtocol with Serializable {
  private val path = new Path(outputPath).toUri.getPath
  private val tmpPath = s"${path}_tmp"

  import YtOutputCommitter._
  import ru.yandex.spark.yt.conf.SparkYtInternalConfiguration._

  override def setupJob(jobContext: JobContext): Unit = {
    val conf = jobContext.getConfiguration
    val transaction = createTransaction(conf, GlobalTransaction, None)
    if (YtTableSparkSettings.isTableSorted(conf)) {
      YtTableUtils.createDir(tmpPath, Some(transaction))
    }
    setupTable(path, conf, transaction)
  }

  private def setupTable(path: String, conf: Configuration, transaction: String): Unit = {
    if (!YtTableUtils.exists(path)) {
      val options = YtTableSparkSettings.deserialize(conf)
      YtTableUtils.createTable(path, options, transaction)
      GlobalTableSettings.setTransaction(path, transaction)
    }
  }

  override def setupTask(taskContext: TaskAttemptContext): Unit = {
    val conf = taskContext.getConfiguration
    val globalTransaction = YtOutputCommitter.getGlobalWriteTransaction(conf)
    val transaction = createTransaction(conf, Transaction, Some(globalTransaction))
    if (YtTableSparkSettings.isTableSorted(conf)) {
      setupTable(newTaskTempFile(taskContext, None, ""), conf, transaction)
    }
  }

  override def abortJob(jobContext: JobContext): Unit = {
    GlobalTableSettings.removeTransaction(path)
    abortTransaction(jobContext.getConfiguration, GlobalTransaction)
  }

  override def abortTask(taskContext: TaskAttemptContext): Unit = {
    abortTransaction(taskContext.getConfiguration, Transaction)
  }

  override def commitJob(jobContext: JobContext, taskCommits: Seq[FileCommitProtocol.TaskCommitMessage]): Unit = {
    val conf = jobContext.getConfiguration
    val globalTransaction = YtOutputCommitter.getGlobalWriteTransaction(conf)
    if (YtTableSparkSettings.isTableSorted(conf)) {
      YtTableUtils.mergeTables(tmpPath, path, sorted = true, Some(globalTransaction))
      YtTableUtils.removeDir(tmpPath, recursive = true, Some(globalTransaction))
    }
    GlobalTableSettings.removeTransaction(path)
    commitTransaction(jobContext.getConfiguration, GlobalTransaction)
  }

  override def commitTask(taskContext: TaskAttemptContext): FileCommitProtocol.TaskCommitMessage = {
    commitTransaction(taskContext.getConfiguration, Transaction)
    null
  }

  override def newTaskTempFile(taskContext: TaskAttemptContext, dir: Option[String], ext: String): String = {
    if (YtTableSparkSettings.isTableSorted(taskContext.getConfiguration)) {
      s"${path}_tmp/part-${taskContext.getTaskAttemptID.getTaskID.getId}"
    } else path
  }

  override def newTaskTempFileAbsPath(taskContext: TaskAttemptContext, absoluteDir: String, ext: String): String = path
}

object YtOutputCommitter {

  import ru.yandex.spark.yt.conf._
  import SparkYtInternalConfiguration._

  private val log = Logger.getLogger(getClass)

  implicit lazy val yt: YtClient = YtClientProvider.ytClient
  implicit lazy val ytHttp: Yt = YtClientProvider.httpClient

  def createTransaction(conf: Configuration, confEntry: StringConfigEntry, parent: Option[String]): String = {
    val transactionGuid = YtTableUtils
      .createTransaction(parent)(YtClientProvider.ytClient(YtClientConfigurationConverter(conf)))
      .toString
    log.debugLazy(s"Create write transaction: $transactionGuid")
    conf.setYtConf(confEntry, transactionGuid)
    transactionGuid
  }

  def abortTransaction(conf: Configuration, confEntry: StringConfigEntry): Unit = {
    val transactionGuid = conf.ytConf(confEntry)
    log.debugLazy(s"Abort write transaction: $transactionGuid")
    YtTableUtils.abortTransaction(transactionGuid)
  }

  def commitTransaction(conf: Configuration, confEntry: StringConfigEntry): Unit = {
    val transactionGuid = conf.ytConf(confEntry)
    log.debugLazy(s"Commit write transaction: $transactionGuid")
    YtTableUtils.commitTransaction(transactionGuid)
  }

  def getWriteTransaction(conf: Configuration): String = {
    conf.ytConf(Transaction)
  }

  def getGlobalWriteTransaction(conf: Configuration): String = {
    conf.ytConf(GlobalTransaction)
  }
}
