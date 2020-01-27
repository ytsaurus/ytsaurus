package ru.yandex.spark.yt.format

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{JobContext, TaskAttemptContext}
import org.apache.log4j.Logger
import org.apache.spark.internal.io.FileCommitProtocol
import ru.yandex.inside.yt.kosher.Yt
import ru.yandex.spark.yt.format.conf.{SparkYtConfiguration, YtTableSparkSettings}
import ru.yandex.spark.yt.fs.{GlobalTableSettings, YtClientConfigurationConverter, YtClientProvider}
import ru.yandex.spark.yt.utils.YtTableUtils.Cancellable
import ru.yandex.spark.yt.utils._
import ru.yandex.yt.ytclient.proxy.{ApiServiceTransaction, YtClient}

import scala.concurrent.ExecutionContext
import scala.util.Success

class YtOutputCommitter(jobId: String,
                        outputPath: String,
                        dynamicPartitionOverwrite: Boolean) extends FileCommitProtocol with Serializable {
  private val path = new Path(outputPath).toUri.getPath
  private val tmpPath = s"${path}_tmp"

  import YtOutputCommitter._
  import ru.yandex.spark.yt.format.conf.SparkYtInternalConfiguration._

  override def setupJob(jobContext: JobContext): Unit = {
    val conf = jobContext.getConfiguration
    implicit val yt: YtClient = YtClientProvider.ytClient(YtClientConfigurationConverter(conf))
    implicit val ytHttp: Yt = YtClientProvider.httpClient

    val transaction = createTransaction(conf, GlobalTransaction, None)
    if (YtTableSparkSettings.isTableSorted(conf)) {
      YtTableUtils.createDir(tmpPath, Some(transaction))
    }
    setupTable(path, conf, transaction)
  }

  private def setupTable(path: String, conf: Configuration, transaction: String): Unit = {
    implicit val yt: YtClient = YtClientProvider.ytClient(YtClientConfigurationConverter(conf))
    implicit val ytHttp: Yt = YtClientProvider.httpClient
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
      implicit val yt: YtClient = YtClientProvider.ytClient(YtClientConfigurationConverter(conf))
      implicit val ytHttp: Yt = YtClientProvider.httpClient
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

  import ru.yandex.spark.yt.format.conf.SparkYtInternalConfiguration._
  import ru.yandex.spark.yt.fs.conf._

  private val log = Logger.getLogger(getClass)

  private val pingFutures = scala.collection.concurrent.TrieMap.empty[String, (ApiServiceTransaction, Cancellable[Unit])]

  def createTransaction(conf: Configuration, confEntry: StringConfigEntry, parent: Option[String]): String = {
    implicit val yt: YtClient = YtClientProvider.ytClient(YtClientConfigurationConverter(conf))
    implicit val ytHttp: Yt = YtClientProvider.httpClient

    val transactionTimeout = conf.ytConf(SparkYtConfiguration.Transaction.Timeout)
    val pingInterval = conf.ytConf(SparkYtConfiguration.Transaction.PingInterval)

    val transaction = YtTableUtils.createTransaction(parent, transactionTimeout)
    val pingFuture = YtTableUtils.pingTransaction(transaction, pingInterval)(yt, ExecutionContext.global)
    pingFutures += transaction.getId.toString -> (transaction, pingFuture)
    log.info(s"Create write transaction: ${transaction.getId}")
    conf.setYtConf(confEntry, transaction.getId.toString)
    transaction.getId.toString
  }

  def abortTransaction(conf: Configuration, confEntry: StringConfigEntry): Unit = {
    implicit val yt: YtClient = YtClientProvider.ytClient(YtClientConfigurationConverter(conf))
    implicit val ytHttp: Yt = YtClientProvider.httpClient

    val transactionGuid = conf.ytConf(confEntry)
    log.info(s"Abort write transaction: $transactionGuid")
    pingFutures.remove(transactionGuid).foreach { case (transaction, (cancel, _)) =>
      cancel.complete(Success())
      transaction.abort().join()
    }
  }

  def commitTransaction(conf: Configuration, confEntry: StringConfigEntry): Unit = {
    implicit val yt: YtClient = YtClientProvider.ytClient(YtClientConfigurationConverter(conf))
    implicit val ytHttp: Yt = YtClientProvider.httpClient

    val transactionGuid = conf.ytConf(confEntry)
    log.info(s"Commit write transaction: $transactionGuid")
    pingFutures.remove(transactionGuid).foreach { case (transaction, (cancel, _)) =>
      log.info(s"Cancel ping transaction: $transactionGuid")
      cancel.complete(Success())
      log.info(s"Send commit request transaction: $transactionGuid")
      transaction.commit().join()
      log.info(s"Success commit transaction: $transactionGuid")
    }
  }

  def getWriteTransaction(conf: Configuration): String = {
    conf.ytConf(Transaction)
  }

  def getGlobalWriteTransaction(conf: Configuration): String = {
    conf.ytConf(GlobalTransaction)
  }
}
