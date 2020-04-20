package ru.yandex.spark.yt.format

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{JobContext, TaskAttemptContext}
import org.apache.log4j.Logger
import org.apache.spark.internal.io.FileCommitProtocol
import ru.yandex.inside.yt.kosher.Yt
import ru.yandex.spark.yt.format.conf.{SparkYtConfiguration, YtTableSparkSettings}
import ru.yandex.spark.yt.fs.YtClientConfigurationConverter.ytClientConfiguration
import ru.yandex.spark.yt.fs.conf._
import ru.yandex.spark.yt.fs.{GlobalTableSettings, YtClientProvider}
import ru.yandex.spark.yt.wrapper.YtWrapper
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
    implicit val ytClient: YtClient = yt(conf)
    withTransaction(createTransaction(conf, GlobalTransaction, None)) { transaction =>
      if (YtTableSparkSettings.isTableSorted(conf)) {
        YtWrapper.createDir(tmpPath, Some(transaction))
      }
      setupTable(path, conf, transaction)
    }
  }

  private def setupTable(path: String, conf: Configuration, transaction: String)
                        (implicit yt: YtClient): Unit = {
    if (!YtWrapper.exists(path)) {
      val options = YtTableSparkSettings.deserialize(conf)
      YtWrapper.createTable(path, options, transaction)
      GlobalTableSettings.setTransaction(path, transaction)
    }
  }

  override def setupTask(taskContext: TaskAttemptContext): Unit = {
    val conf = taskContext.getConfiguration
    val globalTransaction = YtOutputCommitter.getGlobalWriteTransaction(conf)
    withTransaction(createTransaction(conf, Transaction, Some(globalTransaction))) { transaction =>
      if (YtTableSparkSettings.isTableSorted(conf)) {
        setupTable(newTaskTempFile(taskContext, None, ""), conf, transaction)(yt(conf))
      }
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
    withTransaction(YtOutputCommitter.getGlobalWriteTransaction(conf)) { transaction =>
      if (YtTableSparkSettings.isTableSorted(conf)) {
        implicit val yt: YtClient = YtClientProvider.ytClient(ytClientConfiguration(conf))
        implicit val ytHttp: Yt = YtClientProvider.httpClient
        YtWrapper.mergeTables(tmpPath, path, sorted = true, Some(transaction))
        YtWrapper.removeDir(tmpPath, recursive = true, Some(transaction))
      }
      GlobalTableSettings.removeTransaction(path)
      commitTransaction(jobContext.getConfiguration, GlobalTransaction)
    }
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

  private val log = Logger.getLogger(getClass)

  private val pingFutures = scala.collection.concurrent.TrieMap.empty[String, (ApiServiceTransaction, YtWrapper.Cancellable[Unit])]

  private def yt(conf: Configuration): YtClient = YtClientProvider.ytClient(ytClientConfiguration(conf))

  def withTransaction(transaction: String)(f: String => Unit): Unit = {
    try {
      f(transaction)
    } catch {
      case e: Throwable =>
        try {
          abortTransaction(transaction)
        } catch {
          case inner: Throwable =>
            e.addSuppressed(inner)
        }
        throw e
    }
  }

  def createTransaction(conf: Configuration, confEntry: StringConfigEntry, parent: Option[String]): String = {
    implicit val yt: YtClient = YtClientProvider.ytClient(ytClientConfiguration(conf))
    implicit val ytHttp: Yt = YtClientProvider.httpClient

    val transactionTimeout = conf.ytConf(SparkYtConfiguration.Transaction.Timeout)
    val pingInterval = conf.ytConf(SparkYtConfiguration.Transaction.PingInterval)

    val transaction = YtWrapper.createTransaction(parent, transactionTimeout)
    try {
      val pingFuture = YtWrapper.pingTransaction(transaction, pingInterval)(yt, ExecutionContext.global)
      pingFutures += transaction.getId.toString -> (transaction, pingFuture)
      log.info(s"Create write transaction: ${transaction.getId}")
      conf.setYtConf(confEntry, transaction.getId.toString)
      transaction.getId.toString
    } catch {
      case e: Throwable =>
        transaction.abort().join()
        throw e
    }
  }

  def abortTransaction(conf: Configuration, confEntry: StringConfigEntry): Unit = {
    abortTransaction(conf.ytConf(confEntry))
  }

  def abortTransaction(transaction: String): Unit = {
    log.info(s"Abort write transaction: $transaction")
    pingFutures.remove(transaction).foreach { case (transaction, (cancel, _)) =>
      cancel.complete(Success())
      transaction.abort().join()
    }
  }

  def commitTransaction(conf: Configuration, confEntry: StringConfigEntry): Unit = {
    withTransaction(conf.ytConf(confEntry)) { transactionGuid =>
      log.info(s"Commit write transaction: $transactionGuid")
      pingFutures.remove(transactionGuid).foreach { case (transaction, (cancel, _)) =>
        log.info(s"Cancel ping transaction: $transactionGuid")
        cancel.complete(Success())
        log.info(s"Send commit request transaction: $transactionGuid")
        transaction.commit().join()
        log.info(s"Success commit transaction: $transactionGuid")
      }
    }
  }

  def getWriteTransaction(conf: Configuration): String = {
    conf.ytConf(Transaction)
  }

  def getGlobalWriteTransaction(conf: Configuration): String = {
    conf.ytConf(GlobalTransaction)
  }
}
