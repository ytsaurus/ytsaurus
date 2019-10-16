package ru.yandex.spark.yt.format

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{JobContext, TaskAttemptContext}
import org.apache.log4j.Logger
import org.apache.spark.internal.io.FileCommitProtocol
import ru.yandex.spark.yt.{YtClientProvider, _}
import ru.yandex.yt.ytclient.proxy.YtClient

class YtOutputCommitter(jobId: String,
                        outputPath: String,
                        dynamicPartitionOverwrite: Boolean) extends FileCommitProtocol with Serializable {
  private val path = new Path(outputPath).toUri.getPath

  import YtOutputCommitter._

  override def setupJob(jobContext: JobContext): Unit = {
    val transaction = createTransaction(jobContext.getConfiguration, globalWriteTransactionConfName, None)
    if (!YtTableUtils.exists(path)) {
      val options = SparkYtOptions.deserialize(jobContext.getConfiguration)
      YtTableUtils.createTable(path, GlobalTableOptions.getSchema(jobContext.getJobID.toString), options, transaction)
      GlobalTableOptions.setTransaction(path, transaction)
    }
  }

  override def setupTask(taskContext: TaskAttemptContext): Unit = {
    val globalTransaction = YtOutputCommitter.getGlobalWriteTransaction(taskContext.getConfiguration)
    createTransaction(taskContext.getConfiguration, writeTransactionConfName, Some(globalTransaction))
  }

  override def abortJob(jobContext: JobContext): Unit = {
    GlobalTableOptions.removeSchema(jobContext.getJobID.toString)
    GlobalTableOptions.removeTransaction(path)
    abortTransaction(jobContext.getConfiguration, globalWriteTransactionConfName)
  }

  override def abortTask(taskContext: TaskAttemptContext): Unit = {
    abortTransaction(taskContext.getConfiguration, writeTransactionConfName)
  }

  override def commitJob(jobContext: JobContext, taskCommits: Seq[FileCommitProtocol.TaskCommitMessage]): Unit = {
    GlobalTableOptions.removeSchema(jobContext.getJobID.toString)
    GlobalTableOptions.removeTransaction(path)
    commitTransaction(jobContext.getConfiguration, globalWriteTransactionConfName)
  }

  override def commitTask(taskContext: TaskAttemptContext): FileCommitProtocol.TaskCommitMessage = {
    commitTransaction(taskContext.getConfiguration, writeTransactionConfName)
    null
  }

  override def newTaskTempFile(taskContext: TaskAttemptContext, dir: Option[String], ext: String): String = path

  override def newTaskTempFileAbsPath(taskContext: TaskAttemptContext, absoluteDir: String, ext: String): String = path
}

object YtOutputCommitter {
  import SparkYtOptions._

  private val log = Logger.getLogger(getClass)

  implicit lazy val yt: YtClient = YtClientProvider.ytClient

  private val writeTransactionConfName = "write.transaction"
  private val globalWriteTransactionConfName = "write.globalTransaction"

  def createTransaction(conf: Configuration, confName: String, parent: Option[String]): String = {
    val transactionGuid = YtTableUtils
      .createTransaction(parent)(YtClientProvider.ytClient(YtClientConfigurationConverter(conf)))
      .toString
    log.debugLazy(s"Create write transaction: $transactionGuid")
    conf.setYtConf(confName, transactionGuid)
    transactionGuid
  }

  def abortTransaction(conf: Configuration, confName: String): Unit = {
    val transactionGuid = conf.ytConf(confName)
    log.debugLazy(s"Abort write transaction: $transactionGuid")
    YtTableUtils.abortTransaction(transactionGuid)
  }

  def commitTransaction(conf: Configuration, confName: String): Unit = {
    val transactionGuid = conf.ytConf(confName)
    log.debugLazy(s"Commit write transaction: $transactionGuid")
    YtTableUtils.commitTransaction(transactionGuid)
  }

  def getWriteTransaction(conf: Configuration): String = {
    conf.ytConf(writeTransactionConfName)
  }

  def getGlobalWriteTransaction(conf: Configuration): String = {
    conf.ytConf(globalWriteTransactionConfName)
  }
}
