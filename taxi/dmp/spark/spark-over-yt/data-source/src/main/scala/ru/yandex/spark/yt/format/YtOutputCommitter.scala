package ru.yandex.spark.yt.format

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.{JobContext, TaskAttemptContext}
import org.apache.spark.internal.io.FileCommitProtocol
import org.slf4j.LoggerFactory
import ru.yandex.spark.yt.format.conf.YtTableSparkSettings._
import ru.yandex.spark.yt.format.conf.{SparkYtConfiguration, YtTableSparkSettings}
import ru.yandex.spark.yt.fs.GlobalTableSettings
import ru.yandex.spark.yt.fs.YtClientConfigurationConverter.ytClientConfiguration
import ru.yandex.spark.yt.fs.conf._
import ru.yandex.spark.yt.wrapper.YtWrapper
import ru.yandex.spark.yt.wrapper.client.YtClientProvider
import ru.yandex.yt.ytclient.proxy.{ApiServiceTransaction, CompoundClient}

class YtOutputCommitter(jobId: String,
                        outputPath: String,
                        dynamicPartitionOverwrite: Boolean) extends FileCommitProtocol with Serializable {
  private val path = new Path(outputPath).toUri.getPath
  private val tmpPath = s"${path}_tmp"

  @transient private val deletedDirectories = ThreadLocal.withInitial[Seq[Path]](() => Nil)

  import YtOutputCommitter._
  import ru.yandex.spark.yt.format.conf.SparkYtInternalConfiguration._

  override def setupJob(jobContext: JobContext): Unit = {
    val conf = jobContext.getConfiguration
    implicit val ytClient: CompoundClient = yt(conf)
    val externalTransaction = jobContext.getConfiguration.getYtConf(WriteTransaction)

    withTransaction(createTransaction(conf, GlobalTransaction, externalTransaction))({ transaction =>
      GlobalTableSettings.setTransaction(path, transaction)
      deletedDirectories.get().foreach(p => YtWrapper.remove(p.toUri.getPath, Some(transaction)))
      deletedDirectories.set(Nil)
      if (isTableSorted(conf)) setupSortedTmpTables(transaction)
      if (isTable(conf)) setupTable(path, conf, transaction)
    }, removeGlobalTransactions())
  }

  private def setupSortedTmpTables(transaction: String)(implicit yt: CompoundClient): Unit = {
    YtWrapper.createDir(tmpPath, Some(transaction))
  }

  /**
   * @deprecated Do not use before YT 21.1 release
   */
  @Deprecated
  private def setupSortedTable(transaction: String, conf: Configuration)
                              (implicit yt: CompoundClient): Unit = {
    GlobalTableSettings.setTransaction(tmpPath, transaction)
    setupUnsortedTable(tmpPath, conf, transaction)
  }

  private def removeGlobalTransactions(): Unit = {
    GlobalTableSettings.removeTransaction(path)
    GlobalTableSettings.removeTransaction(tmpPath)
  }

  private def setupTable(path: String, conf: Configuration, transaction: String)
                        (implicit yt: CompoundClient): Unit = {
    if (!YtWrapper.exists(path, Some(transaction))) {
      val options = YtTableSparkSettings.deserialize(conf)
      YtWrapper.createTable(path, options, Some(transaction))
    }
  }

  private def setupUnsortedTable(path: String, conf: Configuration, transaction: String)
                                (implicit yt: CompoundClient): Unit = {
    import ru.yandex.spark.yt.fs.conf._
    val newConf = new Configuration(conf)
    newConf.setYtConf(SortColumns, Seq.empty)
    setupTable(path, newConf, transaction)
  }

  private def setupTmpTable(taskContext: TaskAttemptContext, transaction: String): Unit = {
    val conf = taskContext.getConfiguration
    setupTable(tmpTablePath(taskContext), conf, transaction)(yt(conf))
  }

  override def setupTask(taskContext: TaskAttemptContext): Unit = {
    val conf = taskContext.getConfiguration
    val parent = YtOutputCommitter.getGlobalWriteTransaction(conf)
    withTransaction(createTransaction(conf, Transaction, Some(parent))) { transaction =>
      if (isTableSorted(conf)) setupTmpTable(taskContext, transaction)
    }
  }

  override def abortJob(jobContext: JobContext): Unit = {
    deletedDirectories.set(Nil)
    removeGlobalTransactions()
    abortTransaction(jobContext.getConfiguration, GlobalTransaction)
  }

  override def abortTask(taskContext: TaskAttemptContext): Unit = {
    abortTransaction(taskContext.getConfiguration, Transaction)
  }

  /**
   * @deprecated Do not use before YT 21.1 release
   */
  @Deprecated
  private def concatenateSortedTable(conf: Configuration, transaction: String): Unit = {
    implicit val yt: CompoundClient = YtClientProvider.ytClient(ytClientConfiguration(conf))
    YtWrapper.concatenate(Array(tmpPath), path, Some(transaction))
    YtWrapper.remove(tmpPath, Some(transaction))
  }

  private def mergeSortedTables(conf: Configuration, transaction: String): Unit = {
    implicit val yt: CompoundClient = YtClientProvider.ytClient(ytClientConfiguration(conf))
    val mergeSpec = conf.getYtSpecConf("merge")
    YtWrapper.mergeTables(tmpPath, path, sorted = true, Some(transaction), mergeSpec)
    YtWrapper.removeDir(tmpPath, recursive = true, Some(transaction))
  }

  override def commitJob(jobContext: JobContext, taskCommits: Seq[FileCommitProtocol.TaskCommitMessage]): Unit = {
    val conf = jobContext.getConfiguration
    withTransaction(YtOutputCommitter.getGlobalWriteTransaction(conf)) { transaction =>
      if (isTableSorted(conf)) mergeSortedTables(conf, transaction)
      removeGlobalTransactions()
      commitTransaction(jobContext.getConfiguration, GlobalTransaction)
    }
  }

  override def commitTask(taskContext: TaskAttemptContext): FileCommitProtocol.TaskCommitMessage = {
    new FileCommitProtocol.TaskCommitMessage(taskContext.getConfiguration.ytConf(Transaction))
  }

  override def deleteWithJob(fs: FileSystem, path: Path, recursive: Boolean): Boolean = {
    deletedDirectories.set(path +: deletedDirectories.get())
    true
  }

  private def tmpTablePath(taskContext: TaskAttemptContext): String = {
    s"$tmpPath/part-${taskContext.getTaskAttemptID.getTaskID.getId}"
  }

  override def newTaskTempFile(taskContext: TaskAttemptContext, dir: Option[String], ext: String): String = {
    if (isTableSorted(taskContext.getConfiguration)) tmpTablePath(taskContext) else path
  }

  override def newTaskTempFileAbsPath(taskContext: TaskAttemptContext, absoluteDir: String, ext: String): String = path

  override def onTaskCommit(taskCommit: FileCommitProtocol.TaskCommitMessage): Unit = {
    val transactionGuid = taskCommit.obj.asInstanceOf[String]
    val yt = YtClientProvider.cachedClient("committer").yt
    log.info(s"Commit write transaction: $transactionGuid")
    log.info(s"Send commit transaction request: $transactionGuid")
    YtWrapper.commitTransaction(transactionGuid)(yt)
    log.info(s"Success commit transaction: $transactionGuid")
  }
}

object YtOutputCommitter {

  import ru.yandex.spark.yt.format.conf.SparkYtInternalConfiguration._

  private val log = LoggerFactory.getLogger(getClass)

  private val pingFutures = scala.collection.concurrent.TrieMap.empty[String, ApiServiceTransaction]

  private def yt(conf: Configuration): CompoundClient = YtClientProvider.ytClient(ytClientConfiguration(conf), "committer")

  def withTransaction(transaction: String)(f: String => Unit, abort: => Unit = () => ()): Unit = {
    try {
      f(transaction)
    } catch {
      case e: Throwable =>
        try {
          abortTransaction(transaction)
          abort
        } catch {
          case inner: Throwable =>
            e.addSuppressed(inner)
        }
        throw e
    }
  }

  def createTransaction(conf: Configuration, confEntry: StringConfigEntry, parent: Option[String]): String = {
    implicit val yt: CompoundClient = YtClientProvider.ytClient(ytClientConfiguration(conf))
    val transactionTimeout = conf.ytConf(SparkYtConfiguration.Transaction.Timeout)

    val transaction = YtWrapper.createTransaction(parent, transactionTimeout)
    try {
      pingFutures += transaction.getId.toString -> transaction
      log.info(s"Create write transaction: ${transaction.getId}")
      conf.setYtConf(confEntry, transaction.getId.toString)
      transaction.getId.toString
    } catch {
      case e: Throwable =>
        abortTransaction(transaction.getId.toString)
        throw e
    }
  }

  def abortTransaction(conf: Configuration, confEntry: StringConfigEntry): Unit = {
    abortTransaction(conf.ytConf(confEntry))
  }

  def abortTransaction(transaction: String): Unit = {
    log.info(s"Abort write transaction: $transaction")
    pingFutures.remove(transaction).foreach { transaction =>
      transaction.abort().join()
    }
  }

  def commitTransaction(conf: Configuration, confEntry: StringConfigEntry): Unit = {
    withTransaction(conf.ytConf(confEntry)) { transactionGuid =>
      log.info(s"Commit write transaction: $transactionGuid")
      pingFutures.remove(transactionGuid).foreach { transaction =>
        log.info(s"Send commit transaction request: $transactionGuid")
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
