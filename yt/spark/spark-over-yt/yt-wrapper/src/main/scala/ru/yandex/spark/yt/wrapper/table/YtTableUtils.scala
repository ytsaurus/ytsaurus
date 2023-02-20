package ru.yandex.spark.yt.wrapper.table

import org.slf4j.LoggerFactory
import ru.yandex.spark.yt.wrapper.Utils
import ru.yandex.spark.yt.wrapper.YtJavaConverters.RichJavaMap
import ru.yandex.spark.yt.wrapper.cypress.YtCypressUtils
import ru.yandex.spark.yt.wrapper.operation.OperationStatus
import ru.yandex.spark.yt.wrapper.transaction.YtTransactionUtils
import tech.ytsaurus.client.CompoundClient
import tech.ytsaurus.client.request._
import tech.ytsaurus.client.rows.WireRowDeserializer
import tech.ytsaurus.core.GUID
import tech.ytsaurus.core.cypress.{CypressNodeType, YPath}
import tech.ytsaurus.rpcproxy.EOperationType
import tech.ytsaurus.ysontree.{YTreeBuilder, YTreeNode, YTreeTextSerializer}

import java.nio.ByteBuffer
import java.nio.file.Paths
import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.language.postfixOps

trait YtTableUtils {
  self: YtCypressUtils with YtTransactionUtils =>

  private val log = LoggerFactory.getLogger(getClass)

  def createTable(path: String,
                  settings: YtTableSettings,
                  transaction: Option[String] = None)
                 (implicit yt: CompoundClient): Unit = {
    val parent = Paths.get(path).getParent
    if (parent != null) {
      createDir(parent.toString, transaction, ignoreExisting = true)
    }
    createTable(path, settings.options, transaction)
  }

  def createTable(path: String,
                  settings: YtTableSettings)
                 (implicit yt: CompoundClient): Unit = {
    createTable(path, settings, None)
  }

  def createTable(path: String,
                  options: Map[String, YTreeNode],
                  transaction: Option[String])
                 (implicit yt: CompoundClient): Unit = {
    log.debug(s"Create table: $path, transaction: $transaction")
    import scala.collection.JavaConverters._
    val request = CreateNode.builder()
      .setPath(YPath.simple(formatPath(path)))
      .setType(CypressNodeType.TABLE)
      .setAttributes(options.asJava)
      .optionalTransaction(transaction)
    yt.createNode(request).join()
  }

  def readTable[T](path: YPath, deserializer: WireRowDeserializer[T], timeout: Duration = 1 minute,
                   transaction: Option[String] = None, reportBytesRead: Long => Unit)
                  (implicit yt: CompoundClient): TableIterator[T] = {
    val request = ReadTable.builder()
      .setPath(path)
      .setSerializationContext(new ReadSerializationContext(deserializer))
      .setOmitInaccessibleColumns(true)
      .setUnordered(true)
      .optionalTransaction(transaction)
    val reader = yt.readTable(request).join()
    new TableIterator(reader, timeout, reportBytesRead)
  }

  def readTableArrowStream(path: String, timeout: Duration, transaction: Option[String],
                           reportBytesRead: Long => Unit)
                          (implicit yt: CompoundClient): YtArrowInputStream = {
    val request = ReadTable.builder[ByteBuffer]().setPath(path)
      .setSerializationContext(ReadSerializationContext.binaryArrow())
      .optionalTransaction(transaction)
    val reader = yt.readTable(request).join()
    new TableCopyByteStream(reader, timeout, reportBytesRead)
  }

  def readTableArrowStream(path: YPath, timeout: Duration = 1 minute,
                           transaction: Option[String] = None,
                           reportBytesRead: Long => Unit = _ => {})
                          (implicit yt: CompoundClient): YtArrowInputStream = {
    readTableArrowStream(path.toString, timeout, transaction, reportBytesRead)
  }

  private def startedBy(builder: YTreeBuilder): YTreeBuilder = {
    import ru.yandex.spark.yt.BuildInfo
    builder
      .beginMap
      .key("user").value(System.getProperty("user.name"))
      .key("command").beginList.value("command").endList
      .key("hostname").value(Utils.ytHostnameOrIpAddress)
      .key("pid").value(ProcessHandle.current().pid())
      .key("wrapper_version").value(BuildInfo.ytClientVersion)
      .endMap
  }

  private def pathToTree(path: String, transaction: Option[String]): YTreeNode = {
    transaction.foldLeft(YPath.simple(formatPath(path))) { case (p, t) =>
      p.plusAdditionalAttribute("transaction_id", t)
    }.toTree
  }

  private def ysonPaths(paths: Seq[String], transaction: Option[String]): YTreeNode = {
    paths.foldLeft(new YTreeBuilder().beginList()) { case (b, path) =>
      b.value(pathToTree(path, transaction))
    }.endList().build()
  }

  private def operationResult(guid: GUID)(implicit yt: CompoundClient): String = {
    val info = yt.getOperation(new GetOperation(guid)).join()
    Option(info.asMap().get("result"))
      .map(YTreeTextSerializer.serialize).getOrElse("unknown")
  }

  @tailrec
  private def awaitOperation(guid: GUID)(implicit yt: CompoundClient): OperationStatus = {
    val info = yt.getOperation(new GetOperation(guid)).join()
    val status = OperationStatus.getByName(info.asMap().getOrThrow("state").stringValue())
    if (status.isFinished) {
      status
    } else {
      log.info(s"Operation $guid is in status $status")
      Thread.sleep((5 seconds).toMillis)
      awaitOperation(guid)
    }
  }

  def mergeTables(srcDir: String, dstTable: String,
                  sorted: Boolean,
                  transaction: Option[String] = None,
                  specParams: Map[String, YTreeNode] = Map.empty)
                 (implicit yt: CompoundClient): Unit = {
    log.debug(s"Merge tables: $srcDir -> $dstTable, transaction: $transaction")

    val srcList = dstTable +: listDir(srcDir, transaction).map(name => s"$srcDir/$name")

    val operationSpec = new YTreeBuilder()
      .beginMap()
      .key("mode").value(if (sorted) "sorted" else "unordered")
      .key("combine_chunks").value(false)
      .key("started_by").apply(startedBy)
      .key("input_table_paths").value(ysonPaths(srcList, None))
      .key("output_table_path").value(pathToTree(dstTable, None))
      .apply(specParams.foldLeft(_) { case (b, (k, v)) => b.key(k).value(v) })
      .endMap()
      .build()

    val operationRequest = new StartOperation(EOperationType.OT_MERGE, operationSpec).toBuilder
      .optionalTransaction(transaction)
    val guid = yt.startOperation(operationRequest).join()

    val finalStatus = awaitOperation(guid)

    if (!finalStatus.isSuccess) {
      throw new IllegalStateException(s"Merge operation finished with unsuccessful status $finalStatus, " +
        s"result is ${operationResult(guid)}")
    }
  }
}
