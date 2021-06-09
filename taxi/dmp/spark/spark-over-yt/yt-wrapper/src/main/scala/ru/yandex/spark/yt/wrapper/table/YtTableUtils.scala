package ru.yandex.spark.yt.wrapper.table

import java.nio.ByteBuffer
import java.nio.file.Paths

import org.slf4j.LoggerFactory
import ru.yandex.inside.yt.kosher.Yt
import ru.yandex.inside.yt.kosher.common.GUID
import ru.yandex.inside.yt.kosher.cypress.YPath
import ru.yandex.inside.yt.kosher.impl.YtConfiguration
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTreeBuilder
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.YTreeTextSerializer
import ru.yandex.inside.yt.kosher.operations.OperationStatus
import ru.yandex.inside.yt.kosher.ytree.YTreeNode
import ru.yandex.misc.io.exec.ProcessUtils
import ru.yandex.misc.net.HostnameUtils
import ru.yandex.spark.yt.wrapper.YtJavaConverters
import ru.yandex.spark.yt.wrapper.cypress.YtCypressUtils
import ru.yandex.spark.yt.wrapper.transaction.YtTransactionUtils
import ru.yandex.yt.rpcproxy.{EOperationType, ERowsetFormat}
import ru.yandex.yt.ytclient.`object`.WireRowDeserializer
import ru.yandex.yt.ytclient.proxy.CompoundClient
import ru.yandex.yt.ytclient.proxy.internal.TableAttachmentByteBufferReader
import ru.yandex.yt.ytclient.proxy.request._

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
    createDir(Paths.get(path).getParent.toString, transaction, ignoreExisting = true)
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
    val request = new CreateNode(formatPath(path), ObjectType.Table, options.asJava)
      .optionalTransaction(transaction)
    yt.createNode(request).join()
  }

  def readTable[T](path: YPath, deserializer: WireRowDeserializer[T], timeout: Duration = 1 minute)
                  (implicit yt: CompoundClient): TableIterator[T] = {
    readTable(path.toString, deserializer, timeout)
  }

  def readTable[T](path: String, deserializer: WireRowDeserializer[T], timeout: Duration)
                  (implicit yt: CompoundClient): TableIterator[T] = {
    val request = new ReadTable(path, deserializer)
      .setOmitInaccessibleColumns(true)
      .setUnordered(true)
    val reader = yt.readTable(request).join()
    new TableIterator(reader, timeout)
  }

  def readTableArrowStream(path: YPath, timeout: Duration = 1 minute)
                          (implicit yt: CompoundClient): YtArrowInputStream = {
    readTableArrowStream(path.toString, timeout)
  }

  def readTableArrowStream(path: String, timeout: Duration)
                          (implicit yt: CompoundClient): YtArrowInputStream = {
    val request = new ReadTable[ByteBuffer](path, null.asInstanceOf[WireRowDeserializer[ByteBuffer]])
      .setDesiredRowsetFormat(ERowsetFormat.RF_ARROW)
    val reader = yt.readTable(request, new TableAttachmentByteBufferReader).join()
    new TableCopyByteStream(reader, timeout)
  }

  private def startedBy(builder: YTreeBuilder): YTreeBuilder = {
    builder
      .beginMap
      .key("user").value(System.getProperty("user.name"))
      .key("command").beginList.value("command").endList
      .key("hostname").value(HostnameUtils.localHostname)
      .key("pid").value(ProcessUtils.getPid)
      .key("wrapper_version").value(YtConfiguration.getVersion)
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
    YtJavaConverters.toOption(info.asMap().getOptional("result"))
      .map(YTreeTextSerializer.serialize).getOrElse("unknown")
  }

  @tailrec
  private def awaitOperation(guid: GUID)(implicit yt: CompoundClient): OperationStatus = {
    val info = yt.getOperation(new GetOperation(guid)).join()
    val status = OperationStatus.R.valueOfOrUnknown(info.asMap().getOrThrow("state").stringValue())
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
                 (implicit yt: CompoundClient, ytHttp: Yt): Unit = {
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

    val operationRequest = new StartOperation(EOperationType.OT_MERGE, operationSpec)
      .optionalTransaction(transaction)
    val guid = yt.startOperation(operationRequest).join()

    val finalStatus = awaitOperation(guid)

    if (!finalStatus.isSuccess) {
      throw new IllegalStateException(s"Merge operation finished with unsuccessful status $finalStatus, " +
        s"result is ${operationResult(guid)}")
    }
  }
}
