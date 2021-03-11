package ru.yandex.spark.yt.wrapper.table

import java.io.{BufferedInputStream, FileInputStream, InputStream}
import java.nio.ByteBuffer
import java.nio.file.Paths

import ru.yandex.bolts.collection.impl.DefaultListF
import ru.yandex.inside.yt.kosher.Yt
import ru.yandex.inside.yt.kosher.common.GUID
import ru.yandex.inside.yt.kosher.cypress.YPath
import ru.yandex.inside.yt.kosher.operations.specs.{MergeMode, MergeSpec}
import ru.yandex.inside.yt.kosher.ytree.YTreeNode
import ru.yandex.spark.yt.wrapper.YtJavaConverters._
import ru.yandex.spark.yt.wrapper.cypress.YtCypressUtils
import ru.yandex.spark.yt.wrapper.transaction.YtTransactionUtils
import ru.yandex.yt.rpcproxy.ERowsetFormat
import ru.yandex.yt.ytclient.`object`.WireRowDeserializer
import ru.yandex.yt.ytclient.proxy.YtClient
import ru.yandex.yt.ytclient.proxy.internal.TableAttachmentByteBufferReader
import ru.yandex.yt.ytclient.proxy.request._

import scala.concurrent.duration._
import scala.language.postfixOps

trait YtTableUtils {
  self: YtCypressUtils with YtTransactionUtils =>

  def createTable(path: String,
                  settings: YtTableSettings,
                  transaction: Option[String] = None)
                 (implicit yt: YtClient): Unit = {
    createDir(Paths.get(path).getParent.toString, transaction, ignoreExisting = true)
    createTable(path, settings.options, transaction)
  }

  def createTable(path: String,
                  settings: YtTableSettings)
                 (implicit yt: YtClient): Unit = {
    createTable(path, settings, None)
  }

  def createTable(path: String,
                  options: Map[String, YTreeNode],
                  transaction: Option[String])
                 (implicit yt: YtClient): Unit = {
    import scala.collection.JavaConverters._
    val request = new CreateNode(formatPath(path), ObjectType.Table, options.asJava)
      .optionalTransaction(transaction)
    yt.createNode(request).join()
  }

  def readTable[T](path: YPath, deserializer: WireRowDeserializer[T], timeout: Duration = 1 minute)
                  (implicit yt: YtClient): TableIterator[T] = {
    readTable(path.toString, deserializer, timeout)
  }

  def readTable[T](path: String, deserializer: WireRowDeserializer[T], timeout: Duration)
                  (implicit yt: YtClient): TableIterator[T] = {
    val request = new ReadTable(path, deserializer)
      .setOmitInaccessibleColumns(true)
      .setUnordered(true)
    val reader = yt.readTable(request).join()
    new TableIterator(reader, timeout)
  }

  def readTableArrowStream(path: YPath, timeout: Duration = 1 minute)
                          (implicit yt: YtClient): YtArrowInputStream = {
    readTableArrowStream(path.toString, timeout)
  }

  def readTableArrowStream(path: String, timeout: Duration)
                          (implicit yt: YtClient): YtArrowInputStream = {
    val request = new ReadTable[ByteBuffer](path, null.asInstanceOf[WireRowDeserializer[ByteBuffer]])
      .setDesiredRowsetFormat(ERowsetFormat.RF_ARROW)
    val reader = yt.readTable(request, new TableAttachmentByteBufferReader).join()
    new TableCopyByteStream(reader, timeout)
  }

  def mergeTables(srcDir: String, dstTable: String,
                  sorted: Boolean,
                  transaction: Option[String] = None)
                 (implicit yt: YtClient, ytHttp: Yt): Unit = {
    import scala.collection.JavaConverters._
    val srcList = formatPath(dstTable) +: listDir(srcDir, transaction).map(name => formatPath(s"$srcDir/$name"))
    ytHttp.operations().mergeAndGetOp(
      toOptional(transaction.map(GUID.valueOf)),
      false,
      new MergeSpec(
        DefaultListF.wrap(seqAsJavaList(srcList)), formatPath(dstTable)
      ).mergeMode(if (sorted) MergeMode.SORTED else MergeMode.UNORDERED)
    ).awaitAndThrowIfNotSuccess()
  }
}
