package ru.yandex.spark.yt.wrapper.table

import java.nio.file.Paths
import java.util.Optional

import org.apache.log4j.Logger
import ru.yandex.bolts.collection.impl.DefaultListF
import ru.yandex.inside.yt.kosher.Yt
import ru.yandex.inside.yt.kosher.common.GUID
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTreeBuilder
import ru.yandex.inside.yt.kosher.operations.specs.{MergeMode, MergeSpec}
import ru.yandex.inside.yt.kosher.ytree.YTreeNode
import ru.yandex.spark.yt.wrapper.cypress.YtCypressUtils
import ru.yandex.yt.ytclient.`object`.WireRowDeserializer
import ru.yandex.yt.ytclient.proxy.YtClient
import ru.yandex.yt.ytclient.proxy.request.{CreateNode, ObjectType, ReadTable, TransactionalOptions}
import ru.yandex.spark.yt.wrapper.YtJavaConverters._

import scala.concurrent.duration._
import scala.language.postfixOps

trait YtTableUtils {
  self: YtCypressUtils =>

  private val log = Logger.getLogger(getClass)

  def createTable(path: String,
                  settings: YtTableSettings,
                  transaction: String)
                 (implicit yt: YtClient): Unit = {
    import scala.collection.JavaConverters._
    val ytOptions = settings.options.map { case (key, value) =>
      val builder = new YTreeBuilder()
      builder.onString(value)
      key -> builder.build()
    } + ("schema" -> settings.ytSchema)

    createDir(Paths.get(path).getParent.toString, Some(transaction), ignoreExisting = true)
    createTable(path, ytOptions.asJava, transaction)
  }

  def createTable(path: String,
                  options: java.util.Map[String, YTreeNode],
                  transaction: String)
                 (implicit yt: YtClient): Unit = {
    val transactionGuid = GUID.valueOf(transaction)
    val request = new CreateNode(formatPath(path), ObjectType.Table, options)
      .setTransactionalOptions(new TransactionalOptions(transactionGuid))
    yt.createNode(request).join()
  }

  def readTable[T](path: String, deserializer: WireRowDeserializer[T], timeout: Duration = 1 minute)
                  (implicit yt: YtClient): TableIterator[T] = {
    val request = new ReadTable(path, deserializer)
      .setOmitInaccessibleColumns(true)
      .setUnordered(true)
    val reader = yt.readTable(request).join()
    new TableIterator(reader, timeout)
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
