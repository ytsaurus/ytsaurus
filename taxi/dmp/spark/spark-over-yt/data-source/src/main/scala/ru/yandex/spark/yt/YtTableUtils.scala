package ru.yandex.spark.yt

import org.apache.spark.sql.types.StructType
import org.joda.time.Duration
import ru.yandex.bolts.collection.{Option => YOption}
import ru.yandex.bolts.function
import ru.yandex.inside.yt.kosher.Yt
import ru.yandex.inside.yt.kosher.common.GUID
import ru.yandex.inside.yt.kosher.impl.rpc.TransactionManager
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTreeBuilder
import ru.yandex.inside.yt.kosher.ytree.YTreeNode
import ru.yandex.spark.yt.format.TableIterator
import ru.yandex.spark.yt.serializers.SchemaConverter
import ru.yandex.yt.ytclient.`object`.WireRowDeserializer
import ru.yandex.yt.ytclient.proxy.YtClient
import ru.yandex.yt.ytclient.proxy.request._

object YtTableUtils {
  private val tableOptions = Set("optimize_for", "schema")

  def createTable(path: String,
                  schema: StructType,
                  options: Map[String, String],
                  transaction: String)
                 (implicit yt: YtClient): Unit = {

    import scala.collection.JavaConverters._
    val ytOptions = options.collect { case (key, value) if tableOptions.contains(key) =>
      val builder = new YTreeBuilder()
      builder.onString(value)
      key -> builder.build()
    } + ("schema" -> SchemaConverter.ytSchema(schema))

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

  def removeTable(path: String)(implicit yt: YtClient): Unit = {
    yt.removeNode(formatPath(path)).join()
  }

  def removeTableIfExists(path: String)(implicit yt: YtClient): Unit = {
    if (exists(path)) {
      removeTable(path)
    }
  }

  def exists(path: String, transaction: Option[String] = None)(implicit yt: YtClient): Boolean = {
    try {
      val request = new GetNode(formatPath(path)).optionalTransaction(transaction)
      yt.getNode(request).join()
      true
    } catch {
      case _: Throwable => false
    }
  }

  def tableAttribute(path: String, attrName: String, transaction: Option[String] = None)
                    (implicit yt: YtClient): YTreeNode = {
    val request = new GetNode(s"${formatPath(path)}/@$attrName").optionalTransaction(transaction)
    yt.getNode(request).join()
  }

  def formatPath(path: String): String = "/" + path

  def mergeTables(srcDir: String, dstTable: String)(implicit yt: YtClient, ytHttp: Yt): Unit = {
    val srcList = yt
      .listNode(formatPath(srcDir))
      .join().asList()
      .map(new function.Function[YTreeNode, String] {
        override def apply(t: YTreeNode): String = {
          formatPath(s"$srcDir/${t.stringValue()}")
        }
      })

    val dstExists = exists(dstTable)

    val mergeList = if (dstExists) srcList.plus1(formatPath(dstTable)) else srcList

    if (!dstExists) {
      val options = tableAttribute(srcList.first().drop(1), "").asMap()
        .filterKeys(key => tableOptions.contains(key))
      createTable(dstTable, options, "")
    }

    val guid = ytHttp.operations().merge(mergeList, formatPath(dstTable))
    val operation = ytHttp.operations().getOperation(guid)
    while (!operation.getStatus.isFinished) {}
    if (!operation.getStatus.isSuccess) {
      throw new IllegalStateException("Merge failed")
    }
  }

  def createDir(path: String)(implicit yt: YtClient): Unit = {
    yt.createNode(formatPath(path), ObjectType.MapNode).join()
  }

  def removeDir(path: String, recursive: Boolean)(implicit yt: YtClient): Unit = {
    yt.removeNode(new RemoveNode(formatPath(path)).setRecursive(true)).join()
  }

  def removeDirIfExists(path: String, recursive: Boolean)(implicit yt: YtClient): Unit = {
    if (exists(path)) {
      removeDir(path, recursive)
    }
  }

  def readTable[T](path: String, deserializer: WireRowDeserializer[T])(implicit yt: YtClient): TableIterator[T] = {
    val request = new ReadTable(path, deserializer)
      .setOmitInaccessibleColumns(true)
      .setUnordered(true)
    val reader = yt.readTable(request).join()
    new TableIterator(reader)
  }

  def createTransaction(parent: Option[String])(implicit yt: YtClient): GUID = {
    val parentGuid = parent.map(GUID.valueOf)
    val tm = new TransactionManager(yt)
    tm.start(YOption.when(parentGuid.nonEmpty, () => parentGuid.get), false, Duration.standardSeconds(300)).join()
  }

  def abortTransaction(guid: String)(implicit yt: YtClient): Unit = {
    yt.abortTransaction(GUID.valueOf(guid), true).join()
  }

  def commitTransaction(guid: String)(implicit yt: YtClient): Unit = {
    yt.commitTransaction(GUID.valueOf(guid), true).join()
  }

  implicit class RichRequest[T <: GetLikeReq[_]](val request: T) {
    def optionalTransaction(transaction: Option[String]): T = {
      transaction.map{t =>
        request.setTransactionalOptions(new TransactionalOptions(GUID.valueOf(t))).asInstanceOf[T]
      }.getOrElse(request)
    }
  }
}
