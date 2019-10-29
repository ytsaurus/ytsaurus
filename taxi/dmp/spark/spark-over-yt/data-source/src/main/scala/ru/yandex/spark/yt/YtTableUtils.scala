package ru.yandex.spark.yt

import java.io.{InputStream, OutputStream}
import java.util.concurrent.CompletableFuture

import org.apache.spark.sql.types.StructType
import org.joda.time.Duration
import ru.yandex.bolts.collection.{Option => YOption}
import ru.yandex.bolts.function
import ru.yandex.inside.yt.kosher.Yt
import ru.yandex.inside.yt.kosher.common.GUID
import ru.yandex.inside.yt.kosher.impl.rpc.TransactionManager
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTreeBuilder
import ru.yandex.inside.yt.kosher.ytree.YTreeNode
import ru.yandex.spark.yt.format.{FileIterator, PathType, TableIterator, YtFileOutputStream}
import ru.yandex.spark.yt.serializers.SchemaConverter
import ru.yandex.yt.ytclient.`object`.WireRowDeserializer
import ru.yandex.yt.ytclient.proxy.internal.FileWriterImpl
import ru.yandex.yt.ytclient.proxy.request._
import ru.yandex.yt.ytclient.proxy.{FileWriter, YtClient}

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

  def remove(path: String)(implicit yt: YtClient): Unit = {
    yt.removeNode(formatPath(path)).join()
  }

  def removeIfExists(path: String)(implicit yt: YtClient): Unit = {
    if (exists(path)) {
      remove(path)
    }
  }

  def getType(path: String, transaction: Option[String] = None)(implicit yt: YtClient): PathType = {
    val objectType = tableAttribute(path, "type", transaction).stringValue()
    objectType match {
      case "file" => PathType.File
      case "table" => PathType.Table
      case "map_node" => PathType.Directory
      case _ => PathType.None
    }
  }

  def exists(path: String, transaction: Option[String] = None)(implicit yt: YtClient): Boolean = {
    val request = new ExistsNode(s"${formatPath(path)}/@").optionalTransaction(transaction)
    yt.existsNode(request).join().booleanValue()
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

  def createDir(path: String, ignoreExisting: Boolean = false)(implicit yt: YtClient): Unit = {
    yt.createNode(new CreateNode(formatPath(path), ObjectType.MapNode).setIgnoreExisting(ignoreExisting)).join()
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

  def readFile(path: String, transaction: Option[String] = None)(implicit yt: YtClient): InputStream = {
    val fileReader = yt.readFile(new ReadFile(formatPath(path))).join()
    new FileIterator(fileReader)
  }

  def listDirectory(path: String, transaction: Option[String] = None)(implicit yt: YtClient): Array[String] = {
    val response = yt.listNode(new ListNode(formatPath(path)).optionalTransaction(transaction)).join().asList()
    val array = new Array[String](response.length())
    response.zipWithIndex().forEach((t: YTreeNode, i: java.lang.Integer) => {
      array(i) = t.stringValue()
    })
    array
  }

  def createFile(path: String, transaction: Option[String] = None)(implicit yt: YtClient): Unit = {
    val request = new CreateNode(formatPath(path), ObjectType.File)
    transaction.foreach(t => request.setTransactionalOptions(new TransactionalOptions(GUID.valueOf(t))))
    yt.createNode(request).join()
  }

  def writeToFile(path: String, transaction: Option[String] = None)(implicit yt: YtClient): OutputStream = {
    val request = new WriteFile(formatPath(path))
      .setWindowSize(10000000L)
      .setPacketSize(1000000L)

    transaction.foreach(t => request.setTransactionalOptions(new TransactionalOptions(GUID.valueOf(t))))

    val writer = writeFile(request).join()

    new YtFileOutputStream(writer)
  }

  def writeFile(req: WriteFile)(implicit yt: YtClient): CompletableFuture[FileWriter] = {
    val builder = yt.getService.writeFile
    builder.setTimeout(java.time.Duration.ofDays(7))
    req.writeTo(builder.body)
    new FileWriterImpl(builder.startStream(yt.selectDestinations()), req.getWindowSize, req.getPacketSize).startUpload
  }

  def rename(src: String, dst: String, transaction: Option[String] = None)(implicit yt: YtClient): Unit = {
    val request = new MoveNode(formatPath(src), formatPath(dst))
    transaction.foreach(t => request.setTransactionalOptions(new TransactionalOptions(GUID.valueOf(t))))
    yt.moveNode(request).join()
  }

  def fileSize(path: String, transaction: Option[String] = None)(implicit yt: YtClient): Long = {
    tableAttribute(path, "compressed_data_size", transaction).longValue()
  }
}
