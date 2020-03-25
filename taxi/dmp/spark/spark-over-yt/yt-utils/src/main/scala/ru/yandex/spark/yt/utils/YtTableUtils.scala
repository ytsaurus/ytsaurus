package ru.yandex.spark.yt.utils

import java.io.OutputStream
import java.nio.file.Paths
import java.util.Optional
import java.util.concurrent.CompletableFuture

import org.apache.log4j.Logger
import ru.yandex.bolts.collection.impl.DefaultListF
import ru.yandex.inside.yt.kosher.Yt
import ru.yandex.inside.yt.kosher.common.GUID
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTreeBuilder
import ru.yandex.inside.yt.kosher.operations.specs.{MergeMode, MergeSpec}
import ru.yandex.inside.yt.kosher.ytree.YTreeNode
import ru.yandex.yt.rpcproxy.ETransactionType.TT_MASTER
import ru.yandex.yt.ytclient.`object`.WireRowDeserializer
import ru.yandex.yt.ytclient.proxy.internal.FileWriterImpl
import ru.yandex.yt.ytclient.proxy.request._
import ru.yandex.yt.ytclient.proxy.{ApiServiceTransaction, ApiServiceTransactionOptions, FileWriter, YtClient}

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.concurrent.{CancellationException, ExecutionContext, Future, Promise}
import scala.io.Source
import scala.language.postfixOps
import scala.util.Random

object YtTableUtils {
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

  def move(src: String, dst: String, transaction: Option[String] = None)(implicit yt: YtClient): Unit = {
    yt.moveNode(new MoveNode(formatPath(src), formatPath(dst)).optionalTransaction(transaction)).join()
  }

  def remove(path: String, transaction: Option[String] = None)(implicit yt: YtClient): Unit = {
    yt.removeNode(new RemoveNode(formatPath(path)).optionalTransaction(transaction)).join()
  }

  def removeIfExists(path: String, transaction: Option[String] = None)(implicit yt: YtClient): Unit = {
    if (exists(path)) {
      remove(path, transaction)
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

  def formatPath(path: String): String = if (path.startsWith("//")) path else "/" + path

  def mergeTables(srcDir: String, dstTable: String,
                  sorted: Boolean,
                  transaction: Option[String] = None)
                 (implicit yt: YtClient, ytHttp: Yt): Unit = {
    import scala.collection.JavaConverters._
    val srcList = formatPath(dstTable) +: listDirectory(srcDir, transaction).map(name => formatPath(s"$srcDir/$name"))
    ytHttp.operations().mergeAndGetOp(
      toOptional(transaction.map(GUID.valueOf)),
      false,
      new MergeSpec(
        DefaultListF.wrap(seqAsJavaList(srcList)), formatPath(dstTable)
      ).mergeMode(if (sorted) MergeMode.SORTED else MergeMode.UNORDERED)
    ).awaitAndThrowIfNotSuccess()
  }

  def createDir(path: String, transaction: Option[String] = None, ignoreExisting: Boolean = false)
               (implicit yt: YtClient): Unit = {
    yt.createNode(
      new CreateNode(formatPath(path), ObjectType.MapNode)
        .setRecursive(true)
        .setIgnoreExisting(ignoreExisting)
        .optionalTransaction(transaction)
    ).join()
  }

  def removeDir(path: String, recursive: Boolean, transaction: Option[String] = None)
               (implicit yt: YtClient): Unit = {
    yt.removeNode(new RemoveNode(formatPath(path)).setRecursive(true).optionalTransaction(transaction)).join()
  }

  def removeDirIfExists(path: String, recursive: Boolean, transaction: Option[String] = None)
                       (implicit yt: YtClient): Unit = {
    if (exists(path)) {
      removeDir(path, recursive, transaction)
    }
  }

  def readTable[T](path: String, deserializer: WireRowDeserializer[T], timeout: Duration = 1.minute)
                  (implicit yt: YtClient): TableIterator[T] = {
    val request = new ReadTable(path, deserializer)
      .setOmitInaccessibleColumns(true)
      .setUnordered(true)
    val reader = yt.readTable(request).join()
    new TableIterator(reader, timeout)
  }

  def createTransaction(parent: Option[String], timeout: Duration)(implicit yt: YtClient): ApiServiceTransaction = {
    import YtClientUtils._
    val options = new ApiServiceTransactionOptions(TT_MASTER)
      .setTimeout(javaDuration(timeout))
      .setSticky(false)
      .setPing(true)
      .setPingPeriod(javaDuration(30 seconds))
    parent.foreach(p => options.setParentId(GUID.valueOf(p)))
    val tr = yt.startTransaction(options).join()
    tr
  }

  type Cancellable[T] = (Promise[Unit], Future[T])

  def cancellable[T](f: Future[Unit] => T)(implicit ec: ExecutionContext): Cancellable[T] = {
    val cancel = Promise[Unit]()
    val fut = Future {
      val res = f(cancel.future)
      if (!cancel.tryFailure(new Exception)) {
        throw new CancellationException
      }
      res
    }
    (cancel, fut)
  }

  def pingTransaction(tr: ApiServiceTransaction, interval: Duration)
                     (implicit yt: YtClient, ec: ExecutionContext): Cancellable[Unit] = {
    @tailrec
    def ping(cancel: Future[Unit], retry: Int): Unit = {
      try {
        if (!cancel.isCompleted) {
          tr.ping().join()
        }
      } catch {
        case e: Throwable =>
          log.error(s"Error in ping transaction ${tr.getId}, ${e.getMessage},\n" +
            s"Suppressed: ${e.getSuppressed.map(_.getMessage).mkString("\n")}")
          if (retry > 0) {
            Thread.sleep(new Random().nextInt(2000) + 100)
            ping(cancel, retry - 1)
          }
      }
    }

    cancellable { cancel =>
      while (!cancel.isCompleted) {
        log.info(s"Ping transaction ${tr.getId}")
        ping(cancel, 3)
        Thread.sleep(interval.toMillis)
      }
      log.info(s"Ping transaction ${tr.getId} cancelled")
    }
  }

  private def toOptional[T](x: Option[T]): Optional[T] = x match {
    case Some(value) => Optional.of(value)
    case None => Optional.empty()
  }

  def abortTransaction(guid: String)(implicit yt: YtClient): Unit = {
    yt.abortTransaction(GUID.valueOf(guid), true).join()
  }

  def commitTransaction(guid: String)(implicit yt: YtClient): Unit = {
    yt.commitTransaction(GUID.valueOf(guid), true).join()
  }

  implicit class RichGetLikeRequest[T <: GetLikeReq[_]](val request: T) {
    def optionalTransaction(transaction: Option[String]): T = {
      transaction.map { t =>
        request.setTransactionalOptions(new TransactionalOptions(GUID.valueOf(t))).asInstanceOf[T]
      }.getOrElse(request)
    }
  }

  implicit class RichMutateNodeRequest[T <: MutateNode[_]](val request: T) {
    def optionalTransaction(transaction: Option[String]): T = {
      transaction.map { t =>
        request.setTransactionalOptions(new TransactionalOptions(GUID.valueOf(t))).asInstanceOf[T]
      }.getOrElse(request)
    }
  }

  implicit class RichWriteFileRequest[T <: WriteFile](val request: T) {
    def optionalTransaction(transaction: Option[String]): T = {
      transaction.map { t =>
        request.setTransactionalOptions(new TransactionalOptions(GUID.valueOf(t))).asInstanceOf[T]
      }.getOrElse(request)
    }
  }

  def readFile(path: String, transaction: Option[String] = None, timeout: Duration = 1.minute)
              (implicit yt: YtClient): YtFileInputStream = {
    val fileReader = yt.readFile(new ReadFile(formatPath(path))).join()
    new YtFileInputStream(fileReader, timeout)
  }

  def readFileString(path: String, transaction: Option[String] = None, timeout: Duration = 1.minute)
                    (implicit yt: YtClient): String = {
    val in = readFile(path, transaction, timeout)
    try {
      Source.fromInputStream(in).mkString
    } finally {
      in.close()
    }
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
    val request = new CreateNode(formatPath(path), ObjectType.File).optionalTransaction(transaction)
    yt.createNode(request).join()
  }

  private def writeFileRequest(path: String, transaction: Option[String]): WriteFile = {
    new WriteFile(formatPath(path)).setWindowSize(10000000L).setPacketSize(1000000L).optionalTransaction(transaction)
  }

  def writeToFile(path: String, timeout: Duration, yt: YtRpcClient, transaction: Option[String]): OutputStream = {
    val writer = writeFile(writeFileRequest(path, transaction), timeout)(yt.yt).join()
    new YtFileOutputStream(writer, Some(yt))
  }

  def writeToFile(path: String, timeout: Duration, transaction: Option[String])
                 (implicit yt: YtClient): OutputStream = {
    val writer = writeFile(writeFileRequest(path, transaction), timeout).join()
    new YtFileOutputStream(writer, None)
  }

  private def writeFile(req: WriteFile, timeout: Duration)(implicit yt: YtClient): CompletableFuture[FileWriter] = {
    import YtClientUtils._
    val builder = yt.getService.writeFile
    builder.setTimeout(javaDuration(timeout))
    builder.getOptions.setTimeouts(timeout)
    req.writeTo(builder.body)
    new FileWriterImpl(builder.startStream(yt.selectDestinations()), req.getWindowSize, req.getPacketSize).startUpload
  }

  def rename(src: String, dst: String, transaction: Option[String] = None)(implicit yt: YtClient): Unit = {
    val request = new MoveNode(formatPath(src), formatPath(dst)).optionalTransaction(transaction)
    yt.moveNode(request).join()
  }

  def fileSize(path: String, transaction: Option[String] = None)(implicit yt: YtClient): Long = {
    tableAttribute(path, "compressed_data_size", transaction).longValue()
  }

  def readDocument(path: String, transaction: Option[String] = None)(implicit yt: YtClient): YTreeNode = {
    val request = new GetNode(formatPath(path)).optionalTransaction(transaction)
    yt.getNode(request).join()

  }
}
