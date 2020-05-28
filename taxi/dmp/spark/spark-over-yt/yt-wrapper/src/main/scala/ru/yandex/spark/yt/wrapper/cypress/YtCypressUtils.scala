package ru.yandex.spark.yt.wrapper.cypress

import ru.yandex.inside.yt.kosher.cypress.YPath
import ru.yandex.inside.yt.kosher.ytree.YTreeNode
import ru.yandex.spark.yt.wrapper.transaction.YtTransactionUtils
import ru.yandex.yt.ytclient.proxy.YtClient
import ru.yandex.yt.ytclient.proxy.request._

trait YtCypressUtils {
  self: YtTransactionUtils =>

  def formatPath(path: String): String = if (path.startsWith("//")) path else "/" + path

  def createDir(path: String, transaction: Option[String] = None, ignoreExisting: Boolean = false)
               (implicit yt: YtClient): Unit = {
    yt.createNode(
      new CreateNode(formatPath(path), ObjectType.MapNode)
        .setRecursive(true)
        .setIgnoreExisting(ignoreExisting)
        .optionalTransaction(transaction)
    ).join()
  }

  def listDir(path: String, transaction: Option[String] = None)(implicit yt: YtClient): Array[String] = {
    val response = yt.listNode(new ListNode(formatPath(path)).optionalTransaction(transaction)).join().asList()
    val array = new Array[String](response.length())
    response.zipWithIndex().forEach((t: YTreeNode, i: java.lang.Integer) => {
      array(i) = t.stringValue()
    })
    array
  }

  def move(src: String, dst: String, transaction: Option[String] = None)(implicit yt: YtClient): Unit = {
    yt.moveNode(new MoveNode(formatPath(src), formatPath(dst)).optionalTransaction(transaction)).join()
  }

  def remove(path: String, transaction: Option[String] = None)(implicit yt: YtClient): Unit = {
    yt.removeNode(new RemoveNode(formatPath(path)).optionalTransaction(transaction)).join()
  }

  def removeDir(path: String, recursive: Boolean, transaction: Option[String] = None)
               (implicit yt: YtClient): Unit = {
    yt.removeNode(new RemoveNode(formatPath(path)).setRecursive(true).optionalTransaction(transaction)).join()
  }

  def removeIfExists(path: String, transaction: Option[String] = None)(implicit yt: YtClient): Unit = {
    if (exists(path)) {
      remove(path, transaction)
    }
  }

  def removeDirIfExists(path: String, recursive: Boolean, transaction: Option[String] = None)
                       (implicit yt: YtClient): Unit = {
    if (exists(path)) {
      removeDir(path, recursive, transaction)
    }
  }

  def pathType(path: String, transaction: Option[String] = None)(implicit yt: YtClient): PathType = {
    val objectType = attribute(path, "type", transaction).stringValue()
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

  def attribute(path: String, attrName: String, transaction: Option[String] = None)
               (implicit yt: YtClient): YTreeNode = {
    val request = new GetNode(s"${formatPath(path)}/@$attrName").optionalTransaction(transaction)
    yt.getNode(request).join()
  }

  def rename(src: String, dst: String, transaction: Option[String] = None)(implicit yt: YtClient): Unit = {
    val request = new MoveNode(formatPath(src), formatPath(dst)).optionalTransaction(transaction)
    yt.moveNode(request).join()
  }

  def readDocument(path: String, transaction: Option[String] = None)(implicit yt: YtClient): YTreeNode = {
    val request = new GetNode(formatPath(path)).optionalTransaction(transaction)
    yt.getNode(request).join()
  }

  def createEmptyDocument(path: String, transaction: Option[String] = None)
                         (implicit yt: YtClient): Unit = {
    val request = new CreateNode(formatPath(path), ObjectType.Document).optionalTransaction(transaction)
    yt.createNode(request).join()
  }

  def createDocument[T: YsonWriter](path: String, doc: T, transaction: Option[String] = None)
                                   (implicit yt: YtClient): Unit = {
    import YsonSyntax._
    createEmptyDocument(formatPath(path), transaction)
    val request = new SetNode(YPath.simple(formatPath(path)), doc.toYson).optionalTransaction(transaction)
    yt.setNode(request).join()
  }

  def createDocumentFromProduct[T <: Product](path: String, doc: T, transaction: Option[String] = None)
                                             (implicit yt: YtClient): Unit = {
    import YsonableProduct._
    createDocument(path, doc, transaction)
  }
}
