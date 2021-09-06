package ru.yandex.spark.yt.wrapper.cypress

import org.slf4j.LoggerFactory
import ru.yandex.inside.yt.kosher.cypress.YPath
import ru.yandex.inside.yt.kosher.ytree.YTreeNode
import ru.yandex.spark.yt.wrapper.YtWrapper.RichLogger
import ru.yandex.spark.yt.wrapper.transaction.YtTransactionUtils
import ru.yandex.yt.ytclient.proxy.CompoundClient
import ru.yandex.yt.ytclient.proxy.request._

trait YtCypressUtils {
  self: YtTransactionUtils =>

  private val log = LoggerFactory.getLogger(getClass)

  def formatPath(path: String): String = {
    log.debugLazy(s"Formatting path $path")
    if (!path.startsWith("/")) {
      if (path.contains(":/")) {
        formatPath('/' + path.split(":", 2).last.dropWhile(_ == '/'))
      } else {
        throw new IllegalArgumentException(s"Relative paths are not allowed: $path")
      }
    }
    else if (path.startsWith("//")) path
    else "/" + path
  }

  def escape(s: String): String = s.replaceAll("([\\\\/@&*\\[{])", "\\\\$1")

  def createDir(path: String, transaction: Option[String] = None, ignoreExisting: Boolean = false)
               (implicit yt: CompoundClient): Unit = {
    log.debug(s"Create new directory: $path, transaction $transaction")
    yt.createNode(
      new CreateNode(formatPath(path), ObjectType.MapNode)
        .setRecursive(true)
        .setIgnoreExisting(ignoreExisting)
        .optionalTransaction(transaction)
    ).join()
  }

  def listDir(path: String, transaction: Option[String] = None)(implicit yt: CompoundClient): Array[String] = {
    log.debug(s"List directory: $path, transaction $transaction")
    import scala.collection.JavaConverters._
    val request = new ListNode(formatPath(path)).optionalTransaction(transaction)
    val response = yt.listNode(request).join().asList()
    response.asScala.view.map(_.stringValue()).toArray
  }

  def move(src: String, dst: String, transaction: Option[String] = None)(implicit yt: CompoundClient): Unit = {
    log.debug(s"Move: $src -> $dst, transaction $transaction")
    yt.moveNode(new MoveNode(formatPath(src), formatPath(dst)).optionalTransaction(transaction)).join()
  }

  def remove(path: String, transaction: Option[String] = None)(implicit yt: CompoundClient): Unit = {
    log.debug(s"Remove: $path, transaction $transaction")
    yt.removeNode(new RemoveNode(formatPath(path)).optionalTransaction(transaction)).join()
  }

  def removeDir(path: String, recursive: Boolean, transaction: Option[String] = None)
               (implicit yt: CompoundClient): Unit = {
    log.debug(s"Remove directory: $path, transaction $transaction")
    yt.removeNode(new RemoveNode(formatPath(path)).setRecursive(true).optionalTransaction(transaction)).join()
  }

  def removeIfExists(path: String, transaction: Option[String] = None)(implicit yt: CompoundClient): Unit = {
    if (exists(path)) {
      remove(path, transaction)
    }
  }

  def removeDirIfExists(path: String, recursive: Boolean, transaction: Option[String] = None)
                       (implicit yt: CompoundClient): Unit = {
    if (exists(path)) {
      removeDir(path, recursive, transaction)
    }
  }

  def pathType(path: String, transaction: Option[String] = None)(implicit yt: CompoundClient): PathType = {
    val objectType = attribute(path, YtAttributes.`type`, transaction).stringValue()
    PathType.fromString(objectType)
  }

  def pathType(attrs: Map[String, YTreeNode]): PathType = {
    PathType.fromString(attrs(YtAttributes.`type`).stringValue())
  }

  def exists(path: String)(implicit yt: CompoundClient): Boolean = exists(path, None)

  def exists(path: String, transaction: Option[String] = None)(implicit yt: CompoundClient): Boolean = {
    log.debug(s"Exists: $path, transaction $transaction")
    val request = new ExistsNode(s"${formatPath(path)}/@").optionalTransaction(transaction)
    yt.existsNode(request).join().booleanValue()
  }

  def attribute(path: String, attrName: String, transaction: Option[String] = None)
               (implicit yt: CompoundClient): YTreeNode = {
    log.debug(s"Get attribute: $path/@$attrName, transaction $transaction")
    val request = new GetNode(s"${formatPath(path)}/@$attrName").optionalTransaction(transaction)
    yt.getNode(request).join()
  }

  def attributes(path: String, transaction: Option[String] = None, attrNames: Set[String] = Set.empty)
                (implicit yt: CompoundClient): Map[String, YTreeNode] = {
    import scala.collection.JavaConverters._
    log.debug(s"Get attributes: $path/@$attrNames, transaction $transaction")
    val request = new GetNode(s"${formatPath(path)}/@").optionalTransaction(transaction)
    val map = yt.getNode(request).join().asMap().asScala
    val filteredMap = if (attrNames.nonEmpty) map.filterKeys(attrNames.contains) else map
    filteredMap.toMap
  }

  def setAttribute(path: String, attrName: String, attrValue: YTreeNode, transaction: Option[String] = None)
                  (implicit yt: CompoundClient): Unit = {
    log.debug(s"Set attribute: $path/@$attrName, transaction $transaction")
    val request = new SetNode(YPath.simple(s"${formatPath(path)}/@$attrName"), attrValue)
      .optionalTransaction(transaction)
    yt.setNode(request).join()
  }

  def rename(src: String, dst: String, transaction: Option[String] = None)(implicit yt: CompoundClient): Unit = {
    log.debug(s"Rename: $src -> $dst, transaction $transaction")
    val request = new MoveNode(formatPath(src), formatPath(dst)).optionalTransaction(transaction)
    yt.moveNode(request).join()
  }

  def readDocument(path: String, transaction: Option[String] = None)(implicit yt: CompoundClient): YTreeNode = {
    log.debug(s"Read document: $path, transaction $transaction")
    val request = new GetNode(formatPath(path)).optionalTransaction(transaction)
    yt.getNode(request).join()
  }

  def createEmptyDocument(path: String, transaction: Option[String] = None)
                         (implicit yt: CompoundClient): Unit = {
    log.debug(s"Create document: $path, transaction $transaction")
    val request = new CreateNode(formatPath(path), ObjectType.Document).optionalTransaction(transaction)
    yt.createNode(request).join()
  }

  def createDocument[T: YsonWriter](path: String, doc: T, transaction: Option[String] = None)
                                   (implicit yt: CompoundClient): Unit = {
    import YsonSyntax._
    createEmptyDocument(formatPath(path), transaction)
    val request = new SetNode(YPath.simple(formatPath(path)), doc.toYson).optionalTransaction(transaction)
    yt.setNode(request).join()
  }

  def createDocumentFromProduct[T <: Product](path: String, doc: T, transaction: Option[String] = None)
                                             (implicit yt: CompoundClient): Unit = {
    import YsonableProduct._
    createDocument(path, doc, transaction)
  }

  /**
   * @deprecated Do not use before YT 21.1 release
   */
  @Deprecated
  def concatenate(from: Array[String], to: String, transaction: Option[String] = None)
                 (implicit yt: CompoundClient): Unit = {
    log.debug(s"Concatenate: ${from.mkString(",")} -> $to, transaction $transaction")
    val request = new ConcatenateNodes(from.map(formatPath), formatPath(to)).optionalTransaction(transaction)
    yt.concatenateNodes(request).join()
  }
}
