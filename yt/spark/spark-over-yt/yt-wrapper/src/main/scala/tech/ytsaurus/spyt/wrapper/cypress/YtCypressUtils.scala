package tech.ytsaurus.spyt.wrapper.cypress

import org.slf4j.LoggerFactory
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.wrapper.YtWrapper.RichLogger
import tech.ytsaurus.spyt.wrapper.transaction.YtTransactionUtils
import tech.ytsaurus.client.CompoundClient
import tech.ytsaurus.client.request._
import tech.ytsaurus.core.GUID
import tech.ytsaurus.core.cypress.{CypressNodeType, YPath}
import tech.ytsaurus.core.request.LockMode
import tech.ytsaurus.ysontree.{YTree, YTreeNode}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

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

  def getNodeType(path: String, transaction: Option[String] = None)
                 (implicit yt: CompoundClient): Option[CypressNodeType] = {
    getNodeType(YPath.simple(formatPath(path)), transaction)
  }

  def getNodeType(path: YPath, transaction: Option[String])
                 (implicit yt: CompoundClient): Option[CypressNodeType] = {
    val fp = path.justPath().attribute("type")
    if (yt.existsNode(ExistsNode.builder().setPath(fp).optionalTransaction(transaction).build()).join()) {
      val nt = yt.getNode(GetNode.builder().setPath(fp).optionalTransaction(transaction).build())
        .join()
        .stringValue()
      Some(CypressNodeType.R.fromName(nt))
    } else
      None
  }

  def isDir(path: String, transaction: Option[String] = None)(implicit yt: CompoundClient): Boolean = {
    isDir(YPath.simple(formatPath(path)), transaction)
  }

  def isDir(path: YPath, transaction: Option[String])(implicit yt: CompoundClient): Boolean =
    try {
      getNodeType(path, transaction).contains(CypressNodeType.MAP)
    } catch {
      case NonFatal(e) =>
        log.debug(s"Cannot get node type: ${e.getMessage}", e)
        false
    }

  def createLink(sourcePath: String, destPath: String, transaction: Option[String] = None,
                 ignoreExisting: Boolean = false)(implicit yt: CompoundClient): Unit = {
    log.debug(s"Creating link $sourcePath -> $destPath, transaction $transaction")
    yt.createNode(
      CreateNode.builder()
      .setPath(YPath.simple(formatPath(destPath)))
      .setType(CypressNodeType.LINK)
      .optionalTransaction(transaction)
      .setAttributes(Map[String, YTreeNode]("target_path" -> YTree.stringNode(sourcePath)).asJava)
      .setIgnoreExisting(ignoreExisting)
      .setRecursive(true)
      .build()
    ).join()
  }

  def createDir(path: String, transaction: Option[String] = None, ignoreExisting: Boolean = false)
               (implicit yt: CompoundClient): Unit = {
    createDir(YPath.simple(formatPath(path)), transaction, ignoreExisting)
  }

  def createDir(path: YPath, transaction: Option[String], ignoreExisting: Boolean)
               (implicit yt: CompoundClient): Unit = {
    log.debug(s"Create new directory: $path, transaction $transaction")
    if (!ignoreExisting || !isDir(path, transaction)) {
      yt.createNode(
        CreateNode.builder()
          .setPath(path)
          .setType(CypressNodeType.MAP)
          .setRecursive(true)
          .setIgnoreExisting(ignoreExisting)
          .optionalTransaction(transaction)
          .build()
      ).join()
    }
  }

  def listDir(path: String, transaction: Option[String] = None)(implicit yt: CompoundClient): Array[String] = {
    listDir(YPath.simple(formatPath(path)), transaction)
  }

  def listDir(path: YPath, transaction: Option[String])(implicit yt: CompoundClient): Array[String] = {
    log.debug(s"List directory: $path, transaction $transaction")
    import scala.collection.JavaConverters._
    val request = ListNode.builder().setPath(path).optionalTransaction(transaction).build()
    val response = yt.listNode(request).join().asList()
    response.asScala.view.map(_.stringValue()).toArray
  }

  def copy(src: String, dst: String, transaction: Option[String] = None, force: Boolean = false)
          (implicit yt: CompoundClient): Unit = {
    log.debug(s"Copy: $src -> $dst, transaction $transaction, force: $force")
    yt.copyNode(
      CopyNode.builder()
        .setSource(formatPath(src))
        .setDestination(formatPath(dst))
        .setForce(force)
        .optionalTransaction(transaction)
        .build()
    ).join()
  }

  def move(src: String, dst: String, transaction: Option[String] = None, force: Boolean = false)
          (implicit yt: CompoundClient): Unit = {
    log.debug(s"Move: $src -> $dst, transaction $transaction, force: $force")
    yt.moveNode(
      MoveNode.builder()
        .setSource(formatPath(src))
        .setDestination(formatPath(dst))
        .setForce(force)
        .optionalTransaction(transaction)
        .build()
    ).join()
  }

  def remove(path: String, transaction: Option[String] = None)(implicit yt: CompoundClient): Unit = {
    log.debug(s"Remove: $path, transaction $transaction")
    val request = RemoveNode.builder()
      .setPath(YPath.simple(formatPath(path)))
      .setRecursive(true)
      .optionalTransaction(transaction)
      .build()
    yt.removeNode(request).join()
  }

  def removeDir(path: String, recursive: Boolean, transaction: Option[String] = None)
               (implicit yt: CompoundClient): Unit = {
    log.debug(s"Remove directory: $path, transaction $transaction")
    val request = RemoveNode.builder()
      .setPath(YPath.simple(formatPath(path)))
      .setRecursive(true)
      .optionalTransaction(transaction)
      .build()
    yt.removeNode(request).join()
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

  def pathType(path: YPath, transaction: Option[String] = None)(implicit yt: CompoundClient): PathType = {
    val objectType = attribute(path, YtAttributes.`type`, transaction).stringValue()
    PathType.fromString(objectType)
  }

  def pathType(path: String, transaction: Option[String])(implicit yt: CompoundClient): PathType = {
    pathType(YPath.simple(formatPath(path)), transaction)
  }

  def pathType(attrs: Map[String, YTreeNode]): PathType = {
    PathType.fromString(attrs(YtAttributes.`type`).stringValue())
  }

  def exists(path: String)(implicit yt: CompoundClient): Boolean = exists(path, None)

  def exists(path: String, transaction: Option[String])(implicit yt: CompoundClient): Boolean = {
    exists(YPath.simple(formatPath(path)), transaction)
  }

  def exists(path: YPath, transaction: Option[String] = None)(implicit yt: CompoundClient): Boolean = {
    log.debug(s"Exists: $path, transaction $transaction")
    val request = ExistsNode.builder().setPath(path.allAttributes()).optionalTransaction(transaction).build()
    yt.existsNode(request).join().booleanValue()
  }

  def attribute(path: YPath, attrName: String, transaction: Option[String])
               (implicit yt: CompoundClient): YTreeNode = {
    log.debug(s"Get attribute: $path/@$attrName, transaction $transaction")
    val request = GetNode.builder().setPath(path.attribute(attrName)).optionalTransaction(transaction).build()
    yt.getNode(request).join()
  }

  def attribute(path: String, attrName: String, transaction: Option[String] = None)
               (implicit yt: CompoundClient): YTreeNode = {
    attribute(YPath.simple(formatPath(path)), attrName, transaction)
  }

  def attributes(path: YPath, transaction: Option[String] = None, attrNames: Set[String] = Set.empty)
                (implicit yt: CompoundClient): Map[String, YTreeNode] = {
    import scala.collection.JavaConverters._
    log.debug(s"Get attributes: $path/@$attrNames, transaction $transaction")
    val request = GetNode.builder().setPath(path.allAttributes()).optionalTransaction(transaction).build()
    val map = yt.getNode(request).join().asMap().asScala
    val filteredMap = if (attrNames.nonEmpty) map.filterKeys(attrNames.contains) else map
    filteredMap.toMap
  }

  def attributes(path: String, transaction: Option[String], attrNames: Set[String])
                (implicit yt: CompoundClient): Map[String, YTreeNode] = {
    attributes(YPath.simple(formatPath(path)), transaction, attrNames)
  }

  def setAttribute(path: String, attrName: String, attrValue: YTreeNode, transaction: Option[String] = None)
                  (implicit yt: CompoundClient): Unit = {
    log.debug(s"Set attribute: $path/@$attrName, transaction $transaction")
    val request = SetNode.builder()
      .setPath(YPath.simple(s"${formatPath(path)}/@$attrName"))
      .setValue(attrValue)
      .optionalTransaction(transaction)
      .build()
    yt.setNode(request).join()
  }

  def readDocument(path: String, transaction: Option[String] = None)(implicit yt: CompoundClient): YTreeNode = {
    log.debug(s"Read document: $path, transaction $transaction")
    val request = GetNode.builder()
      .setPath(YPath.simple(formatPath(path)))
      .optionalTransaction(transaction)
      .build()
    yt.getNode(request).join()
  }

  def createEmptyDocument(path: String, transaction: Option[String] = None)
                         (implicit yt: CompoundClient): Unit = {
    log.debug(s"Create document: $path, transaction $transaction")
    val request = CreateNode.builder()
      .setPath(YPath.simple(formatPath(path)))
      .setType(CypressNodeType.DOCUMENT)
      .optionalTransaction(transaction)
      .build()
    yt.createNode(request).join()
  }

  def createDocument[T: YsonWriter](path: String, doc: T, transaction: Option[String] = None)
                                   (implicit yt: CompoundClient): Unit = {
    import YsonSyntax._
    createEmptyDocument(formatPath(path), transaction)
    val request = SetNode.builder()
      .setPath(YPath.simple(formatPath(path)))
      .setValue(doc.toYson)
      .optionalTransaction(transaction)
      .build()
    yt.setNode(request).join()
  }

  def createDocumentFromProduct[T <: Product](path: String, doc: T, transaction: Option[String] = None)
                                             (implicit yt: CompoundClient): Unit = {
    import YsonableProduct._
    createDocument(path, doc, transaction)
  }

  def concatenate(from: Array[String], to: String, transaction: Option[String] = None)
                 (implicit yt: CompoundClient): Unit = {
    import scala.collection.JavaConverters._
    log.debug(s"Concatenate: ${from.mkString(",")} -> $to, transaction $transaction")
    val request = ConcatenateNodes.builder()
      .setSourcePaths(from.map(formatPath).map(YPath.simple).toList.asJava)
      .setDestinationPath(YPath.simple(formatPath(to)))
      .optionalTransaction(transaction)
      .build()
    yt.concatenateNodes(request).join()
  }

  def lockNode(path: String, transaction: String, mode: LockMode)
              (implicit yt: CompoundClient): String = {
    lockNode(YPath.simple(formatPath(path)), transaction, mode)
  }

  def lockNode(path: YPath, transaction: String, mode: LockMode = LockMode.Snapshot)
              (implicit yt: CompoundClient): String = {
    val request = LockNode.builder().setPath(path).setMode(mode).optionalTransaction(Some(transaction)).build()
    yt.lockNode(request).join().nodeId.toString
  }

  def lockCount(path: String)(implicit yt: CompoundClient): Long = {
    attribute(path, "lock_count").longValue()
  }

  def lockCount(path: YPath)(implicit yt: CompoundClient): Long = {
    YtWrapper.attribute(path, "lock_count", None).longValue()
  }

  def objectPath(path: String, transaction: Option[String])
                (implicit yt: CompoundClient): YPath = {
    val nodeId = attribute(path, "id", transaction).stringValue()
    YPath.objectRoot(GUID.valueOf(nodeId))
  }

  def objectPath(path: String, transaction: String)
                (implicit yt: CompoundClient): YPath = {
    objectPath(path, Some(transaction))
  }
}
