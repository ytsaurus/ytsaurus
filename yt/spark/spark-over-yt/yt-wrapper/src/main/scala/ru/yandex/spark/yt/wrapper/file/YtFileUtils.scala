package ru.yandex.spark.yt.wrapper.file

import org.slf4j.LoggerFactory
import ru.yandex.spark.yt.wrapper.YtJavaConverters._
import ru.yandex.spark.yt.wrapper.client.{YtClientUtils, YtRpcClient}
import ru.yandex.spark.yt.wrapper.cypress.{YtAttributes, YtCypressUtils}
import ru.yandex.spark.yt.wrapper.transaction.YtTransactionUtils
import tech.ytsaurus.client.CompoundClient
import tech.ytsaurus.client.request.{CreateNode, ReadFile, WriteFile}
import tech.ytsaurus.core.cypress.{CypressNodeType, YPath}
import tech.ytsaurus.ysontree.YTreeNode

import java.io.{FileOutputStream, OutputStream}
import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneOffset}
import scala.concurrent.duration._
import scala.io.Source
import scala.language.postfixOps

trait YtFileUtils {
  self: YtCypressUtils with YtTransactionUtils with YtClientUtils =>

  private val log = LoggerFactory.getLogger(getClass)

  def readFile(path: String, transaction: Option[String] = None, timeout: Duration = 1 minute)
              (implicit yt: CompoundClient): YtFileInputStream = {
    readFile(YPath.simple(formatPath(path)), transaction, timeout)
  }

  def readFile(path: YPath, transaction: Option[String], timeout: Duration)
              (implicit yt: CompoundClient): YtFileInputStream = {
    log.debug(s"Read file: $path, transaction: $transaction")
    val fileReader = yt.readFile(ReadFile.builder().setPath(path.toString).optionalTransaction(transaction)).join()
    new YtFileInputStream(fileReader, timeout)
  }

  def readFileAsString(path: String, transaction: Option[String] = None, timeout: Duration = 1.minute)
                      (implicit yt: CompoundClient): String = {
    val in = readFile(path, transaction, timeout)
    try {
      Source.fromInputStream(in).mkString
    } finally {
      in.close()
    }
  }

  def downloadFile(path: String, localPath: String)(implicit yt: CompoundClient): Unit = {
    val is = readFile(path)
    try {
      val os = new FileOutputStream(localPath)
      try {
        val b = new Array[Byte](65536)
        Stream.continually(is.read(b)).takeWhile(_ > 0).foreach(os.write(b, 0, _))
      } finally {
        os.close()
      }
    } finally {
      is.close()
    }
  }

  def createFile(path: String, transaction: Option[String] = None, force: Boolean = false)
                (implicit yt: CompoundClient): Unit = {
    createFile(YPath.simple(formatPath(path)), transaction, force)
  }

  def createFile(path: YPath, transaction: Option[String], force: Boolean)
                (implicit yt: CompoundClient): Unit = {
    log.debug(s"Create file: $path, transaction: $transaction")
    val request = CreateNode.builder().setPath(path).setType(CypressNodeType.FILE).optionalTransaction(transaction).setForce(force)
    yt.createNode(request).join()
  }

  private def writeFileRequest(path: YPath, transaction: Option[String], timeout: Duration): WriteFile.Builder = {
    WriteFile.builder()
      .setPath(path.toString)
      .setWindowSize(10000000L)
      .setPacketSize(1000000L)
      .optionalTransaction(transaction)
      .setTimeout(toJavaDuration(timeout))
  }

  def writeFile(path: YPath, timeout: Duration, ytRpcClient: Option[YtRpcClient], transaction: Option[String])
               (implicit yt: CompoundClient): OutputStream = {
    log.debug(s"Write file: $path, transaction: $transaction")
    val writer = yt.writeFile(writeFileRequest(path, transaction, timeout)).join()
    new YtFileOutputStream(writer, ytRpcClient)
  }

  def writeFile(path: String, timeout: Duration, ytRpcClient: Option[YtRpcClient], transaction: Option[String])
               (implicit yt: CompoundClient): OutputStream = {
    writeFile(YPath.simple(formatPath(path)), timeout, ytRpcClient, transaction)
  }

  def writeFile(path: String, timeout: Duration, transaction: Option[String])
               (implicit yt: CompoundClient): OutputStream = {
    writeFile(path, timeout, None, transaction)
  }

  def fileSize(path: YPath, transaction: Option[String] = None)(implicit yt: CompoundClient): Long = {
    attribute(path, YtAttributes.compressedDataSize, transaction).longValue()
  }

  def fileSize(path: String, transaction: Option[String])(implicit yt: CompoundClient): Long = {
    attribute(path, YtAttributes.compressedDataSize, transaction).longValue()
  }

  def fileSize(attrs: Map[String, YTreeNode]): Long = {
    attrs(YtAttributes.compressedDataSize).longValue()
  }

  def modificationTimeTs(path: YPath, transaction: Option[String])(implicit yt: CompoundClient): Long = {
    modificationTimeTs(modificationTime(path, transaction))
  }

  def modificationTime(path: YPath, transaction: Option[String])(implicit yt: CompoundClient): String = {
    attribute(path, YtAttributes.modificationTime, transaction).stringValue()
  }

  def modificationTime(path: String, transaction: Option[String] = None)(implicit yt: CompoundClient): String = {
    attribute(path, YtAttributes.modificationTime, transaction).stringValue()
  }

  def modificationTime(attributes: Map[String, YTreeNode]): String = {
    attributes(YtAttributes.modificationTime).stringValue()
  }

  private val tsFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'")

  private def modificationTimeTs(dateTime: String): Long = {
    val zdt = LocalDateTime.parse(dateTime, tsFormatter).atZone(ZoneOffset.UTC)
    zdt.toInstant.toEpochMilli
  }

  def modificationTimeTs(path: String, transaction: Option[String] = None)(implicit yt: CompoundClient): Long = {
    modificationTimeTs(modificationTime(path, transaction))
  }

  def modificationTimeTs(attributes: Map[String, YTreeNode]): Long = {
    modificationTimeTs(modificationTime(attributes))
  }
}
