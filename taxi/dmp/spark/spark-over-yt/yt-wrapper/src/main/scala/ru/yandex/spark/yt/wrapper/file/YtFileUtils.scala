package ru.yandex.spark.yt.wrapper.file

import java.io.OutputStream
import java.time.format.DateTimeFormatter
import java.time.{LocalDateTime, ZoneOffset}

import ru.yandex.inside.yt.kosher.ytree.YTreeNode
import ru.yandex.spark.yt.wrapper.YtJavaConverters._
import ru.yandex.spark.yt.wrapper.client.{YtClientUtils, YtRpcClient}
import ru.yandex.spark.yt.wrapper.cypress.{YtAttributes, YtCypressUtils}
import ru.yandex.spark.yt.wrapper.transaction.YtTransactionUtils
import ru.yandex.yt.ytclient.proxy.CompoundClient
import ru.yandex.yt.ytclient.proxy.request.{CreateNode, ObjectType, ReadFile, WriteFile}

import scala.concurrent.duration._
import scala.io.Source
import scala.language.postfixOps

trait YtFileUtils {
  self: YtCypressUtils with YtTransactionUtils with YtClientUtils =>

  def readFile(path: String, transaction: Option[String] = None, timeout: Duration = 1 minute)
              (implicit yt: CompoundClient): YtFileInputStream = {
    val fileReader = yt.readFile(new ReadFile(formatPath(path))).join()
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

  def createFile(path: String, transaction: Option[String] = None)(implicit yt: CompoundClient): Unit = {
    val request = new CreateNode(formatPath(path), ObjectType.File).optionalTransaction(transaction)
    yt.createNode(request).join()
  }

  private def writeFileRequest(path: String, transaction: Option[String], timeout: Duration): WriteFile = {
    new WriteFile(formatPath(path))
      .setWindowSize(10000000L)
      .setPacketSize(1000000L)
      .optionalTransaction(transaction)
      .setTimeout(toJavaDuration(timeout))
  }

  def writeFile(path: String, timeout: Duration, ytRpcClient: Option[YtRpcClient], transaction: Option[String])
               (implicit yt: CompoundClient): OutputStream = {
    val writer = yt.writeFile(writeFileRequest(path, transaction, timeout)).join()
    new YtFileOutputStream(writer, ytRpcClient)
  }

  def writeFile(path: String, timeout: Duration, transaction: Option[String])(implicit yt: CompoundClient): OutputStream = {
    val writer = yt.writeFile(writeFileRequest(path, transaction, timeout)).join()
    new YtFileOutputStream(writer, None)
  }

  def fileSize(path: String, transaction: Option[String] = None)(implicit yt: CompoundClient): Long = {
    attribute(path, YtAttributes.compressedDataSize, transaction).longValue()
  }

  def modificationTime(path: String, transaction: Option[String] = None)(implicit yt: CompoundClient): String = {
    attribute(path, YtAttributes.modificationTime, transaction).stringValue()
  }

  def modificationTimeTs(path: String, transaction: Option[String] = None)(implicit yt: CompoundClient): Long = {
    val str = modificationTime(path, transaction)
    val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS'Z'")
    val zdt = LocalDateTime.parse(str, formatter).atZone(ZoneOffset.UTC)
    zdt.toInstant.toEpochMilli
  }

  def fileSize(attrs: Map[String, YTreeNode]): Long = {
    attrs(YtAttributes.compressedDataSize).longValue()
  }
}
