package ru.yandex.spark.yt.wrapper.file

import java.io.OutputStream
import java.util.concurrent.CompletableFuture

import ru.yandex.inside.yt.kosher.ytree.YTreeNode
import ru.yandex.spark.yt.wrapper.YtJavaConverters._
import ru.yandex.spark.yt.wrapper.client.{YtClientUtils, YtRpcClient}
import ru.yandex.spark.yt.wrapper.cypress.{YtAttributes, YtCypressUtils}
import ru.yandex.spark.yt.wrapper.transaction.YtTransactionUtils
import ru.yandex.yt.ytclient.proxy.internal.FileWriterImpl
import ru.yandex.yt.ytclient.proxy.request.{CreateNode, ObjectType, ReadFile, WriteFile}
import ru.yandex.yt.ytclient.proxy.{FileWriter, YtClient}

import scala.concurrent.duration._
import scala.io.Source
import scala.language.postfixOps

trait YtFileUtils {
  self: YtCypressUtils with YtTransactionUtils with YtClientUtils =>

  def readFile(path: String, transaction: Option[String] = None, timeout: Duration = 1 minute)
              (implicit yt: YtClient): YtFileInputStream = {
    val fileReader = yt.readFile(new ReadFile(formatPath(path))).join()
    new YtFileInputStream(fileReader, timeout)
  }

  def readFileAsString(path: String, transaction: Option[String] = None, timeout: Duration = 1.minute)
                      (implicit yt: YtClient): String = {
    val in = readFile(path, transaction, timeout)
    try {
      Source.fromInputStream(in).mkString
    } finally {
      in.close()
    }
  }

  def createFile(path: String, transaction: Option[String] = None)(implicit yt: YtClient): Unit = {
    val request = new CreateNode(formatPath(path), ObjectType.File).optionalTransaction(transaction)
    yt.createNode(request).join()
  }

  private def writeFileRequest(path: String, transaction: Option[String]): WriteFile = {
    new WriteFile(formatPath(path)).setWindowSize(10000000L).setPacketSize(1000000L).optionalTransaction(transaction)
  }

  def writeFile(path: String, timeout: Duration, ytRpcClient: Option[YtRpcClient], transaction: Option[String])
               (implicit yt: YtClient): OutputStream = {
    val writer = writeFileWithTimeout(writeFileRequest(path, transaction), timeout)(yt).join()
    new YtFileOutputStream(writer, ytRpcClient)
  }

  def writeFile(path: String, timeout: Duration, transaction: Option[String])(implicit yt: YtClient): OutputStream = {
    val writer = writeFileWithTimeout(writeFileRequest(path, transaction), timeout).join()
    new YtFileOutputStream(writer, None)
  }

  def fileSize(path: String, transaction: Option[String] = None)(implicit yt: YtClient): Long = {
    attribute(path, YtAttributes.compressedDataSize, transaction).longValue()
  }

  def fileSize(attrs: Map[String, YTreeNode]): Long = {
    attrs(YtAttributes.compressedDataSize).longValue()
  }

  private def writeFileWithTimeout(req: WriteFile, timeout: Duration)
                                  (implicit yt: YtClient): CompletableFuture[FileWriter] = {
    val builder = yt.getService.writeFile
    builder.setTimeout(toJavaDuration(timeout))
    builder.getOptions.setTimeouts(timeout)
    req.writeTo(builder.body)
    new FileWriterImpl(builder.startStream(yt.selectDestinations()), req.getWindowSize, req.getPacketSize).startUpload
  }
}
