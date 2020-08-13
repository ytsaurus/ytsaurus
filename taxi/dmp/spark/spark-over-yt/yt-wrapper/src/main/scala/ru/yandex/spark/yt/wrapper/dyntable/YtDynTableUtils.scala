package ru.yandex.spark.yt.wrapper.dyntable

import java.io.ByteArrayOutputStream

import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTreeBuilder
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.{YTreeBinarySerializer, YTreeTextSerializer}
import ru.yandex.inside.yt.kosher.ytree.YTreeNode
import ru.yandex.spark.yt.wrapper.YtJavaConverters._
import ru.yandex.spark.yt.wrapper.cypress.{YtAttributes, YtCypressUtils}
import ru.yandex.yt.ytclient.proxy.YtClient

import scala.annotation.tailrec
import scala.concurrent.TimeoutException
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

trait YtDynTableUtils {
  self: YtCypressUtils =>

  type PivotKey = Array[Byte]
  val emptyPivotKey: PivotKey = serialiseYson(new YTreeBuilder().beginMap().endMap().build())

  private def serialiseYson(node: YTreeNode): Array[Byte] = {
    val baos = new ByteArrayOutputStream
    try {
      YTreeBinarySerializer.serialize(node, baos)
      baos.toByteArray
    } finally {
      baos.close()
    }
  }

  def pivotKeysYson(path: String)(implicit yt: YtClient): Seq[YTreeNode] = {
    import scala.collection.JavaConverters._
    yt
      .getTablePivotKeys(formatPath(path), toJavaDuration(5 minutes))
      .join()
      .asScala
  }

  def pivotKeys(path: String)(implicit yt: YtClient): Seq[PivotKey] = {
    pivotKeysYson(path).map(serialiseYson)
  }

  def keyColumns(path: String, transaction: Option[String] = None)(implicit yt: YtClient): Seq[String] = {
    keyColumns(attribute(path, YtAttributes.sortedBy, transaction))
  }

  def keyColumns(attr: YTreeNode): Seq[String] = {
    import scala.collection.JavaConverters._
    attr.asList().asScala.map(_.stringValue())
  }

  def keyColumns(attrs: Map[String, YTreeNode]): Seq[String] = {
    keyColumns(attrs(YtAttributes.sortedBy))
  }

  def mountTable(path: String)(implicit yt: YtClient): Unit = {
    yt.mountTable(formatPath(path)).join()
  }

  def unmountTable(path: String)(implicit yt: YtClient): Unit = {
    yt.unmountTable(formatPath(path)).join()
  }

  def waitState(path: String, state: TabletState, timeout: Duration)
               (implicit yt: YtClient): Try[Unit] = {
    @tailrec
    def waitUnmount(timeoutMillis: Long): Try[Unit] = {
      tabletState(path) match {
        case s if s == state => Success()
        case _ if timeoutMillis > 0 =>
          Thread.sleep(1000)
          waitUnmount(timeoutMillis - 1000)
        case _ => Failure(new TimeoutException)
      }
    }

    waitUnmount(timeout.toMillis)
  }

  def tabletState(path: String)(implicit yt: YtClient): TabletState = {
    TabletState.fromString(attribute(formatPath(path), "tablet_state").stringValue())
  }

  def remountTable(path: String)(implicit yt: YtClient): Unit = {
    yt.remountTable(formatPath(path)).join()
  }

  sealed abstract class TabletState(val name: String)

  object TabletState {

    case object Mounted extends TabletState("mounted")

    case object Unmounted extends TabletState("unmounted")

    case object Unknown extends TabletState("")

    def fromString(str: String): TabletState = {
      Seq(Mounted, Unmounted).find(_.name == str).getOrElse(Unknown)
    }
  }

}
