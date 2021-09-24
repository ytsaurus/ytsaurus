package ru.yandex.spark.yt.fs

import org.apache.hadoop.fs.Path
import ru.yandex.inside.yt.kosher.common.GUID
import ru.yandex.inside.yt.kosher.cypress.YPath
import ru.yandex.spark.yt.fs.YPathEnriched.{YtTimestampPath, YtTransactionPath}
import ru.yandex.spark.yt.wrapper.YtWrapper
import ru.yandex.yt.ytclient.proxy.CompoundClient

sealed trait YPathEnriched {
  def toPath: Path

  def toYPath: YPath

  def toStringPath: String = toPath.toString

  def transaction: Option[String]

  def parent: YPathEnriched

  def child(name: String): YPathEnriched

  def withTransaction(transactionId: String): YtTransactionPath = YtTransactionPath(this, transactionId)

  def withTransaction(transactionId: Option[String]): YPathEnriched = transactionId match {
    case Some(tr) => withTransaction(tr)
    case None => this
  }

  def withTimestamp(timestamp: Long): YtTimestampPath = YtTimestampPath(this, timestamp)

  def lock()(implicit yt: CompoundClient): YPathEnriched = this

}

object YPathEnriched {
  implicit class RichPath(val path: Path) {
    def child(name: String): Path = {
      new Path(path, name)
    }
  }

  case class YtObjectPath(parent: YtTransactionPath, nodeId: String) extends YPathEnriched {
    override def toPath: Path = parent.toPath.child(s"@node_$nodeId")

    override def toYPath: YPath = YPath.objectRoot(GUID.valueOf(nodeId))

    override def transaction: Option[String] = None

    override def child(name: String): YPathEnriched = ???
  }

  case class YtTransactionPath(parent: YPathEnriched, transactionId: String) extends YPathEnriched {
    override def toPath: Path = parent.toPath.child(s"@transaction_$transactionId")

    override def toYPath: YPath = parent.toYPath

    override def transaction: Option[String] = Some(transactionId)


    override def child(name: String): YPathEnriched = YtSimplePath(this, name).withTransaction(transactionId)

    override def lock()(implicit yt: CompoundClient): YtObjectPath = {
      val lock = YtWrapper.lockNode(toYPath, transactionId)
      YtObjectPath(this, lock)
    }
  }

  case class YtSimplePath(parent: YPathEnriched, name: String) extends YPathEnriched {
    override def toPath: Path = parent.toPath.child(name)

    override def toYPath: YPath = parent.toYPath.child(name)

    override def transaction: Option[String] = None

    override def child(name: String): YPathEnriched = YtSimplePath(this, name)
  }

  case class YtRootPath(path: Path) extends YPathEnriched {
    override def toPath: Path = path

    override def toYPath: YPath = YPath.simple(YtWrapper.formatPath(path.toString))

    override def transaction: Option[String] = None

    override def parent: YPathEnriched = null

    override def child(name: String): YPathEnriched = YtSimplePath(this, name)
  }

  case class YtTimestampPath(parent: YPathEnriched, timestamp: Long) extends YPathEnriched {
    override def toPath: Path = parent.toPath.child(s"@timestamp_$timestamp")

    override def toYPath: YPath = parent.toYPath.withTimestamp(timestamp)

    override def transaction: Option[String] = None

    override def child(name: String): YPathEnriched = {
      parent match {
        case p: YtTransactionPath =>
          YtSimplePath(this, name).withTransaction(p.transactionId).withTimestamp(timestamp)
        case _ =>
          YtSimplePath(this, name).withTimestamp(timestamp)
      }
    }
  }

  object YtObjectPath {
    def unapply(path: Path): Option[(Path, String)] = {
      if (path.getName.startsWith("@node_")) {
        Some((path.getParent, path.getName.drop("@node_".length)))
      } else None
    }
  }

  object YtTransactionPath {
    def apply(path: Path, transactionId: String): YtTransactionPath = {
      new YtTransactionPath(ypath(path), transactionId)
    }

    def unapply(path: Path): Option[(Path, String)] = {
      if (path.getName.startsWith("@transaction_")) {
        Some(path.getParent, path.getName.drop("@transaction_".length))
      } else {
        None
      }
    }
  }

  object YtTimestampPath {
    def unapply(path: Path): Option[(Path, Long)] = {
      if (path.getName.startsWith("@timestamp_")) {
        Some(path.getParent, path.getName.drop("@timestamp_".length).toLong)
      } else {
        None
      }
    }
  }

  // TODO tailrec?
  def ypath(path: Path): YPathEnriched = {
    path match {
      case YtObjectPath(parentPath, nodeId) =>
        ypath(parentPath) match {
          case parent: YtTransactionPath =>
            YtObjectPath(parent, nodeId)
          case _ =>
            throw new IllegalArgumentException(s"Invalid path: $path")
        }
      case YtTransactionPath(parentPath, transactionId) =>
        YtTransactionPath(ypath(parentPath), transactionId)
      case YtTimestampPath(parentPath, timestamp) =>
        YtTimestampPath(ypath(parentPath), timestamp)
      case _ =>
        val tr = GlobalTableSettings.getTransaction(path.toString)
        if (path.getParent.toString.contains("@")) {
          YtSimplePath(ypath(path.getParent), path.getName).withTransaction(tr)
        } else {
          YtRootPath(path).withTransaction(tr)
        }
    }
  }
}

