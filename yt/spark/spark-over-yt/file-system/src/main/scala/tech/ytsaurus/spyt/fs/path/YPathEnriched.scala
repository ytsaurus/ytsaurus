package tech.ytsaurus.spyt.fs.path

import org.apache.hadoop.fs.Path
import tech.ytsaurus.spyt.fs.path.YPathEnriched.{YtLatestVersionPath, YtPartitionedPath, YtTimestampPath, YtTransactionPath}
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.client.CompoundClient
import tech.ytsaurus.core.GUID
import tech.ytsaurus.core.cypress.YPath

sealed trait YPathEnriched {
  def toPath: Path

  def toYPath: YPath = parent.toYPath

  def toStringPath: String = toPath.toString

  def transaction: Option[String] = None

  def parent: YPathEnriched

  def child(name: String): YPathEnriched

  def withTransaction(transactionId: String): YtTransactionPath = YtTransactionPath(this, transactionId)

  def withTransaction(transactionId: Option[String]): YPathEnriched = transactionId match {
    case Some(tr) => withTransaction(tr)
    case None => this
  }

  def withTimestamp(timestamp: Long): YtTimestampPath = YtTimestampPath(this, timestamp)

  def withLatestVersion(): YtLatestVersionPath = YtLatestVersionPath(this)

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

    override def child(name: String): YPathEnriched = ???
  }

  case class YtTransactionPath(parent: YPathEnriched, transactionId: String) extends YPathEnriched {
    override def toPath: Path = parent.toPath.child(s"@transaction_$transactionId")

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

    override def child(name: String): YPathEnriched = YtSimplePath(this, name)
  }

  case class YtRootPath(path: Path) extends YPathEnriched {
    override def toPath: Path = path

    override def toYPath: YPath = YPath.simple(YtWrapper.formatPath(path.toString))

    override def parent: YPathEnriched = null

    override def child(name: String): YPathEnriched = YtSimplePath(this, name)
  }

  sealed trait YtDynamicVersionPath extends YPathEnriched

  /**
   * Yt path with fixed timestamp, used for a consistent read from dynamic tables
   * see https://yt.yandex-team.ru/docs/description/dynamic_tables/dynamic_tables_mapreduce#dyntable_mr_timestamp
   * @param parent Path to the table
   * @param timestamp Yt timestamp (not a UNIX)
   */
  case class YtTimestampPath(parent: YPathEnriched, timestamp: Long) extends YtDynamicVersionPath {
    override def toPath: Path = parent.toPath.child(s"@timestamp_$timestamp")

    override def toYPath: YPath = parent.toYPath.withTimestamp(timestamp)

    override def child(name: String): YPathEnriched = {
      parent match {
        case p: YtTransactionPath =>
          YtSimplePath(this, name).withTransaction(p.transactionId).withTimestamp(timestamp)
        case _ =>
          YtSimplePath(this, name).withTimestamp(timestamp)
      }
    }

    override def lock()(implicit yt: CompoundClient): YPathEnriched = {
      parent.lock().withTimestamp(timestamp)
    }
  }

  /**
   * Yt path to dynamic table without fixed timestamp. Read from this path will be inconsistent and not recommended
   * If user disabled fixing timestamp manually this class will be used, otherwise YtTimestampPath will be used
   * @param parent Path to the table
   */
  case class YtLatestVersionPath(parent: YPathEnriched) extends YtDynamicVersionPath {
    override def toPath: Path = parent.toPath.child("@latest_version")

    override def toYPath: YPath = parent.toYPath

    override def child(name: String): YPathEnriched = {
      parent match {
        case p: YtTransactionPath =>
          YtSimplePath(this, name).withTransaction(p.transactionId).withLatestVersion()
        case _ =>
          YtSimplePath(this, name).withLatestVersion()
      }
    }

    override def lock()(implicit yt: CompoundClient): YPathEnriched = {
      parent.lock().withLatestVersion()
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

  object YtLatestVersionPath {
    def unapply(path: Path): Option[Path] = {
      if (path.getName == "@latest_version") {
        Some(path.getParent)
      } else {
        None
      }
    }
  }

  object YtPartitionedPath {
    def unapply(path: Path): Option[Path] = {
      if (path.getName == "@yt_partitioned") {
        Some(path.getParent)
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
      case YtLatestVersionPath(parentPath) =>
        YtLatestVersionPath(ypath(parentPath))
      case _ =>
        val tr = GlobalTableSettings.getTransaction(path.toString)
        if (path.getParent != null && path.getParent.toString.contains("@")) {
          YtSimplePath(ypath(path.getParent), path.getName).withTransaction(tr)
        } else {
          YtRootPath(path).withTransaction(tr)
        }
    }
  }
}

