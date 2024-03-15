package tech.ytsaurus.spyt.fs

import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import tech.ytsaurus.client.CompoundClient
import tech.ytsaurus.spyt.fs.path.YPathEnriched.{YtDynamicVersionPath, YtObjectPath, YtRootPath, YtSimplePath, YtTimestampPath, YtTransactionPath, ypath}
import tech.ytsaurus.spyt.fs.path._
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.wrapper.cypress.PathType
import tech.ytsaurus.spyt.wrapper.table.TableType
import tech.ytsaurus.ysontree.YTreeNode

import java.io.FileNotFoundException
import scala.annotation.tailrec
import scala.language.postfixOps

@SerialVersionUID(1L)
class YtTableFileSystem extends YtFileSystemBase {
  private val log = LoggerFactory.getLogger(getClass)

  override def listStatus(f: Path): Array[FileStatus] = {
    log.debugLazy(s"List status $f")
    listStatus(ypath(f), expandDirectory = true)
  }

  private def listStatus(path: YPathEnriched, expandDirectory: Boolean)(implicit yt: CompoundClient = yt): Array[FileStatus] = {
    if (!YtWrapper.exists(path.toYPath, path.transaction)) {
      throw new FileNotFoundException(path.toStringPath)
    }
    val attributes = YtWrapper.attributes(path.toYPath, path.transaction)
    PathType.fromAttributes(attributes) match {
      case PathType.File => Array(getFileStatus(path.lock()))
      case PathType.Table => Array(lockTable(path, attributes))
      case PathType.Directory => if (expandDirectory) listYtDirectory(path) else Array(getFileStatus(path))
      case pathType => throw new IllegalArgumentException(s"Can't list $pathType")
    }
  }

  private def listYtDirectory(path: YPathEnriched)(implicit yt: CompoundClient): Array[FileStatus] = {
    YtWrapper.listDir(path.toYPath, path.transaction).flatMap { name => listStatus(path.child(name), expandDirectory = false) }
  }

  private def lockTable(path: YPathEnriched, attributes: Map[String, YTreeNode])
                       (implicit yt: CompoundClient): FileStatus = {
    YtWrapper.tableType(attributes) match {
      case TableType.Static => lockStaticTable(path, attributes)
      case TableType.Dynamic => lockDynamicTable(path)
    }
  }

  private lazy val isDriver: Boolean = {
    SparkSession.getDefaultSession.nonEmpty
  }

  @tailrec
  private def lockStaticTable(path: YPathEnriched, attributes: Map[String, YTreeNode])
                             (implicit yt: CompoundClient): FileStatus = {
    path match {
      case p@(_: YtSimplePath | _: YtRootPath | _: YtObjectPath) => getFileStatus(p)
      case p: YtTransactionPath => getFileStatus(p.lock())
      case p: YtTimestampPath => lockStaticTable(p.parent, attributes)
    }
  }

  private def lockDynamicTable(path: YPathEnriched)(implicit yt: CompoundClient): FileStatus = {
    path match {
      case p: YtDynamicVersionPath => getFileStatus(p.lock())
      case p@(_: YtSimplePath | _: YtObjectPath | _: YtRootPath) =>
        if (!isDriver) {
          log.warn("Generating timestamps of dynamic tables on executors causes reading files with different timestamps")
        }
        val ts = YtWrapper.maxAvailableTimestamp(p.toYPath)
        getFileStatus(p.withTimestamp(ts))
      case p: YtTransactionPath =>
        getFileStatus(p.lock())
    }
  }

  override def getFileStatus(f: Path): FileStatus = {
    getFileStatus(f, ypath(f))
  }

  private def getFileStatus(path: YPathEnriched): FileStatus = {
    getFileStatus(path.toPath, path)
  }

  private def getFileStatus(f: Path, path: YPathEnriched): FileStatus = {
    log.debugLazy(s"Get file status $f")
    if (!YtWrapper.exists(path.toYPath, path.transaction)(yt)) {
      throw new FileNotFoundException(s"File $path is not found")
    } else {
      val attributes = YtWrapper.attributes(path.toYPath, path.transaction)(yt)
      val modificationTime = YtWrapper.modificationTimeTs(attributes)
      YtWrapper.pathType(attributes) match {
        case PathType.File =>
          val size = YtWrapper.fileSize(attributes)
          new FileStatus(size, false, 1, size, modificationTime, f)
        case PathType.Table =>
          val size = YtWrapper.fileSize(attributes) max 1L // NB: Size may be 0 when a real data exists
          val optimizeMode = YtWrapper.optimizeMode(attributes)
          val (rowCount, isDynamic) = YtWrapper.tableType(attributes) match {
            case TableType.Static => (YtWrapper.rowCount(attributes), false)
            case TableType.Dynamic => (YtWrapper.chunkRowCount(attributes), true)
          }
          val richPath = YtHadoopPath(path, YtTableMeta(rowCount, size, modificationTime, optimizeMode, isDynamic))
          YtFileStatus.toFileStatus(richPath)
        case PathType.Directory => new FileStatus(0, true, 1, 0, modificationTime, f)
        case PathType.None => null
      }
    }
  }

  override def create(f: Path, permission: FsPermission, overwrite: Boolean, bufferSize: Int,
                      replication: Short, blockSize: Long, progress: Progressable): FSDataOutputStream = {
    create(f, permission, overwrite, bufferSize, replication, blockSize, progress, statistics)
  }

  override def open(f: Path, bufferSize: Int): FSDataInputStream = {
    log.debugLazy(s"Open file ${f.toUri.toString}")
    val path = ypath(f)
    val is = YtWrapper.readFile(path.toYPath, path.transaction, _ytConf.timeout)(yt)
    new FSDataInputStream(new YtFsInputStream(is, statistics))
  }
}
