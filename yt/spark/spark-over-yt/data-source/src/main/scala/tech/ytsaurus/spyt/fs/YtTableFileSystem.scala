package tech.ytsaurus.spyt.fs

import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import tech.ytsaurus.spyt.fs.path.YPathEnriched.{YtDynamicVersionPath, YtObjectPath, YtPartitionedPath, YtRootPath, YtSimplePath, YtTimestampPath, YtTransactionPath, ypath}
import tech.ytsaurus.spyt.fs.path._
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.wrapper.cypress.PathType
import tech.ytsaurus.spyt.wrapper.table.TableType
import tech.ytsaurus.client.CompoundClient
import tech.ytsaurus.ysontree.YTreeNode

import java.io.FileNotFoundException
import scala.annotation.tailrec
import scala.language.postfixOps

@SerialVersionUID(1L)
class YtTableFileSystem extends YtFileSystemBase {
  private val log = LoggerFactory.getLogger(getClass)

  override def listStatus(f: Path): Array[FileStatus] = {
    log.debugLazy(s"List status $f")
    implicit val ytClient: CompoundClient = yt

    val path = ypath(f)

    val attributes = YtWrapper.attributes(path.toYPath, path.transaction)

    PathType.fromAttributes(attributes) match {
      case PathType.File => lockFile(path)
      case PathType.Table =>
        YtWrapper.tableType(attributes) match {
          case TableType.Static => lockStaticTable(path, attributes)
          case TableType.Dynamic =>
            if (!isDriver) throw new IllegalStateException("Listing dynamic tables on executors is not supported")
            lockDynamicTable(path, attributes)
        }
      case PathType.Directory => listYtDirectory(path)
      case pathType => throw new IllegalArgumentException(s"Can't list $pathType")
    }
  }

  def listYtDirectory(path: YPathEnriched)(implicit yt: CompoundClient): Array[FileStatus] = {
    YtWrapper.listDir(path.toYPath, path.transaction).map { name => getFileStatus(path.child(name)) }
  }

  private lazy val isDriver: Boolean = {
    SparkSession.getDefaultSession.nonEmpty
  }

  private def lockFile(path: YPathEnriched)
                      (implicit yt: CompoundClient): Array[FileStatus] = {
    Array(getFileStatus(path.lock()))
  }

  @tailrec
  private def lockStaticTable(path: YPathEnriched,
                              attributes: Map[String, YTreeNode], useYtPartitioning: Boolean = false)
                             (implicit yt: CompoundClient): Array[FileStatus] = {
    path match {
      case p: YtObjectPath =>
        val newAttributes = YtWrapper.attributes(p.toYPath)
        listStaticTableAsFiles(p, newAttributes, useYtPartitioning)
      case p@(_: YtSimplePath | _: YtRootPath) => listStaticTableAsFiles(p, attributes, useYtPartitioning)
      case p: YtTransactionPath => Array(getFileStatus(p.lock()))
      case p: YtTimestampPath => lockStaticTable(p.parent, attributes, useYtPartitioning)
      case p: YtPartitionedPath => lockStaticTable(p.parent, attributes, useYtPartitioning = true)
    }
  }

  private def lockDynamicTable(path: YPathEnriched,
                               attributes: Map[String, YTreeNode], useYtPartitioning: Boolean = false)
                              (implicit yt: CompoundClient): Array[FileStatus] = {
    path match {
      case p: YtDynamicVersionPath =>
        p.parent match {
          case pp: YtTransactionPath =>
            Array(getFileStatus(p.lock()))
          case _ =>
            listDynamicTableAsFiles(p, YtWrapper.attributes(p.toYPath), useYtPartitioning)
        }
      case p@(_: YtSimplePath | _: YtObjectPath | _: YtRootPath) =>
        val ts = YtWrapper.maxAvailableTimestamp(p.toYPath)
        Array(getFileStatus(p.withTimestamp(ts)))
      case p: YtTransactionPath =>
        Array(getFileStatus(p.lock()))
      case p: YtPartitionedPath =>
        lockDynamicTable(p.parent, attributes, useYtPartitioning = true)
    }
  }


  private def listStaticTableAsFiles(f: YPathEnriched, attributes: Map[String, YTreeNode],
                                     useYtPartitioning: Boolean = false)
                                    (implicit yt: CompoundClient): Array[FileStatus] = {
    val rowCount = YtWrapper.rowCount(attributes)
    val optimizeMode = YtWrapper.optimizeMode(attributes)
    val tableSize = YtWrapper.dataWeight(attributes)
    val approximateRowSize = if (rowCount > 0) tableSize / rowCount else 0
    val modificationTime = YtWrapper.modificationTimeTs(attributes)
    if (useYtPartitioning) {
      // No real partitioning. YT partitioning must be used further.
      val chunkPath = YtStaticPath(f, YtStaticPathAttributes(optimizeMode, 0, rowCount))
      val result = new Array[FileStatus](1)
      result(0) = new YtFileStatus(chunkPath, approximateRowSize, modificationTime)
      result
    } else {
      val chunkCount = YtWrapper.chunkCount(attributes)

      val filesCount = if (chunkCount > 0) chunkCount else 1
      val result = new Array[FileStatus](filesCount)
      for (chunkIndex <- 0 until chunkCount) {
        val chunkStart = chunkIndex * rowCount / chunkCount
        val chunkRowCount = (chunkIndex + 1) * rowCount / chunkCount - chunkStart
        val chunkPath = YtStaticPath(f, YtStaticPathAttributes(optimizeMode, chunkStart, chunkRowCount))
        result(chunkIndex) = new YtFileStatus(chunkPath, approximateRowSize, modificationTime)
      }

      if (chunkCount == 0) {
        // Add path for schema resolving.
        val chunkPath = YtStaticPath(f, YtStaticPathAttributes(optimizeMode, 0, 0))
        result(0) = new YtFileStatus(chunkPath, approximateRowSize, modificationTime)
      }
      result
    }
  }

  private def listDynamicTableAsFiles(f: YPathEnriched,
                                      attributes: Map[String, YTreeNode], useYtPartitioning: Boolean = false)
                                     (implicit yt: CompoundClient): Array[FileStatus] = {
    val keyColumns = YtWrapper.keyColumns(attributes)
    val tableSize = YtWrapper.dataWeight(attributes)
    val modificationTime = YtWrapper.modificationTimeTs(attributes)
    if (useYtPartitioning) {
      // No real partitioning. YT partitioning must be used further.
      val result = new Array[FileStatus](1)
      val approximateChunkSize = tableSize

      val chunkPath = YtDynamicPath(f, YtWrapper.emptyPivotKey, YtWrapper.emptyPivotKey, 0.toString, keyColumns)
      result(0) = new YtFileStatus(chunkPath, approximateChunkSize, modificationTime)
      result
    } else {
      val pivotKeys = YtWrapper.pivotKeys(f.toYPath) :+ YtWrapper.emptyPivotKey
      val result = new Array[FileStatus](pivotKeys.length - 1)
      val approximateChunkSize = if (result.length > 0) tableSize / result.length else 0

      pivotKeys.sliding(2).zipWithIndex.foreach {
        case (Seq(startKey, endKey), i) =>
          val chunkPath = YtDynamicPath(f, startKey, endKey, i.toString, keyColumns)
          result(i) = new YtFileStatus(chunkPath, approximateChunkSize, modificationTime)
      }
      result
    }
  }

  override def getFileStatus(f: Path): FileStatus = {
    getFileStatus(f, ypath(f))
  }

  def getFileStatus(path: YPathEnriched): FileStatus = {
    getFileStatus(path.toPath, path)
  }

  def getFileStatus(f: Path, path: YPathEnriched): FileStatus = {
    log.debugLazy(s"Get file status $f")
    implicit val ytClient: CompoundClient = yt

    if (!YtWrapper.exists(path.toYPath, path.transaction)) {
      throw new FileNotFoundException(s"File $path is not found")
    } else {
      val pathType = YtWrapper.pathType(path.toYPath, path.transaction)
      pathType match {
        case PathType.Table => new FileStatus(YtWrapper.fileSize(path.toYPath, path.transaction), true, 1, 0, 0, f)
        case PathType.File => new FileStatus(YtWrapper.fileSize(path.toYPath, path.transaction), false, 1, 0, 0, f)
        case PathType.Directory => new FileStatus(0, true, 1, 0, 0, f)
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
