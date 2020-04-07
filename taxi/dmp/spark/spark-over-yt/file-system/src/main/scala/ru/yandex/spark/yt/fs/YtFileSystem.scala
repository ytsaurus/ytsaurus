package ru.yandex.spark.yt.fs

import java.io.FileNotFoundException
import java.net.URI
import java.util.UUID

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable
import org.apache.log4j.Logger
import ru.yandex.spark.yt.utils._
import ru.yandex.yt.ytclient.proxy.YtClient

import scala.concurrent.duration._
import scala.language.postfixOps

class YtFileSystem extends FileSystem {
  val id: String = UUID.randomUUID().toString

  private val log = Logger.getLogger(getClass)

  private var _uri: URI = _
  private var _workingDirectory: Path = new Path("/")
  private var _conf: Configuration = _
  private var _ytConf: YtClientConfiguration = _

  override def initialize(uri: URI, conf: Configuration): Unit = {
    super.initialize(uri, conf)
    this._conf = conf
    this._uri = uri
    this._ytConf = YtClientConfigurationConverter(_conf)
  }

  def yt: YtClient = YtClientProvider.ytClient(_ytConf, id)

  override def getUri: URI = _uri

  override def open(f: Path, bufferSize: Int): FSDataInputStream = {
    log.debugLazy(s"Open file ${f.toUri.toString}")
    new FSDataInputStream(new YtFsInputStream(YtTableUtils.readFile(ytPath(f), timeout = _ytConf.timeout)(yt)))
  }

  override def create(f: Path, permission: FsPermission, overwrite: Boolean, bufferSize: Int,
                      replication: Short, blockSize: Long, progress: Progressable): FSDataOutputStream = {
    log.debugLazy(s"Create new file: $f")
    val ytConf = _ytConf.copy(timeout = 7 days)
    val ytRpcClient: YtRpcClient = YtClientUtils.createRpcClient(ytConf)
    try {
      val path = ytPath(f)
      val transaction = GlobalTableSettings.getTransaction(path)
      YtTableUtils.createFile(path, transaction)(ytRpcClient.yt)
      statistics.incrementWriteOps(1)
      new FSDataOutputStream(YtTableUtils.writeToFile(path, 7 days, ytRpcClient, transaction), statistics)
    } catch {
      case e: Throwable =>
        ytRpcClient.close()
        throw e
    }
  }

  override def append(f: Path, bufferSize: Int, progress: Progressable): FSDataOutputStream = ???

  override def rename(src: Path, dst: Path): Boolean = {
    log.debugLazy(s"Rename $src to $dst")
    val transaction = GlobalTableSettings.getTransaction(ytPath(src))
    YtTableUtils.rename(ytPath(src), ytPath(dst), transaction)(yt)
    true
  }

  override def delete(f: Path, recursive: Boolean): Boolean = {
    log.debugLazy(s"Delete $f")
    YtTableUtils.remove(ytPath(f))(yt)
    true
  }

  override def listStatus(f: Path): Array[FileStatus] = {
    log.debugLazy(s"List status $f")
    implicit val ytClient: YtClient = yt
    val path = ytPath(f)

    val transaction = GlobalTableSettings.getTransaction(path)
    val pathType = YtTableUtils.getType(path, transaction)

    pathType match {
      case PathType.Table => listTableAsFiles(f, path, transaction)
      case PathType.Directory => listYtDirectory(f, path, transaction)
      case PathType.File => Array(getFileStatus(f))
      case _ => throw new IllegalArgumentException(s"Can't list $pathType")
    }
  }

  private def listYtDirectory(f: Path, path: String, transaction: Option[String])
                             (implicit yt: YtClient): Array[FileStatus] = {
    YtTableUtils.listDirectory(path, transaction).map(name => getFileStatus(new Path(f, name)))
  }

  private def listTableAsFiles(f: Path, path: String, transaction: Option[String])
                              (implicit yt: YtClient): Array[FileStatus] = {
    val rowCount = YtTableUtils.tableAttribute(path, "row_count", transaction).longValue()
    val chunksCount = GlobalTableSettings.getFilesCount(path).getOrElse(
      YtTableUtils.tableAttribute(path, "chunk_count", transaction).longValue().toInt
    )
    GlobalTableSettings.removeFilesCount(path)
    val filesCount = if (chunksCount > 0) chunksCount else 1
    val result = new Array[FileStatus](filesCount)
    for (chunkIndex <- 0 until chunksCount) {
      val chunkStart = chunkIndex * rowCount / chunksCount
      val chunkRowCount = (chunkIndex + 1) * rowCount / chunksCount - chunkStart
      val chunkPath = new YtPath(f, chunkStart, chunkRowCount)
      result(chunkIndex) = new YtFileStatus(chunkPath, rowCount, chunksCount)
    }
    if (chunksCount == 0) {
      // add path for schema resolving
      val chunkPath = new YtPath(f, 0, 0)
      result(0) = new YtFileStatus(chunkPath, rowCount, chunksCount)
    }
    result
  }

  override def setWorkingDirectory(new_dir: Path): Unit = {
    _workingDirectory = new_dir
  }

  override def getWorkingDirectory: Path = _workingDirectory

  override def mkdirs(f: Path, permission: FsPermission): Boolean = ???

  override def getFileStatus(f: Path): FileStatus = {
    log.debugLazy(s"Get file status $f")
    implicit val ytClient: YtClient = yt
    val path = ytPath(f)
    val transaction = GlobalTableSettings.getTransaction(path)

    f match {
      case yp: YtPath =>
        new FileStatus(yp.rowCount, false, 1, yp.rowCount, 0, yp)
      case _ =>
        if (!YtTableUtils.exists(path, transaction)) {
          throw new FileNotFoundException(s"File $path is not found")
        } else {
          val pathType = YtTableUtils.getType(path, transaction)
          pathType match {
            case PathType.Table => new FileStatus(0, true, 1, 0, 0, f)
            case PathType.File => new FileStatus(YtTableUtils.fileSize(path, transaction), false, 1, 0, 0, f)
            case PathType.Directory => new FileStatus(0, true, 1, 0, 0, f)
            case PathType.None => null
          }
        }
    }
  }

  private def ytPath(f: Path): String = {
    f.toUri.getPath
  }

  override def close(): Unit = {
    log.info("Close YtFileSystem")
    YtClientProvider.close(id)
    super.close()
  }
}
