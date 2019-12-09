package ru.yandex.spark.yt.fs

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable
import org.apache.log4j.Logger
import ru.yandex.spark.yt.utils.{PathType, YtTableUtils}
import ru.yandex.yt.ytclient.proxy.YtClient

class YtFileSystem extends FileSystem {
  private val log = Logger.getLogger(getClass)

  private var _uri: URI = _
  private var _workingDirectory: Path = new Path("/")
  private var _conf: Configuration = _

  override def initialize(uri: URI, conf: Configuration): Unit = {
    super.initialize(uri, conf)
    this._conf = conf
    _uri = uri
  }

  def yt: YtClient = YtClientProvider.ytClient(YtClientConfigurationConverter(_conf))

  override def getUri: URI = _uri

  override def open(f: Path, bufferSize: Int): FSDataInputStream = {
    log.info(s"Open file ${f.toUri.toString}")
    new FSDataInputStream(new YtFsInputStream(YtTableUtils.readFile(ytPath(f))(yt)))
  }

  override def create(f: Path, permission: FsPermission, overwrite: Boolean, bufferSize: Int,
                      replication: Short, blockSize: Long, progress: Progressable): FSDataOutputStream = {
    implicit val ytClient: YtClient = yt
    val path = ytPath(f)
    val transaction = GlobalTableSettings.getTransaction(path)
    YtTableUtils.createFile(path, transaction)
    statistics.incrementWriteOps(1)
    new FSDataOutputStream(YtTableUtils.writeToFile(path, java.time.Duration.ofDays(7), transaction), statistics)
  }

  override def append(f: Path, bufferSize: Int, progress: Progressable): FSDataOutputStream = ???

  override def rename(src: Path, dst: Path): Boolean = {
    val transaction = GlobalTableSettings.getTransaction(ytPath(src))
    YtTableUtils.rename(ytPath(src), ytPath(dst), transaction)(yt)
    true
  }

  override def delete(f: Path, recursive: Boolean): Boolean = {
    YtTableUtils.remove(ytPath(f))(yt)
    true
  }

  override def listStatus(f: Path): Array[FileStatus] = {
    implicit val ytClient: YtClient = yt
    val path = ytPath(f)

    val transaction = GlobalTableSettings.getTransaction(path)
    val pathType = YtTableUtils.getType(path, transaction)

    pathType match {
      case PathType.Table => listTableAsFiles(f, path, transaction)
      case PathType.Directory => listYtDirectory(f, path, transaction)
      case _ => throw new IllegalArgumentException(s"Can't list $pathType")
    }
  }

  private def listYtDirectory(f: Path, path: String, transaction: Option[String])
                             (implicit yt: YtClient): Array[FileStatus] = {
    YtTableUtils.listDirectory(path, transaction)
      .map(name => getFileStatus(new Path(f, name)))
  }

  private def listTableAsFiles(f: Path, path: String, transaction: Option[String])
                              (implicit yt: YtClient): Array[FileStatus] = {
    val rowCount = YtTableUtils.tableAttribute(path, "row_count", transaction).longValue()
    val chunksCount = GlobalTableSettings.getFilesCount(path).getOrElse(
      YtTableUtils.tableAttribute(path, "chunk_count", transaction).longValue().toInt
    )
    GlobalTableSettings.removeFilesCount(path)
    val result = new Array[FileStatus](chunksCount)
    for (chunkIndex <- 0 until chunksCount) {
      val chunkStart = chunkIndex * rowCount / chunksCount
      val chunkRowCount = (chunkIndex + 1) * rowCount / chunksCount - chunkStart
      val chunkPath = new YtPath(f, chunkStart, chunkRowCount)
      result(chunkIndex) = new YtFileStatus(chunkPath, rowCount, chunksCount)
    }
    result
  }

  override def setWorkingDirectory(new_dir: Path): Unit = {
    _workingDirectory = new_dir
  }

  override def getWorkingDirectory: Path = _workingDirectory

  override def mkdirs(f: Path, permission: FsPermission): Boolean = ???

  override def getFileStatus(f: Path): FileStatus = {
    implicit val ytClient: YtClient = yt
    val path = ytPath(f)
    val transaction = GlobalTableSettings.getTransaction(path)

    f match {
      case yp: YtPath =>
        new FileStatus(yp.rowCount, false, 1, yp.rowCount, 0, yp)
      case _ =>
        if (!YtTableUtils.exists(path, transaction)) {
          null
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
    log.info("YtFileSystem closed")
    super.close()
  }
}
