package ru.yandex.spark.yt.format

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable
import org.apache.log4j.Logger
import ru.yandex.spark.yt.YtTableUtils._
import ru.yandex.spark.yt.{YtClientConfigurationConverter, YtClientProvider, YtTableUtils}
import ru.yandex.yt.ytclient.proxy.YtClient

class YtFileSystem extends FileSystem {
  private val log = Logger.getLogger(getClass)
  private var _uri: URI = _
  private var _workingDirectory: Path = new Path("/")
  private var yt: YtClient = _

  override def initialize(uri: URI, conf: Configuration): Unit = {
    super.initialize(uri, conf)
    yt = YtClientProvider.ytClient(YtClientConfigurationConverter(conf))
    _uri = uri
  }

  override def getUri: URI = _uri

  override def open(f: Path, bufferSize: Int): FSDataInputStream =
    YtTableUtils.downloadFile(ytPath(f))(yt)

  override def create(f: Path, permission: FsPermission, overwrite: Boolean, bufferSize: Int,
                      replication: Short, blockSize: Long, progress: Progressable): FSDataOutputStream = ???

  override def append(f: Path, bufferSize: Int, progress: Progressable): FSDataOutputStream = ???

  override def rename(src: Path, dst: Path): Boolean = ???

  override def delete(f: Path, recursive: Boolean): Boolean = {
    removeTable(ytPath(f))(yt)
    true
  }

  override def listStatus(f: Path): Array[FileStatus] = {
    implicit val ytClient: YtClient = yt
    val path = ytPath(f)
    val transaction = GlobalTableOptions.getTransaction(path)
    val rowCount = YtTableUtils.tableAttribute(path, "row_count", transaction).longValue()
    val chunksCount = GlobalTableOptions.getFilesCount(path).getOrElse(
      YtTableUtils.tableAttribute(path, "chunk_count", transaction).longValue().toInt
    )
    GlobalTableOptions.removeFilesCount(path)
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
    val transaction = GlobalTableOptions.getTransaction(path)

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
            case PathType.File => new FileStatus(0, false, 1, 0, 0, f)
            case PathType.None => null
          }
        }
    }
  }

  private def ytPath(f: Path): String = {
    f.toUri.getPath
  }
}
