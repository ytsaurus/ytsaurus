package ru.yandex.spark.yt.format

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable
import ru.yandex.spark.yt.YtTableUtils._
import ru.yandex.spark.yt.{YtClientConfigurationConverter, YtClientProvider, YtTableUtils}
import ru.yandex.yt.ytclient.proxy.YtClient

class YtFileSystem extends FileSystem {
  private var _uri: URI = _
  private var _workingDirectory: Path = new Path("/")


  override def initialize(uri: URI, conf: Configuration): Unit = {
    super.initialize(uri, conf)
    val yt = YtClientProvider.ytClient(YtClientConfigurationConverter(conf))
    _uri = uri
  }

  override def getUri: URI = _uri

  override def open(f: Path, bufferSize: Int): FSDataInputStream = ???

  override def create(f: Path, permission: FsPermission, overwrite: Boolean, bufferSize: Int,
                      replication: Short, blockSize: Long, progress: Progressable): FSDataOutputStream = ???

  override def append(f: Path, bufferSize: Int, progress: Progressable): FSDataOutputStream = ???

  override def rename(src: Path, dst: Path): Boolean = ???

  override def delete(f: Path, recursive: Boolean): Boolean = {
    removeTable(ytPath(f))(YtClientProvider.ytClient)
    true
  }

  override def listStatus(f: Path): Array[FileStatus] = {
    implicit val yt: YtClient = YtClientProvider.ytClient
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

  override def mkdirs(f: Path, permission: FsPermission): Boolean = {
    //createTable(ytPath(f), )
    ???
  }

  override def getFileStatus(f: Path): FileStatus = {
    implicit val yt: YtClient = YtClientProvider.ytClient
    val path = ytPath(f)
    val transaction = GlobalTableOptions.getTransaction(path)

    f match {
      case yp: YtPath =>
        new FileStatus(yp.rowCount, false, 1, yp.rowCount, 0, yp)
      case _ =>
        if (!YtTableUtils.exists(path, transaction)) {
          null
        } else {
          new FileStatus(0, true, 1, 0, 0, f)
        }
    }
  }

  private def ytPath(f: Path): String = {
    f.toUri.getPath
  }
}
