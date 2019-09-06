package ru.yandex.spark.yt.file

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, FileStatus, FileSystem, Path}
import org.apache.hadoop.util.Progressable
import ru.yandex.spark.yt.YtClientProvider

class YtFileSystem extends FileSystem {
  private var _uri: URI = _
  private var _workingDirectory: Path = new Path("/")


  override def initialize(uri: URI, conf: Configuration): Unit = {
    super.initialize(uri, conf)
    val yt = YtClientProvider.ytClient(conf)
    _uri = uri
  }

  override def getUri: URI = _uri

  override def open(f: Path, bufferSize: Int): FSDataInputStream = ???

  override def create(f: Path, permission: FsPermission, overwrite: Boolean, bufferSize: Int, replication: Short, blockSize: Long, progress: Progressable): FSDataOutputStream = ???

  override def append(f: Path, bufferSize: Int, progress: Progressable): FSDataOutputStream = ???

  override def rename(src: Path, dst: Path): Boolean = ???

  override def delete(f: Path, recursive: Boolean): Boolean = ???

  override def listStatus(f: Path): Array[FileStatus] = {
    Array(getFileStatus(f))
  }

  override def setWorkingDirectory(new_dir: Path): Unit = {
    _workingDirectory = new_dir
  }

  override def getWorkingDirectory: Path = _workingDirectory

  override def mkdirs(f: Path, permission: FsPermission): Boolean = ???

  override def getFileStatus(f: Path): FileStatus = {
    val yt = YtClientProvider.ytClient
    val rowCount = yt.getNode(s"/${f.toUri.getPath}/@row_count").join().longValue()
    new FileStatus(rowCount, false, 1, rowCount, 0, f)
  }
}
