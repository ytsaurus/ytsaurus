package ru.yandex.spark.yt.fs

import java.net.URI
import java.util.UUID

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable
import org.apache.log4j.Logger
import ru.yandex.spark.yt.fs.YtClientConfigurationConverter._
import ru.yandex.spark.yt.wrapper.client.{YtClientConfiguration, YtRpcClient}
import ru.yandex.spark.yt.wrapper.{LogLazy, YtWrapper}
import ru.yandex.yt.ytclient.proxy.YtClient

import scala.concurrent.duration._
import scala.language.postfixOps
import scala.reflect.internal.util.Statistics

trait YtFileSystemBase extends FileSystem with LogLazy {
  val id: String = UUID.randomUUID().toString

  private val log = Logger.getLogger(getClass)

  private var _uri: URI = _
  private var _workingDirectory: Path = new Path("/")
  protected var _ytConf: YtClientConfiguration = _

  override def initialize(uri: URI, conf: Configuration): Unit = {
    super.initialize(uri, conf)
    setConf(conf)
    this._uri = uri
    this._ytConf = ytClientConfiguration(getConf, Option(uri.getAuthority).filter(_.nonEmpty))
  }

  lazy val yt: YtClient = YtClientProvider.ytClient(_ytConf, id)

  override def getUri: URI = _uri

  override def open(f: Path, bufferSize: Int): FSDataInputStream = {
    log.debugLazy(s"Open file ${f.toUri.toString}")
    new FSDataInputStream(new YtFsInputStream(YtWrapper.readFile(ytPath(f), timeout = _ytConf.timeout)(yt)))
  }

  protected def create(f: Path, permission: FsPermission, overwrite: Boolean, bufferSize: Int,
                      replication: Short, blockSize: Long, progress: Progressable,
                      statistics: FileSystem.Statistics): FSDataOutputStream = {
    log.debugLazy(s"Create new file: $f")
    val path = ytPath(f)

    def createFile(ytRpcClient: Option[YtRpcClient], ytClient: YtClient): FSDataOutputStream = {
      YtWrapper.createFile(path)(ytClient)
      statistics.incrementWriteOps(1)
      new FSDataOutputStream(YtWrapper.writeFile(path, 7 days, ytRpcClient, None)(ytClient), statistics)
    }

    if (_ytConf.extendedFileTimeout) {
      val ytConf = _ytConf.copy(timeout = 7 days)
      val ytRpcClient: YtRpcClient = YtWrapper.createRpcClient(s"create-file-${UUID.randomUUID().toString}", ytConf)
      try {
        createFile(Some(ytRpcClient), ytRpcClient.yt)
      } catch {
        case e: Throwable =>
          ytRpcClient.close()
          throw e
      }
    } else {
      createFile(None, yt)
    }
  }

  override def append(f: Path, bufferSize: Int, progress: Progressable): FSDataOutputStream = ???

  override def rename(src: Path, dst: Path): Boolean = {
    log.debugLazy(s"Rename $src to $dst")
    YtWrapper.rename(ytPath(src), ytPath(dst))(yt)
    true
  }

  override def mkdirs(f: Path, permission: FsPermission): Boolean = {
    log.debugLazy(s"Create $f")
    YtWrapper.createDir(ytPath(f), ignoreExisting = true)(yt)
    true
  }


  override def delete(f: Path, recursive: Boolean): Boolean = {
    log.debugLazy(s"Delete $f")
    YtWrapper.remove(ytPath(f))(yt)
    true
  }

  def listYtDirectory(f: Path, path: String, transaction: Option[String])
                     (implicit yt: YtClient): Array[FileStatus] = {
    YtWrapper.listDir(path, transaction).map(name => getFileStatus(new Path(f, name)))
  }

  override def setWorkingDirectory(new_dir: Path): Unit = {
    _workingDirectory = new_dir
  }

  override def getWorkingDirectory: Path = _workingDirectory

  def ytPath(f: Path): String = {
    f.toUri.getPath
  }

  override def close(): Unit = {
    log.info("Close YtFileSystem")
    YtClientProvider.close(id)
    super.close()
  }

}
