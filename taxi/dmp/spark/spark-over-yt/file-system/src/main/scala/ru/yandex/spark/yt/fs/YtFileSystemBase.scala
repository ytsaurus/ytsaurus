package ru.yandex.spark.yt.fs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable
import org.slf4j.LoggerFactory
import ru.yandex.spark.yt.fs.PathUtils.hadoopPathToYt
import ru.yandex.spark.yt.fs.YtClientConfigurationConverter._
import ru.yandex.spark.yt.wrapper.client.{YtClientConfiguration, YtRpcClient}
import ru.yandex.spark.yt.wrapper.{LogLazy, YtWrapper}
import ru.yandex.yt.ytclient.proxy.CompoundClient

import java.net.URI
import java.util.UUID
import scala.concurrent.duration._
import scala.language.postfixOps

trait YtFileSystemBase extends FileSystem with LogLazy {
  val id: String = UUID.randomUUID().toString

  private val log = LoggerFactory.getLogger(getClass)

  private var _uri: URI = _
  private var _workingDirectory: Path = new Path("/")
  protected var _ytConf: YtClientConfiguration = _
  protected lazy val yt: CompoundClient = YtClientProvider.ytClient(_ytConf, id)

  override def initialize(uri: URI, conf: Configuration): Unit = {
    super.initialize(uri, conf)
    setConf(conf)
    this._uri = uri
    this._ytConf = ytClientConfiguration(getConf, Option(uri.getAuthority).filter(_.nonEmpty))
  }

  override def getUri: URI = _uri

  override def open(f: Path, bufferSize: Int): FSDataInputStream = {
    log.debugLazy(s"Open file ${f.toUri.toString}")
    new FSDataInputStream(new YtFsInputStream(YtWrapper.readFile(hadoopPathToYt(f), timeout = _ytConf.timeout)(yt)))
  }

  protected def create(f: Path, permission: FsPermission, overwrite: Boolean, bufferSize: Int,
                       replication: Short, blockSize: Long, progress: Progressable,
                       statistics: FileSystem.Statistics): FSDataOutputStream = {
    log.debugLazy(s"Create new file: $f")
    val path = hadoopPathToYt(f)

    YtWrapper.createDir(hadoopPathToYt(f.getParent), None, ignoreExisting = true)(yt)

    def createFile(ytRpcClient: Option[YtRpcClient], ytClient: CompoundClient): FSDataOutputStream = {
      YtWrapper.createFile(path, None, overwrite)(ytClient)
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
    YtWrapper.rename(hadoopPathToYt(src), hadoopPathToYt(dst))(yt)
    true
  }

  override def mkdirs(f: Path, permission: FsPermission): Boolean = {
    log.debugLazy(s"Create $f")
    YtWrapper.createDir(hadoopPathToYt(f), ignoreExisting = true)(yt)
    true
  }


  override def delete(f: Path, recursive: Boolean): Boolean = {
    log.debugLazy(s"Delete $f")
    if (!YtWrapper.exists(hadoopPathToYt(f))(yt)) {
      log.debugLazy(s"$f is not exist")
      return false
    }

    YtWrapper.remove(hadoopPathToYt(f))(yt)
    true
  }

  def listYtDirectory(f: Path, path: String, transaction: Option[String])
                     (implicit yt: CompoundClient): Array[FileStatus] = {
    YtWrapper.listDir(path, transaction).map(name => getFileStatus(new Path(f, name)))
  }

  override def setWorkingDirectory(new_dir: Path): Unit = {
    _workingDirectory = new_dir
  }

  override def getWorkingDirectory: Path = _workingDirectory

  override def close(): Unit = {
    log.info("Close YtFileSystem")
    YtClientProvider.close(id)
    super.close()
  }

}
