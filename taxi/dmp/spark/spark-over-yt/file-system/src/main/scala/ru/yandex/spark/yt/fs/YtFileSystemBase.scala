package ru.yandex.spark.yt.fs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable
import org.slf4j.LoggerFactory
import ru.yandex.spark.yt.fs.PathUtils.hadoopPathToYt
import ru.yandex.spark.yt.fs.YtClientConfigurationConverter._
import ru.yandex.spark.yt.wrapper.client.{YtClientConfiguration, YtClientProvider, YtRpcClient}
import ru.yandex.spark.yt.wrapper.{LogLazy, YtWrapper}
import ru.yandex.yt.TError
import ru.yandex.yt.ytclient.proxy.CompoundClient
import ru.yandex.yt.ytclient.rpc.RpcError
import ru.yandex.yt.ytree.TAttributeDictionary

import java.io.FileNotFoundException
import java.net.URI
import java.util.UUID
import java.util.concurrent.CompletionException
import java.util.stream.Collectors
import scala.annotation.tailrec
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.concurrent.duration._
import scala.language.postfixOps

abstract class YtFileSystemBase extends FileSystem with LogLazy {
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

  override def open(f: Path, bufferSize: Int): FSDataInputStream = convertExceptions {
    log.debugLazy(s"Open file ${f.toUri.toString}")
    statistics.incrementReadOps(1)
    new FSDataInputStream(new YtFsInputStream(YtWrapper.readFile(hadoopPathToYt(f), timeout = _ytConf.timeout)(yt),
      statistics))
  }

  protected def create(f: Path, permission: FsPermission, overwrite: Boolean, bufferSize: Int,
                       replication: Short, blockSize: Long, progress: Progressable,
                       statistics: FileSystem.Statistics): FSDataOutputStream = convertExceptions {
    log.debugLazy(s"Create new file: $f")
    statistics.incrementWriteOps(1)
    val path = hadoopPathToYt(f)

    YtWrapper.createDir(hadoopPathToYt(f.getParent), None, ignoreExisting = true)(yt)

    def createFile(ytRpcClient: Option[YtRpcClient], ytClient: CompoundClient): FSDataOutputStream = {
      YtWrapper.createFile(path, None, overwrite)(ytClient)
      statistics.incrementWriteOps(1)
      new FSDataOutputStream(YtWrapper.writeFile(path, 7 days, ytRpcClient, None)(ytClient), statistics)
    }

    if (_ytConf.extendedFileTimeout) {
      val ytConf = _ytConf.copy(timeout = 7 days)
      val ytRpcClient: YtRpcClient = YtClientProvider.ytRpcClient(ytConf, s"create-file-${UUID.randomUUID().toString}")
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

  override def append(f: Path, bufferSize: Int, progress: Progressable): FSDataOutputStream = convertExceptions {
    ???
  }

  override def rename(src: Path, dst: Path): Boolean = convertExceptions {
    log.debugLazy(s"Rename $src to $dst")
    statistics.incrementWriteOps(1)
    YtWrapper.rename(hadoopPathToYt(src), hadoopPathToYt(dst))(yt)
    true
  }

  override def mkdirs(f: Path, permission: FsPermission): Boolean = convertExceptions {
    log.debugLazy(s"Create $f")
    statistics.incrementWriteOps(1)
    YtWrapper.createDir(hadoopPathToYt(f), ignoreExisting = true)(yt)
    true
  }


  override def delete(f: Path, recursive: Boolean): Boolean = convertExceptions {
    log.debugLazy(s"Delete $f")
    statistics.incrementWriteOps(1)
    if (!YtWrapper.exists(hadoopPathToYt(f))(yt)) {
      log.debugLazy(s"$f is not exist")
      return false
    }

    YtWrapper.remove(hadoopPathToYt(f))(yt)
    true
  }

  def listYtDirectory(f: Path, path: String, transaction: Option[String])
                     (implicit yt: CompoundClient): Array[FileStatus] = convertExceptions {
    statistics.incrementReadOps(1)
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

  def internalStatistics: FileSystem.Statistics = this.statistics

  def convertExceptions[T](f: => T): T = {
    try {
      f
    } catch {
      case FileNotFound(ex) => throw ex
    }
  }


  private object FileNotFound {
    private val ERR_PATH_PFX = "Error resolving path "
    private val ERR_CODE = 500

    def findFileNotFound(e: TError): Option[String] = {
      if (e.getCode == ERR_CODE && e.getMessage.startsWith(ERR_PATH_PFX)) {
        Some(e.getMessage.substring(ERR_PATH_PFX.length))
      } else if (e.getInnerErrorsList.isEmpty) {
        None
      } else {
        e.getInnerErrorsList.toSeq.flatMap(findFileNotFound).headOption
      }
    }

    @tailrec
    def unapply(ex: Throwable): Option[FileNotFoundException] = ex match {
      case err: CompletionException if err.getCause != null =>
        unapply(err.getCause)
      case err: RpcError =>
        findFileNotFound(err.getError).foreach(file => {
          throw new FileNotFoundException(file) {
            override def getCause: Throwable = err
          }
        })
        throw err
      case unknown: Throwable =>
        throw unknown
    }
  }
}
