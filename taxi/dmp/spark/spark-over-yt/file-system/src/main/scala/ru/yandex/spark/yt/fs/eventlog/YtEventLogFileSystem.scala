package ru.yandex.spark.yt.fs.eventlog

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable
import org.slf4j.LoggerFactory
import ru.yandex.spark.yt.fs.PathUtils.{getMetaPath, hadoopPathToYt}
import ru.yandex.spark.yt.fs.YtClientConfigurationConverter.ytClientConfiguration
import ru.yandex.spark.yt.wrapper.{LogLazy, YtWrapper}
import ru.yandex.spark.yt.wrapper.client.{YtClientConfiguration, YtClientProvider, YtRpcClient}
import ru.yandex.spark.yt.wrapper.cypress.PathType
import ru.yandex.spark.yt.wrapper.model.EventLogSchema.Key._
import ru.yandex.spark.yt.wrapper.model.EventLogSchema._
import ru.yandex.yt.ytclient.proxy.{ApiServiceTransaction, CompoundClient}

import java.io.FileNotFoundException
import java.net.URI
import java.time.Clock
import java.util
import java.util.UUID
import scala.util.{Failure, Success, Try}

class YtEventLogFileSystem extends FileSystem with LogLazy {
  val id: String = UUID.randomUUID().toString

  private val log = LoggerFactory.getLogger(getClass)

  private var _uri: URI = _
  private var _workingDirectory: Path = new Path("/")
  protected var _ytConf: YtClientConfiguration = _
  protected lazy val yt: CompoundClient = YtClientProvider.ytClient(_ytConf, id)

  private var clock = Clock.systemUTC()

  private[eventlog] def setClock(clock: Clock): Unit = {
    this.clock = clock
  }

  override def initialize(uri: URI, conf: Configuration): Unit = {
    super.initialize(uri, conf)
    setConf(conf)
    this._uri = uri
    this._ytConf = ytClientConfiguration(getConf, Option(uri.getAuthority).filter(_.nonEmpty))
  }

  override def create(f: Path, permission: FsPermission, overwrite: Boolean, bufferSize: Int,
                      replication: Short, blockSize: Long, progress: Progressable): FSDataOutputStream = {
    val (tablePath, fullTableName) = splitTablePath(f)
    val ytTablePath = hadoopPathToYt(tablePath)

    if (!overwrite && exists(f)) {
      throw new FileAlreadyExistsException()
    }

    YtWrapper.createDir(hadoopPathToYt(tablePath.getParent), None, ignoreExisting = true)(yt)

    def createFile(ytRpcClient: Option[YtRpcClient], ytClient: CompoundClient): FSDataOutputStream = {
      statistics.incrementWriteOps(1)
      new FSDataOutputStream(new YtEventLogFsOutputStream(getConf, ytTablePath, fullTableName, clock, ytClient), statistics)
    }

    val oldDetails = getFileDetailsImpl(ytTablePath, fullTableName)
    val out = createFile(None, yt)

    oldDetails match {
      case Some(v) =>
        YtWrapper.runWithRetry(transaction => {
          deleteAllRowsWithId(ytTablePath, v.id, v.meta.blocksCnt, Some(transaction))
        })(yt)
      case _ =>
    }

    out
  }

  def splitTablePath(f: Path): (Path, String) = {
    (f.getParent, f.getName)
  }

  private def isCreatedAndMounted(f: Path): Boolean = {
    YtWrapper.exists(hadoopPathToYt(f))(yt) && YtWrapper.tabletState(hadoopPathToYt(f))(yt) == YtWrapper.TabletState.Mounted
  }

  override def exists(f: Path): Boolean = {
    log.debugLazy(s"Exists $f")
    getFileStatusEither(f).toOption.exists(_ != null)
  }

  def existsTable(f: Path): Boolean = {
    isCreatedAndMounted(f) && isCreatedAndMounted(new Path(getMetaPath(f)))
  }

  override def getUri: URI = _uri

  override def open(f: Path, bufferSize: Int): FSDataInputStream = {
    log.debugLazy(s"Open $f")
    val (tablePath, fullTableName) = splitTablePath(f)
    getFileDetailsImpl(hadoopPathToYt(tablePath), fullTableName) match {
      case None => throw new IllegalArgumentException("No such file found")
      case Some(details) => new FSDataInputStream(new YtEventLogFsInputStream(getConf, hadoopPathToYt(tablePath), details, yt))
    }
  }

  override def append(f: Path, bufferSize: Int, progress: Progressable): FSDataOutputStream = ???

  override def rename(src: Path, dst: Path): Boolean = {
    implicit val ytClient: CompoundClient = yt

    val (srcTablePath, srcName) = splitTablePath(src)
    val srcMetaTablePath = getMetaPath(srcTablePath)
    val (dstTablePath, dstName) = splitTablePath(dst)
    val dstMetaTablePath = getMetaPath(dstTablePath)
    if (srcTablePath == dstTablePath) {
      YtWrapper.runWithRetry(transaction => {
        getFileDetailsImpl(hadoopPathToYt(srcTablePath), srcName, Some(transaction)).exists {
          details => {
            YtWrapper.deleteRow(hadoopPathToYt(srcMetaTablePath), metaSchema,
              util.Map.of(FILENAME, srcName), Some(transaction))
            YtWrapper.insertRows(hadoopPathToYt(dstMetaTablePath), metaSchema,
              List(details.copy(fileName = dstName).toList), Some(transaction))
            true
          }
        }
      })
    } else {
      throw new IllegalArgumentException("Renaming doesn't support different parent tables")
    }
  }

  private def deleteAllRowsWithId(path: String, id: String, blocksCnt: Int, transaction: Option[ApiServiceTransaction]): Unit = {
    implicit val ytClient: CompoundClient = yt
    for (i <- 1 to blocksCnt) {
      YtWrapper.deleteRow(path, schema,
        java.util.Map.of(ID, id, ORDER, i), transaction)
    }
  }

  override def delete(f: Path, recursive: Boolean): Boolean = {
    log.debugLazy(s"Delete $f")
    implicit val ytClient: CompoundClient = yt

    val (tablePath, fullTableName) = splitTablePath(f)
    val tablePathStr = hadoopPathToYt(tablePath)
    val meta_path = getMetaPath(tablePathStr)
    YtWrapper.runWithRetry(transaction => {
      getFileDetailsImpl(tablePathStr, fullTableName, Some(transaction)).exists(details => {
        YtWrapper.deleteRow(meta_path, metaSchema, java.util.Map.of(FILENAME, fullTableName), Some(transaction))
        deleteAllRowsWithId(tablePathStr, fullTableName, details.meta.blocksCnt, Some(transaction))
        true
      })
    })
  }

  override def listStatus(f: Path): Array[FileStatus] = {
    log.debugLazy(s"List status $f")
    val meta_path = getMetaPath(hadoopPathToYt(f))
    implicit val ytClient: CompoundClient = yt

    val pathType = YtWrapper.pathType(hadoopPathToYt(f), None)
    pathType match {
      case PathType.Table =>
        if (!existsTable(f)) {
          throw new IllegalArgumentException(s"Corrupted table found at $f")
        }
        val rows = YtWrapper.selectRows(meta_path, metaSchema, None)
        rows.map(YtEventLogFileDetails.apply).map {
          details => {
            new FileStatus(
              details.meta.length, false, 1, 0,
              details.meta.modificationTs, new Path(f, details.fileName))
          }
        }.toArray
      case _ => throw new IllegalArgumentException(s"Can't visit $f")
    }
  }

  override def setWorkingDirectory(new_dir: Path): Unit = {
    _workingDirectory = new_dir
  }

  override def getWorkingDirectory: Path = _workingDirectory

  override def mkdirs(f: Path, permission: FsPermission): Boolean = {
    implicit val ytClient: CompoundClient = yt
    YtWrapper.createDir(hadoopPathToYt(f.getParent), ignoreExisting = true)
    val path = hadoopPathToYt(f)
    YtWrapper.createDynTableAndMount(path, schema)
    YtWrapper.createDynTableAndMount(getMetaPath(path), metaSchema)
    true
  }

  def getFileDetailsImpl(path: String,
                         fileName: String,
                         transaction: Option[ApiServiceTransaction] = None): Option[YtEventLogFileDetails] = {
    log.debugLazy(s"Get details $path, $fileName")
    implicit val ytClient: CompoundClient = yt
    val meta_path = getMetaPath(path)
    if (!YtWrapper.exists(meta_path)) {
      None
    } else {
      val selectedRows = YtWrapper.selectRows(meta_path, metaSchema,
        Some(s"""${FILENAME}="$fileName""""), transaction)
      selectedRows match {
        case Nil => None
        case meta :: Nil => Some(YtEventLogFileDetails(meta))
        case _ => throw new RuntimeException(s"Meta table $meta_path has a few rows with file_name=$fileName")
      }
    }
  }

  private def getFileStatusEither(f: Path): Try[FileStatus] = Try {
    implicit val ytClient: CompoundClient = yt

    val (tablePath, fullTableName) = splitTablePath(f)
    val tablePathStr = hadoopPathToYt(tablePath)

    if (!YtWrapper.exists(tablePathStr)) {
      throw new FileNotFoundException(s"Path $tablePathStr doesn't exist")
    } else {
      val parentPathType = YtWrapper.pathType(tablePathStr, None)
      parentPathType match {
        case PathType.Table =>
          if (!existsTable(tablePath)) {
            throw new FileNotFoundException(s"Corrupted table found at $f")
          } else {
            getFileDetailsImpl(tablePathStr, fullTableName) match {
              case Some(details) =>
                new FileStatus(
                  details.meta.length, false, 1, 0, details.meta.modificationTs, f
                )
              case _ => throw new FileNotFoundException(s"File $fullTableName doesn't exist in $tablePathStr")
            }
          }
        case PathType.Directory =>
          val fStr = hadoopPathToYt(f)
          if (!YtWrapper.exists(fStr)) {
            throw new FileNotFoundException(s"Path $fStr doesn't exist")
          } else {
            val pathType = YtWrapper.pathType(fStr, None)
            pathType match {
              case PathType.Table =>
                new FileStatus(0, true, 1, 0, YtWrapper.modificationTimeTs(fStr), f)
              case _ => null
            }
          }
        case _ => null
      }
    }
  }

  override def getFileStatus(f: Path): FileStatus = {
    log.debugLazy(s"Get file status $f")

    val res = getFileStatusEither(f)
    res match {
      case Failure(e) => throw e
      case Success(v) => v
    }
  }
}
