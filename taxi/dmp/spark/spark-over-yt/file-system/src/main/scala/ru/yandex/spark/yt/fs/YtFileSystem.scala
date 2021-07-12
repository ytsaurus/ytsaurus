package ru.yandex.spark.yt.fs

import java.io.FileNotFoundException
import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable
import org.slf4j.LoggerFactory
import ru.yandex.spark.yt.fs.PathUtils.hadoopPathToYt
import ru.yandex.spark.yt.wrapper.YtWrapper
import ru.yandex.spark.yt.wrapper.cypress.PathType
import ru.yandex.yt.ytclient.proxy.CompoundClient

import scala.language.postfixOps

@SerialVersionUID(1L)
class YtFileSystem extends YtFileSystemBase {
  private val log = LoggerFactory.getLogger(getClass)

  override def listStatus(f: Path): Array[FileStatus] = {
    log.debugLazy(s"List status $f")
    implicit val ytClient: CompoundClient = yt
    val path = hadoopPathToYt(f)

    if (!YtWrapper.exists(path)) {
      throw new PathNotFoundException(s"Path $f doesn't exist")
    } else {
      val pathType = YtWrapper.pathType(path)

      pathType match {
        case PathType.File => Array(getFileStatus(f))
        case PathType.Directory => listYtDirectory(f, path, None)
        case _ => throw new IllegalArgumentException(s"Can't list $pathType")
      }
    }
  }

  override def getFileStatus(f: Path): FileStatus = {
    log.debugLazy(s"Get file status $f")
    implicit val ytClient: CompoundClient = yt
    val path = hadoopPathToYt(f)

    if (!YtWrapper.exists(path)) {
      throw new FileNotFoundException(s"File $path is not found")
    } else {
      val pathType = YtWrapper.pathType(path)
      pathType match {
        case PathType.File => new FileStatus(
          YtWrapper.fileSize(path), false, 1, 0, YtWrapper.modificationTimeTs(path), f
        )
        case PathType.Directory => new FileStatus(0, true, 1, 0, 0, f)
        case PathType.None => null
      }
    }
  }

  override def create(f: Path, permission: FsPermission, overwrite: Boolean, bufferSize: Int,
                      replication: Short, blockSize: Long, progress: Progressable): FSDataOutputStream = {
    create(f, permission, overwrite, bufferSize, replication, blockSize, progress, statistics)
  }

  override def mkdirs(f: Path, permission: FsPermission): Boolean = {
    implicit val ytClient: CompoundClient = yt
    val path = hadoopPathToYt(f)
    YtWrapper.createDir(path, ignoreExisting = true)
    true
  }
}
