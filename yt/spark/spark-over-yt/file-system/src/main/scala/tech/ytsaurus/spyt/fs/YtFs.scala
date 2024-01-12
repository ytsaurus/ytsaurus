package tech.ytsaurus.spyt.fs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{DelegateToFileSystem, Path}
import org.slf4j.LoggerFactory
import tech.ytsaurus.spyt.fs.PathUtils.hadoopPathToYt
import tech.ytsaurus.spyt.wrapper.{LogLazy, YtWrapper}

import java.net.URI


class YtFs(uri: URI, conf: Configuration)
  extends DelegateToFileSystem(uri, new YtFileSystem, conf, "yt", false) with LogLazy {
  private val log = LoggerFactory.getLogger(getClass)

  override def renameInternal(src: Path, dst: Path, overwrite: Boolean): Unit = {
    log.debugLazy(s"Rename internal: $src -> $dst. Overwrite: $overwrite")
    val yt = this.fsImpl.asInstanceOf[YtFileSystem].ytClient
    YtWrapper.move(hadoopPathToYt(src), hadoopPathToYt(dst), force = overwrite)(yt)
  }
}
