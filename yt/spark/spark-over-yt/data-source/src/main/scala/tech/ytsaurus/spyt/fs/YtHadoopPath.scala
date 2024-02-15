package tech.ytsaurus.spyt.fs

import org.apache.hadoop.fs.Path
import tech.ytsaurus.core.cypress.YPath
import tech.ytsaurus.spyt.fs.path.YPathEnriched
import tech.ytsaurus.spyt.fs.path.YPathEnriched.ypath
import tech.ytsaurus.spyt.wrapper.table.OptimizeMode

import scala.util.Try

case class YtTableMeta(rowCount: Long,
                       size: Long,
                       modificationTime: Long,
                       optimizeMode: OptimizeMode,
                       isDynamic: Boolean) {
  def approximateRowSize: Long = if (rowCount == 0) 0 else (size + rowCount - 1) / rowCount
}

case class YtHadoopPath(ypath: YPathEnriched,
                        meta: YtTableMeta) extends Path(ypath.toPath, YtHadoopPath.toFileName(meta)) {
  def toStringPath: String = ypath.toStringPath

  def toYPath: YPath = ypath.toYPath
}

object YtHadoopPath {
  private def toFileName(meta: YtTableMeta): String = {
    import meta._
    s"${rowCount}_${size}_${modificationTime}_${optimizeMode.name}_${isDynamic}"
  }

  private def tryDeserialize(path: Path): Option[YtHadoopPath] = {
    Try {
      val rowCountStr :: sizeStr :: modificationTimeStr :: optimizeModeStr :: isDynamicStr :: Nil =
        path.getName.trim.split("_", 5).toList
      val rowCount = rowCountStr.trim.toLong
      val size = sizeStr.trim.toLong
      val modificationTime = modificationTimeStr.trim.toLong
      val optimizeMode = OptimizeMode.fromName(optimizeModeStr.trim)
      val isDynamic = isDynamicStr.trim.toBoolean
      YtHadoopPath(ypath(path.getParent), YtTableMeta(rowCount, size, modificationTime, optimizeMode, isDynamic))
    }.toOption
  }

  def fromPath(path: Path): Path = {
    path match {
      case yp: YtHadoopPath => yp
      case p => YtHadoopPath.tryDeserialize(p).getOrElse(p)
    }
  }
}
