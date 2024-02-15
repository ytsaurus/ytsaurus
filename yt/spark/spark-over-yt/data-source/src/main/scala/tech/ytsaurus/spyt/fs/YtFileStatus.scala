package tech.ytsaurus.spyt.fs

import org.apache.hadoop.fs.{FileStatus, LocatedFileStatus}

object YtFileStatus {
    def toFileStatus(path: YtHadoopPath): FileStatus = {
        new LocatedFileStatus(
            new FileStatus(path.meta.size, false, 1, path.meta.size,
                path.meta.modificationTime, path),
            Array.empty)
    }
}
