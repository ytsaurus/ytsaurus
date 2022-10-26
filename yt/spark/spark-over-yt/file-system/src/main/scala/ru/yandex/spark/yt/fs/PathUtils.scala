package ru.yandex.spark.yt.fs

import org.apache.hadoop.fs.Path

object PathUtils {
  def hadoopPathToYt(f: Path): String = {
    f.toUri.getPath
  }

  def hadoopPathToYt(f: String): String = {
    hadoopPathToYt(new Path(f))
  }

  def getMetaPath(f: String): String = {
    s"${f}_meta"
  }

  def getMetaPath(f: Path): String = {
    getMetaPath(f.toString)
  }
}
