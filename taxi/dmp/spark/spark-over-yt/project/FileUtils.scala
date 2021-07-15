package spyt

import sbt._

import java.io.FilenameFilter
import scala.annotation.tailrec

object FileUtils {
  def deleteDirectory(dir: File, recursive: Boolean = false): Unit = {
    @tailrec
    def inner(files: List[File]): Unit = {
      files match {
        case head :: tail if head.isDirectory && head.listFiles().nonEmpty =>
          inner(head.listFiles().toList ++ files)
        case head :: tail =>
          IO.delete(head)
          inner(tail)
        case Nil =>
      }
    }

    if (dir.exists()) {
      if (!dir.isDirectory) {
        throw new IllegalArgumentException("")
      }
      if (dir.listFiles().isEmpty) {
        IO.delete(dir)
      } else {
        if (!recursive) {
          throw new IllegalArgumentException("")
        }
        inner(List(dir))
      }
    }
  }

  def deleteFiles(dir: File, filter: FilenameFilter): Unit = {
    println(dir.listFiles(filter).map(_.getPath).mkString(", "))
    dir.listFiles(filter).foreach(IO.delete)
  }

  def copyDirectory(src: File, dst: File, ignore: Set[String]): Unit = {
    @tailrec
    def inner(copy: Seq[(File, File)]): Unit = {
      copy match {
        case (innerSrc, innerDst) :: tail =>
          innerSrc match {
            case ignored if ignore.exists(ignored.getName.contains) => inner(tail)
            case d if d.isDirectory =>
              IO.createDirectory(innerDst)
              val newFiles = IO.listFiles(d).map(f => f -> new File(innerDst, f.getName))
              inner(tail ++ newFiles)
            case f if f.isFile =>
              IO.copyFile(f, innerDst)
              inner(tail)
          }
        case Nil =>
      }
    }

    inner(Seq(src -> dst))
  }

}
