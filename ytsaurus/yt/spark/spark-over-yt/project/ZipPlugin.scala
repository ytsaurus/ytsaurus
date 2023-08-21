package spyt

import org.apache.commons.compress.utils.IOUtils
import sbt.Keys._
import sbt.PluginTrigger.AllRequirements
import sbt._

import java.io.{FileInputStream, FileOutputStream}
import java.util.zip.{ZipEntry, ZipOutputStream}
import scala.annotation.tailrec

object ZipPlugin extends AutoPlugin {

  override def trigger = AllRequirements

  override def requires = super.requires

  object autoImport {
    val zip = taskKey[File]("Build .zip archive")

    val zipMapping = taskKey[Seq[(File, String)]]("Source files for archive")
    val zipPath = taskKey[Option[File]]("Path to create tar archive")
    val zipIgnore = taskKey[File => Boolean]("Rule to skip files")
  }

  import autoImport._

  private def addFilesToZip(zipStream: ZipOutputStream,
                            files: Seq[(File, String)],
                            ignoreFile: File => Boolean,
                            log: Logger): Unit = {
    @tailrec
    def inner(fs: Seq[(File, String)]): Unit = {
      fs match {
        case Nil =>
        case (file, base) :: tail =>
          val entryName = if (base.isEmpty) file.getName else s"$base/${file.getName}"

          file match {
            case ignored if ignoreFile(ignored) =>
              inner(tail)
            case regFile if regFile.isFile =>
              val entry = new ZipEntry(entryName)
              zipStream.putNextEntry(entry)
              IOUtils.copy(new FileInputStream(regFile), zipStream)
              zipStream.closeEntry()
              inner(tail)
            case directory if file.isDirectory =>
              val newFiles = directory.listFiles().map(_ -> entryName)
              inner(tail ++ newFiles)
          }
      }
    }

    inner(files)
  }

  override def projectSettings: Seq[Def.Setting[_]] = Seq(
    zipMapping := Nil,
    zipPath := None,
    zipIgnore := { _ => false },
    zip := {
      val log = streams.value.log
      val path = zipPath.value.get
      if (path.exists()) path.delete()
      val zipStream = new ZipOutputStream(new FileOutputStream(path))

      try {
        addFilesToZip(zipStream, zipMapping.value, zipIgnore.value, log)
      } finally {
        zipStream.close()
      }
      path
    }
  )


}
