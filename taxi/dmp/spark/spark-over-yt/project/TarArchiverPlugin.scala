import java.io.{BufferedOutputStream, FileInputStream, FileOutputStream}
import java.nio.file.attribute.PosixFilePermission
import java.nio.file.{Files, Paths}
import java.util.zip.GZIPOutputStream

import org.apache.commons.compress.archivers.tar.{TarArchiveEntry, TarArchiveOutputStream}
import org.apache.commons.compress.utils.IOUtils
import sbt.PluginTrigger.AllRequirements
import sbt._

import scala.annotation.tailrec

object TarArchiverPlugin extends AutoPlugin {

  override def trigger = AllRequirements

  override def requires = super.requires

  object autoImport {
    val tarArchiveBuild = taskKey[File]("Build .tar.gz archive")

    val tarArchiveMapping = taskKey[Seq[(File, String)]]("Source files for archive")
    val tarArchivePath = taskKey[Option[File]]("Path to create tar archive")
  }

  import autoImport._

  override def projectSettings: Seq[Def.Setting[_]] = Seq(
    tarArchiveMapping := Nil,
    tarArchivePath := None,
    tarArchiveBuild := {
      if (tarArchiveMapping.value.isEmpty || tarArchivePath.value.isEmpty) {
        throw new RuntimeException("")
      }
      archiveTo(tarArchivePath.value.get, tarArchiveMapping.value)
    }
  )

  private def archiveTo(tgzFile: File, files: Seq[(File, String)]): File = {
    writeTar(tgzFile) { tos =>
      TarArchiverPlugin.addFilesToTgz(tos, files, rename = true)
    }
    tgzFile
  }

  private def writeTar(file: File)(f: TarArchiveOutputStream => Unit): Unit = {
    val fos = new FileOutputStream(file)
    try {
      val bos = new BufferedOutputStream(fos)
      try {
        val gos = new GZIPOutputStream(bos)
        try {
          val tos = new TarArchiveOutputStream(gos)
          tos.setLongFileMode(TarArchiveOutputStream.LONGFILE_POSIX)
          f(tos)
        } finally gos.close()
      } finally bos.close()
    } finally fos.close()
  }

  private def intFilePermissions(perm: Set[PosixFilePermission]): Int = {
    import java.nio.file.attribute.PosixFilePermission._
    val permToInt = Map(
      OWNER_READ -> 400, OWNER_WRITE -> 200, OWNER_EXECUTE -> 100,
      GROUP_READ -> 40, GROUP_WRITE -> 20, GROUP_EXECUTE -> 10,
      OTHERS_READ -> 4, OTHERS_WRITE -> 2, OTHERS_EXECUTE -> 1
    )
    val permSum = perm.map(permToInt).sum
    BigInt(s"100$permSum", 8).toInt
  }

  @tailrec
  private def addFilesToTgz(tos: TarArchiveOutputStream, files: Seq[(File, String)], rename: Boolean): Unit = {
    files match {
      case Nil =>
      case (file, base) :: tail =>
        import scala.collection.JavaConverters._

        val entryName = if (rename) base else s"$base/${file.getName}"
        val entry = new TarArchiveEntry(file, entryName)
        val mode = Files.getPosixFilePermissions(Paths.get(file.getAbsolutePath)).asScala.toSet
        entry.setMode(intFilePermissions(mode))
        tos.putArchiveEntry(entry)

        file match {
          case regFile if regFile.isFile =>
            IOUtils.copy(new FileInputStream(regFile), tos)
            tos.closeArchiveEntry()
            addFilesToTgz(tos, tail, rename = false)
          case directory if file.isDirectory =>
            tos.closeArchiveEntry()
            val newFiles = directory.listFiles().map(_ -> entryName)
            addFilesToTgz(tos, tail ++ newFiles, rename = false)
        }
    }
  }

}
