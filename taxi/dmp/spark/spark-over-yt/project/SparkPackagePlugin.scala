import java.io.{BufferedOutputStream, FileInputStream, FileOutputStream, PrintWriter}
import java.nio.file.attribute.{PosixFilePermission, PosixFilePermissions}
import java.nio.file.{Files, Paths, StandardCopyOption}
import java.util.zip.GZIPOutputStream

import com.typesafe.sbt.packager.debian.{DebianPlugin, JDebPackaging}
import com.typesafe.sbt.packager.linux.LinuxPackageMapping
import org.apache.commons.compress.archivers.tar.{TarArchiveEntry, TarArchiveOutputStream}
import org.apache.commons.compress.utils.IOUtils
import sbt.Keys._
import sbt.PluginTrigger.NoTrigger
import sbt._

import scala.annotation.tailrec
import scala.io.Source

object SparkPackagePlugin extends AutoPlugin {
  override def requires = super.requires && DebianPlugin && JDebPackaging && CommonPlugin

  override def trigger = NoTrigger

  private def copy(src: File, dst: String): Unit = {
    Files.copy(Paths.get(src.getAbsolutePath), Paths.get(dst), StandardCopyOption.REPLACE_EXISTING)
  }

  private def readFile(file: File): String = {
    val source = Source.fromFile(file)
    try source.mkString finally source.close()
  }

  private def writeFile(file: File, str: String): Unit = {
    val pw = new PrintWriter(new FileOutputStream(file))
    try pw.write(str) finally pw.close()
  }

  private def createFileFromTemplate(templateFile: File, parameters: Map[String, String], dstFile: File): Unit = {
    import yamusca.imports._

    val context = Context(parameters.mapValues(Value.of).toSeq: _*)
    val template = mustache.parse(readFile(templateFile)) match {
      case Right(templ) => templ
      case Left((_, message)) => throw new RuntimeException(message)
    }
    writeFile(dstFile, mustache.render(template)(context))
  }

  private def buildSpark(sparkHome: String): Unit = {
    import scala.language.postfixOps
    import scala.sys.process._

    val sparkBuildCommand = s"$sparkHome/dev/make-distribution.sh --pip -Phadoop-2.7"
    println("Building spark...")
    val code = (sparkBuildCommand !)
    if (code != 0) {
      throw new RuntimeException("Spark build failed")
    }
    println("Spark build completed")
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

  object autoImport {
    val packageSpark = taskKey[File]("Package spark task")
    val packageSparkTgz = taskKey[File]("Package spark tgz task")

    val sparkAdditionalJars = taskKey[Seq[File]]("Additional spark jars")
    val sparkDefaults = settingKey[File]("spark-defaults.conf")
    val sparkAdditionalBin = settingKey[Seq[File]]("Spark python scripts")
    val sparkName = settingKey[String]("Spark name, for example spark-2.4.4.1-yt")
    val sparkLaunchConfigTemplate = settingKey[File]("Spark launch config template")
    val sparkLauncherName = settingKey[String]("")

    def createPackageMapping(src: File, dst: String): LinuxPackageMapping = {

      @tailrec
      def inner(src: Seq[(File, String)], result: Seq[(File, String)]): Seq[(File, String)] = {
        src.headOption match {
          case None => result
          case Some((f, d)) if f.isFile => inner(src.tail, (f, d) +: result)
          case Some((f, d)) if f.isDirectory =>
            val children = f.listFiles().map(f => f -> s"$d/${f.name}")
            inner(src.tail ++ children, (f, d) +: result)
        }
      }

      LinuxPackageMapping(inner(Seq(src -> dst), Nil))
    }
  }

  import autoImport._
  import CommonPlugin.autoImport._

  override def projectSettings: Seq[Def.Setting[_]] = super.projectSettings ++ Seq(
    sparkDefaults := {
      (resourceDirectory in Compile).value / "spark-defaults.conf"
    },
    sparkAdditionalBin := {
      val pythonDir = sourceDirectory.value / "main" / "python"
      pythonDir.listFiles()
    },
    sparkLaunchConfigTemplate := {
      (resourceDirectory in Compile).value / "spark-launch.ini.template"
    },
    packageSpark := {
      val sparkHome = baseDirectory.value.getParentFile.getParentFile / "spark"
      val sparkDist = sparkHome / "dist"
      val rebuildSpark = Option(System.getProperty("rebuildSpark")).forall(_.toBoolean)

      if (rebuildSpark) {
        buildSpark(sparkHome.toString)
      }

      sparkAdditionalJars.value.foreach { file =>
        copy(file, s"$sparkDist/jars/${file.name}")
      }

      copy(sparkDefaults.value, s"$sparkDist/conf/spark-defaults.conf")

      sparkAdditionalBin.value.foreach { file =>
        copy(file, s"$sparkDist/bin/${file.name}")
      }

      createFileFromTemplate(sparkLaunchConfigTemplate.value, Map(
        "spark_yt_base_path" -> publishYtTo.value,
        "launcher_name" -> s"${sparkLauncherName.value}-${version.value}.jar",
        "spark_name" -> sparkName.value
      ), sparkDist / "conf" / "spark-launch.ini")

      sparkDist
    },
    packageSparkTgz := {
      val dist = packageSpark.value
      val tgzFile = target.value / s"${sparkName.value}.tgz"

      writeTar(tgzFile) { tos =>
        addFilesToTgz(tos, Seq(dist -> sparkName.value), rename = true)
      }

      tgzFile
    }
  )
}
