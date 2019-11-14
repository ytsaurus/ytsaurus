import java.nio.file.{Files, Paths, StandardCopyOption}

import com.typesafe.sbt.packager.debian.{DebianPlugin, JDebPackaging}
import com.typesafe.sbt.packager.linux.{LinuxPackageMapping, LinuxSymlink}
import sbt.Keys._
import sbt.PluginTrigger.NoTrigger
import sbt._

import scala.annotation.tailrec

object SparkPackagePlugin extends AutoPlugin {
  override def requires = super.requires && DebianPlugin && JDebPackaging

  override def trigger = NoTrigger

  def copy(src: File, dst: String): Unit = {
    Files.copy(Paths.get(src.getAbsolutePath), Paths.get(dst), StandardCopyOption.REPLACE_EXISTING)
  }

  object autoImport {
    val packageSpark = taskKey[File]("Package spark task")

    val sparkAdditionalJars = taskKey[Seq[File]]("Additional spark jars")
    val sparkDefaults = settingKey[File]("spark-defaults.conf")
    val sparkAdditionalBin = settingKey[Seq[File]]("Spark python scripts")
    val sparkName = settingKey[String]("Spark name, for example spark-2.4.4.1-yt")

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

  override def projectSettings: Seq[Def.Setting[_]] = Seq(
    sparkDefaults := {
      (resourceDirectory in Compile).value / "spark-defaults.conf"
    },
    sparkAdditionalBin := {
      val pythonDir = sourceDirectory.value / "main" / "python"
      pythonDir.listFiles()
    },
    packageSpark := {
      val sparkHome = baseDirectory.value.getParentFile.getParentFile / "spark"
      val sparkBuildCommand = s"$sparkHome/dev/make-distribution.sh --pip -Phadoop-2.7"
      val sparkDist = sparkHome / "dist"
      val rebuildSpark = Option(System.getProperty("rebuildSpark")).forall(_.toBoolean)

      import scala.sys.process._
      import scala.language.postfixOps

      if (rebuildSpark) {
        println("Building spark...")
        val code = (sparkBuildCommand !)
        if (code != 0) {
          throw new RuntimeException("Spark build failed")
        }
        println("Spark build completed")
      }

      sparkAdditionalJars.value.foreach{ file =>
        copy(file, s"$sparkDist/jars/${file.name}")
      }

      copy(sparkDefaults.value, s"$sparkDist/conf/spark-defaults.conf")

      sparkAdditionalBin.value.foreach { file =>
        copy(file, s"$sparkDist/bin/${file.name}")
      }

      sparkDist
    }
  )
}
