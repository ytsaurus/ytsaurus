import java.io.File

import _root_.io.circe.generic.auto._
import _root_.io.circe.syntax._
import com.typesafe.sbt.packager.linux.LinuxPackageMapping
import ru.yandex.sbt.YtPublishPlugin
import sbt.Keys._
import sbt.PluginTrigger.NoTrigger
import sbt._

import scala.annotation.tailrec
import scala.language.postfixOps

object SparkPackagePlugin extends AutoPlugin {
  override def requires = super.requires && YtPublishPlugin

  override def trigger = NoTrigger

  case class SparkLaunchConfig(spark_yt_base_path: String,
                               spark_launcher_name: String,
                               spark_name: String,
                               shuffle_service_port: Int = 27000,
                               start_port: Int = 27001,
                               port_max_retries: Int = 200)

  object autoImport {
    val sparkPackage = taskKey[File]("Build spark and add custom files")

    val sparkAdditionalJars = taskKey[Seq[File]]("Additional spark jars")
    val sparkDefaults = settingKey[File]("spark-defaults.conf")
    val sparkEnv = settingKey[File]("spark-env.sh")
    val sparkAdditionalBin = settingKey[Seq[File]]("Scripts to copy in SPARK_HOME/bin")
    val sparkAdditionalPython = settingKey[Seq[File]]("Files to copy in SPARK_HOME/python")
    val sparkName = settingKey[String]("Spark name, for example spark-2.4.4-0.0.1-SNAPSHOT")
    val sparkLaunchConfig = taskKey[Seq[File]]("Spark launch config")
    val sparkLauncherName = settingKey[String]("Name of spark-launcher jar")

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

  import YtPublishPlugin.autoImport._
  import autoImport._

  override def projectSettings: Seq[Def.Setting[_]] = super.projectSettings ++ Seq(
    sparkName := s"spark-${version.value}",
    sparkDefaults := {
      (resourceDirectory in Compile).value / "spark-defaults.conf"
    },
    sparkEnv := {
      (resourceDirectory in Compile).value / "spark-env.sh"
    },
    sparkAdditionalBin := {
      val pythonDir = sourceDirectory.value / "main" / "python" / "bin"
      pythonDir.listFiles()
    },
    sparkLaunchConfig := {
      val conf = SparkLaunchConfig(
        spark_yt_base_path = publishYtDir.value,
        spark_launcher_name = s"${sparkLauncherName.value}-${(version in ThisBuild).value}.jar",
        spark_name = sparkName.value
      )

      val file = target.value / "spark-launch.json"
      IO.write(file, conf.asJson.spaces4SortKeys)

      Seq(file)
    },
    sparkPackage := {
      val sparkHome = baseDirectory.value.getParentFile.getParentFile / "spark"
      val sparkDist = sparkHome / "dist"
      val rebuildSpark = Option(System.getProperty("rebuildSpark")).forall(_.toBoolean)

      if (rebuildSpark) {
        buildSpark(sparkHome.toString)
      }

      sparkAdditionalJars.value.foreach { file =>
        IO.copyFile(file, sparkDist / "jars" / file.name)
      }
      IO.copyFile(sparkDefaults.value, sparkDist / "conf" / "spark-defaults.conf")
      IO.copyFile(sparkEnv.value, sparkDist / "conf" / "spark-env.sh")
      sparkAdditionalBin.value.foreach { file =>
        IO.copyFile(file, sparkDist / "bin" / file.name, preserveExecutable = true)
      }
      val pythonDir = sparkDist / "bin" / "python"
      if (!pythonDir.exists()) {
        IO.createDirectory(sparkDist / "bin" / "python")
      }
      IO.copyDirectory(sourceDirectory.value / "main" / "python" / "client", sparkDist / "bin" / "python")

      val ytClient = sourceDirectory.value / "main" / "python" / "client"
      (ytClient +: sparkAdditionalPython.value).foreach { f =>
        IO.copyDirectory(f, sparkDist / "python")

        import sys.process._
        IO.listFiles(f).foreach {
          case ff if ff.isDirectory && IO.listFiles(ff).nonEmpty && Set("egg-info", "dist", "build").forall(n => !ff.getName.contains(n)) =>
            IO.delete(new File(s"/tmp/${ff.getName}.zip"))
            val processString = s"zip /tmp/${ff.getName}.zip ${IO.listFiles(ff).map(i => ff.getName + File.separator + i.getName).mkString(" ")}"
            println(processString)
            Process(processString, cwd = f) !

            IO.copyFile(new File(s"/tmp/${ff.getName}.zip"), sparkDist / "python" / "lib" / s"${ff.getName}.zip")
          case _ =>
        }
      }

      sparkDist
    }
  )

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

}
