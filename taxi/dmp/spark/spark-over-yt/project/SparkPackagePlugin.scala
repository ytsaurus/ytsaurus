import java.io.{File, FileInputStream, InputStreamReader}
import java.util.Properties

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

  import YtPublishPlugin.autoImport._
  import autoImport._
  object autoImport {
    val sparkPackage = taskKey[File]("Build spark and add custom files")

    val sparkAdditionalJars = taskKey[Seq[File]]("Jars to copy in SPARK_HOME/jars")
    val sparkAdditionalBin = settingKey[Seq[File]]("Scripts to copy in SPARK_HOME/bin")
    val sparkAdditionalPython = settingKey[Seq[File]]("Files to copy in SPARK_HOME/python")
    val sparkLocalConfigs = taskKey[Seq[File]]("Configs to copy in SPARK_HOME/conf")

    val sparkYtProxies = settingKey[Seq[String]]("YT proxies to create configs")
    val sparkYtConfigs = taskKey[Seq[YtPublishArtifact]]("Configs to copy in YT conf dir")
    val sparkYtBasePath = settingKey[String]("YT base path for spark")
    val sparkYtBinBasePath = taskKey[String]("YT base path for spark binaries")
    val sparkYtSubdir = taskKey[String]("Snapshots or releases")
    val sparkIsSnapshot = settingKey[Boolean]("Flag of spark snapshot version")

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

  private def readSparkDefaults(file: File): Map[String, String] = {
    import scala.collection.JavaConverters._
    val reader = new InputStreamReader(new FileInputStream(file))
    val properties = new Properties()
    try {
      properties.load(reader)
    } finally {
      reader.close()
    }
    properties.stringPropertyNames().asScala.map{name =>
      name -> properties.getProperty(name)
    }.toMap
  }

  private def copyDirectory(src: File, dst: File, ignore: Set[String]): Unit = {
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

  override def projectSettings: Seq[Def.Setting[_]] = super.projectSettings ++ Seq(
    sparkIsSnapshot := isSnapshot.value || version.value.contains("beta"),
    sparkLocalConfigs := {
      Seq(
        (resourceDirectory in Compile).value / "spark-defaults.conf",
        (resourceDirectory in Compile).value / "spark-env.sh"
      )
    },
    sparkAdditionalBin := {
      val pythonDir = sourceDirectory.value / "main" / "python" / "bin"
      pythonDir.listFiles()
    },
    sparkYtBasePath := "//sys/spark",
    sparkYtSubdir := {
      if (sparkIsSnapshot.value) "snapshots" else "releases"
    },
    sparkYtBinBasePath := s"${sparkYtBasePath.value}/bin/${sparkYtSubdir.value}/${version.value}",
    sparkYtConfigs := {
      val binBasePath = sparkYtBinBasePath.value
      val confBasePath = s"${sparkYtBasePath.value}/conf"
      val sparkVersion = version.value

      val launchConfig = SparkLaunchConfig(binBasePath, Map("spark.testVersion" -> "testtesttest"))
      val launchConfigPublish = YtPublishDocument(
        launchConfig.toYson,
        s"$confBasePath/${sparkYtSubdir.value}/$sparkVersion",
        None,
        "spark-launch-conf"
      )

      val globalConfigPublish = if (!sparkIsSnapshot.value) {
        sparkYtProxies.value.map { proxy =>
          val proxyShort = proxy.split("\\.").head
          val proxyDefaultsFile = (resourceDirectory in Compile).value / s"spark-defaults-$proxyShort.conf"
          val proxyDefaults = readSparkDefaults(proxyDefaultsFile)
          val globalConfig = SparkGlobalConfig(proxyDefaults, sparkVersion)

          YtPublishDocument(globalConfig.toYson, confBasePath, Some(proxy), "global")
        }
      } else Nil

      launchConfigPublish +: globalConfigPublish
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
      sparkLocalConfigs.value.foreach { file =>
        IO.copyFile(file, sparkDist / "conf" / file.name)
      }
      sparkAdditionalBin.value.foreach { file =>
        IO.copyFile(file, sparkDist / "bin" / file.name, preserveExecutable = true)
      }

      val pythonDir = sparkDist / "bin" / "python"
      if (!pythonDir.exists()) IO.createDirectory(pythonDir)
      val ignorePython = Set("build", "dist", ".egg-info", "setup.py", ".pyc", "__pycache__")
      sparkAdditionalPython.value.foreach(copyDirectory(_, pythonDir, ignorePython))

      sparkDist
    }
  )

  private def buildSpark(sparkHome: String): Unit = {
    import scala.language.postfixOps
    import scala.sys.process._

    val sparkBuildCommand = s"$sparkHome/dev/make-distribution.sh -Phadoop-2.7"
    println("Building spark...")
    val code = (sparkBuildCommand !)
    if (code != 0) {
      throw new RuntimeException("Spark build failed")
    }
    println("Spark build completed")
  }

}
