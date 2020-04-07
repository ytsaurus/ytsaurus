import java.io.{File, FileInputStream, InputStreamReader}
import java.util.Properties

import com.typesafe.sbt.packager.linux.LinuxPackageMapping
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTreeBuilder
import ru.yandex.inside.yt.kosher.impl.ytree.serialization.YTreeTextSerializer
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
                               spark_conf: Map[String, String],
                               file_paths: Seq[String],
                               layer_paths: Seq[String] = Seq(
                                 "//home/sashbel/delta/jdk/layer_with_jdk_lastest.tar.gz",
                                 "//home/sashbel/delta/python/layer_with_python37.tar.gz",
                                 "//home/sashbel/delta/python/layer_with_python34.tar.gz",
                                 "//porto_layers/base/xenial/porto_layer_search_ubuntu_xenial_app_lastest.tar.gz"
                               ),
                               environment: Map[String, String] = Map(
                                 "JAVA_HOME" -> "/opt/jdk8",
                                 "IS_SPARK_CLUSTER" -> "true",
                                 "YT_ALLOW_HTTP_REQUESTS_TO_YT_FROM_JOB" -> "1"
                               ),
                               operation_spec: Map[String, String] = Map(),
                               python_cluster_paths: Map[String, String] = Map(
                                 "3.7" -> "/opt/python3.7/bin/python3.7",
                                 "3.5" -> "python3.5",
                                 "3.4" -> "/opt/python3.4/bin/python3.4",
                                 "2.7" -> "python2.7"
                               )) {

    private def toYson(value: Any, builder: YTreeBuilder): YTreeBuilder = {
      value match {
        case s: String => builder.value(s)
        case m: Map[String, String] => m.foldLeft(builder.beginMap()){case (res, (k, v)) => res.key(k).value(v)}.endMap()
        case ss: Seq[String] => ss.foldLeft(builder.beginList()){case (res, next) => res.value(next)}.endList()
        case i: Int => builder.value(i)
      }
    }

    def toYson: String = {
      val node = this.getClass.getDeclaredFields.foldLeft(new YTreeBuilder().beginMap()){
        case (res, nextField) => toYson(nextField.get(this), res.key(nextField.getName))
      }.endMap().build()
      YTreeTextSerializer.serialize(node)
    }
  }

  object SparkLaunchConfig {
    def apply(spark_yt_base_path: String, spark_conf: Map[String, String]): SparkLaunchConfig = {
      new SparkLaunchConfig(
        spark_yt_base_path = spark_yt_base_path,
        spark_conf = spark_conf,
        file_paths = Seq(
        s"$spark_yt_base_path/spark.tgz",
        s"$spark_yt_base_path/spark-yt-launcher.jar"
      ))
    }
  }

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
    val sparkYtConfBasePath = taskKey[String]("YT base path for spark configs")
    val sparkYtSubdir = taskKey[String]("Snapshots or releases")

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

  override def projectSettings: Seq[Def.Setting[_]] = super.projectSettings ++ Seq(
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
      if (isSnapshot.value || version.value.contains("beta")) {
        "snapshots"
      } else {
        "releases"
      }
    },
    sparkYtBinBasePath := s"${sparkYtBasePath.value}/bin/${sparkYtSubdir.value}/${version.value}",
    sparkYtConfBasePath := s"${sparkYtBasePath.value}/conf/${sparkYtSubdir.value}",
    sparkYtConfigs := {
      val binBasePath = sparkYtBinBasePath.value
      val confBasePath = sparkYtConfBasePath.value

      sparkYtProxies.value.map { proxy =>
        val proxyShort = proxy.split("\\.").head
        val proxyDefaultsFile = (resourceDirectory in Compile).value / s"spark-defaults-$proxyShort.conf"
        val proxyDefaults = readSparkDefaults(proxyDefaultsFile)
        val launchConfig = SparkLaunchConfig(binBasePath, proxyDefaults)

        YtPublishDocument(launchConfig.toYson, confBasePath, Some(proxy), "spark-launch-conf")
      }
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

      sparkAdditionalPython.value.foreach { f =>
        IO.copyDirectory(f, pythonDir)

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

    val sparkBuildCommand = s"$sparkHome/dev/make-distribution.sh -Phadoop-2.7"
    println("Building spark...")
    val code = (sparkBuildCommand !)
    if (code != 0) {
      throw new RuntimeException("Spark build failed")
    }
    println("Spark build completed")
  }

}
