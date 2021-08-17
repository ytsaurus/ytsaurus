package ru.yandex.spark.launcher

import java.io.{File, FileWriter}

import scala.io.Source
import scala.util.{Failure, Success, Try}

trait VanillaLauncher {
  lazy val home: String = new File(sys.env.getOrElse("HOME", ".")).getAbsolutePath

  lazy val sparkSystemProperties: Map[String, String] = {
    import scala.collection.JavaConverters._
    System.getProperties
      .stringPropertyNames().asScala
      .collect {
        case name if name.startsWith("spark.") => name -> System.getProperty(name)
      }
      .toMap
  }

  def path(path: String): String = replaceHome(path)

  def env(name: String, default: => String): String = {
    replaceHome(sys.env.getOrElse(name, default))
  }

  def replaceHome(str: String): String = str.replaceAll("\\$HOME", home)

  def createFromTemplate(src: File)
                        (f: String => String): File = {
    val dst = new File(src.getAbsolutePath.replace(".template", ""))
    val is = Source.fromFile(src)
    val os = new FileWriter(dst)
    val res = Try(os.write(f(is.mkString)))
    is.close()
    os.close()
    res match {
      case Success(_) => dst
      case Failure(exception) => throw exception
    }
  }

  def profilingJavaOpt(port: Int) =
    s"-agentpath:/slot/sandbox/YourKit-JavaProfiler-2019.8/bin/linux-x86-64/libyjpagent.so=port=$port,listen=all"

  def isProfilingEnabled: Boolean = sparkSystemProperties.get("spark.hadoop.yt.profiling.enabled").exists(_.toBoolean)

  def prepareProfiler(): Unit = {
    import sys.process._
    import scala.language.postfixOps

    if (isProfilingEnabled) {
      val code = "unzip profiler.zip" !

      if (code != 0) {
        throw new IllegalStateException("Failed to unzip profiler")
      }
    }
  }
}
