package org.apache.spark.deploy.worker

import org.apache.spark.SparkConf
import org.apache.spark.deploy.{DriverDescription, SparkHadoopUtil}
import org.apache.spark.deploy.worker.DriverRunnerDecorators.addLocalPyFiles
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.SUBMIT_PYTHON_FILES
import org.apache.spark.util.Utils
import tech.ytsaurus.spyt.patch.annotations.{Decorate, DecoratedMethod, OriginClass}

import java.io.{File, IOException}
import java.net.URI
import java.util.stream.Collectors

@Decorate
@OriginClass("org.apache.spark.deploy.worker.DriverRunner")
class DriverRunnerDecorators {

  @DecoratedMethod
  private def runDriver(builder: ProcessBuilder, baseDir: File, supervise: Boolean): Int = {
    val conf = this.getClass.getDeclaredField("conf").get(this).asInstanceOf[SparkConf]
    val driverDesc = this.getClass.getDeclaredField("driverDesc").get(this).asInstanceOf[DriverDescription]
    val localPyFiles = DriverRunnerDecorators.downloadPythonFiles(driverDesc, baseDir, conf)

    builder.command(addLocalPyFiles(builder.command(), localPyFiles))

    __runDriver(builder, baseDir, supervise)
  }

  private def __runDriver(builder: ProcessBuilder, baseDir: File, supervise: Boolean): Int = ???
}

object DriverRunnerDecorators extends Logging {
  def downloadPythonFiles(driverDesc: DriverDescription, driverDir: File, conf: SparkConf): Seq[String] = {
    val pyFiles = driverDesc.command.javaOpts.flatMap {
      case opt if opt.trim.startsWith(s"-D${SUBMIT_PYTHON_FILES.key}=") =>
        Utils.stringToSeq(opt.substring(opt.indexOf("=") + 1))
      case _ => Nil
    }
    logInfo(s"Found python dependencies: ${pyFiles}")
    pyFiles.map { pyFileUrl =>
      // almost exact copy of a DriverRunner.downloadUserJar method
      val fileName = new URI(pyFileUrl).getPath.split("/").last
      val localFile = new File(driverDir, fileName)
      if (!localFile.exists()) {
        logInfo(s"Copying supplied python file $pyFileUrl to $localFile")
        Utils.fetchFile(
          pyFileUrl,
          driverDir,
          conf,
          SparkHadoopUtil.get.newConfiguration(conf),
          System.currentTimeMillis(),
          useCache = false
        )
        if (!localFile.exists()) { // Verify copy succeeded
          throw new IOException(s"Can not find expected file $fileName which should have been loaded in $driverDir")
        }
      }
      localFile.getAbsolutePath
    }
  }

  def addLocalPyFiles(command: java.util.List[String], localPyFiles: Seq[String]): java.util.List[String] = {
    command.stream().map {
      case "{{PY_FILES}}" => localPyFiles.mkString(",")
      case other => other
    }.collect(Collectors.toList()).asInstanceOf[java.util.List[String]]
  }
}