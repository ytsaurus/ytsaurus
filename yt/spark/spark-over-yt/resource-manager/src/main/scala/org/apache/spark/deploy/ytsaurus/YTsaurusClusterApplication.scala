
package org.apache.spark.deploy.ytsaurus

import scala.collection.mutable
import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkApplication
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.cluster.ytsaurus.YTsaurusOperationManager
import org.apache.spark.scheduler.cluster.ytsaurus.YTsaurusOperationManager.{getWebUIAddress, getOperationState}


private[spark] class YTsaurusClusterApplication extends SparkApplication with Logging {
  import YTsaurusClusterApplication._

  override def start(args: Array[String], conf: SparkConf): Unit = {
    val masterURL = conf.get("spark.master")
    val ytProxy = YTsaurusUtils.parseMasterUrl(masterURL)

    val appArgs = ApplicationArguments.fromCommandLineArgs(args)

    logInfo(s"Submitting spark application to YTsaurus cluster at $ytProxy")
    logInfo(s"Application arguments: $appArgs")

    val operationManager = YTsaurusOperationManager.create(ytProxy, conf)

    try {
      val driverOperation = operationManager.startDriver(conf, appArgs)

      var currentState = "undefined"
      var webUIAddress: Option[String] = None
      while (!YTsaurusOperationManager.isFinalState(currentState)) {
        Thread.sleep(pingInterval)
        val opSpec = operationManager.getOperation(driverOperation)
        currentState = getOperationState(opSpec)
        logInfo(s"Operation: ${driverOperation.id}, State: $currentState")

        val currentWebUiAddress = getWebUIAddress(opSpec)
        if (currentWebUiAddress.isDefined && currentWebUiAddress != webUIAddress) {
          webUIAddress = currentWebUiAddress
          webUIAddress.foreach(addr => logInfo(s"Web UI: $addr"))
        }
      }
    } finally {
      operationManager.close()
    }
  }
}

object YTsaurusClusterApplication {
  private val pingInterval = 3000L
}

private[spark] case class ApplicationArguments(
  mainAppResource: Option[String],
  mainAppResourceType: String,
  mainClass: String,
  driverArgs: Array[String],
  proxyUser: Option[String])

private[spark] object ApplicationArguments {

  def fromCommandLineArgs(args: Array[String]): ApplicationArguments = {
    var mainAppResource: Option[String] = None
    var mainAppResourceType: String = "java"
    var mainClass: Option[String] = None
    val driverArgs = mutable.ArrayBuffer.empty[String]
    var proxyUser: Option[String] = None

    args.sliding(2, 2).toList.foreach {
      case Array("--primary-java-resource", primaryJavaResource: String) =>
        mainAppResource = Some(primaryJavaResource)
        mainAppResourceType = "java"
      case Array("--primary-py-file", primaryPythonResource: String) =>
        mainAppResource = Some(primaryPythonResource)
        mainAppResourceType = "python"
      case Array("--primary-r-file", primaryRFile: String) =>
        mainAppResource = Some(primaryRFile)
        mainAppResourceType = "R"
      case Array("--main-class", clazz: String) =>
        mainClass = Some(clazz)
      case Array("--arg", arg: String) =>
        driverArgs += arg
      case Array("--proxy-user", user: String) =>
        proxyUser = Some(user)
      case other =>
        val invalid = other.mkString(" ")
        throw new RuntimeException(s"Unknown arguments: $invalid")
    }

    require(mainClass.isDefined, "Main class must be specified via --main-class")

    ApplicationArguments(
      mainAppResource,
      mainAppResourceType,
      mainClass.get,
      driverArgs.toArray,
      proxyUser)
  }
}
