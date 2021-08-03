package ru.yandex.spark.yt.submit

import java.io.File
import java.util.UUID

import org.apache.commons.io.FileUtils
import org.apache.spark.deploy.rest.{DriverState, RestSubmissionClientWrapper}
import org.apache.spark.launcher.InProcessLauncher
import org.slf4j.LoggerFactory
import ru.yandex.inside.yt.kosher.cypress.YPath
import ru.yandex.spark.yt.wrapper.YtWrapper
import ru.yandex.spark.yt.wrapper.client.YtClientConfiguration
import ru.yandex.spark.yt.wrapper.discovery.CypressDiscoveryService

import scala.concurrent.duration._
import scala.language.postfixOps

class SubmissionClient(proxy: String,
                       discoveryPath: String,
                       spytVersion: String,
                       user: String,
                       token: String) {
  private val log = LoggerFactory.getLogger(getClass)

  private val (address, clusterVersion, eventLogPath) = {
    val ytClient = YtWrapper.createRpcClient("submission client", YtClientConfiguration.default(proxy, user, token))
    try {

      implicit val yt = ytClient.yt
      val discoveryService = new CypressDiscoveryService(discoveryPath + "/discovery")

      val a = discoveryService.discoverAddress().
        getOrElse(throw new IllegalArgumentException(s"Master address is not found, check discovery path $discoveryPath"))

      val cv = discoveryService.clusterVersion
        .getOrElse(throw new IllegalArgumentException(s"Cluster version is not found, check discovery path $discoveryPath"))

      (a, cv, discoveryService.eventLogPath(discoveryPath))
    } finally {
      ytClient.close()
    }
  }

  private val master = s"spark://${address.hostAndPort}"
  private val rest = s"spark://${address.restHostAndPort}"

  private val client = RestSubmissionClientWrapper.create(rest)

  def newLauncher(): InProcessLauncher = new InProcessLauncher()

  def submit(launcher: InProcessLauncher): String = {
    val fileName = s"spark-submission-${UUID.randomUUID()}"
    val submissionIdFile = new File(FileUtils.getTempDirectory, s"$fileName-id")
    val submissionErrorFile = new File(FileUtils.getTempDirectory, s"$fileName-error")

    launcher.setMaster(master)
    launcher.setDeployMode("cluster")
    launcher.setConf("spark.master.rest.enabled", "true")
    launcher.setConf("spark.rest.master", rest)
    launcher.setConf("spark.rest.client.awaitTermination.enabled", "false")
    launcher.setConf("spark.hadoop.yt.proxy", proxy)
    launcher.setConf("spark.hadoop.yt.user", user)
    launcher.setConf("spark.hadoop.yt.token", token)
    launcher.setConf("spark.yt.cluster.version", clusterVersion)
    launcher.setConf("spark.yt.version", spytVersion)
    launcher.setConf("spark.yt.jars", s"yt:/${spytJarPath(spytVersion)}")
    launcher.setConf("spark.yt.pyFiles", s"yt:/${spytPythonPath(spytVersion)}")
    launcher.setConf("spark.eventLog.dir", "ytEventLog:/" + eventLogPath)
    launcher.setConf("spark.rest.client.submissionIdFile", submissionIdFile.getAbsolutePath)
    launcher.setConf("spark.rest.client.submissionErrorFile", submissionErrorFile.getAbsolutePath)

    launcher.startApplication()
    getSubmissionId(submissionIdFile, submissionErrorFile)
  }

  def getStatus(id: String): DriverState = {
    DriverState.valueOf(getStringStatus(id))
  }

  def getStringStatus(id: String): String = {
    RestSubmissionClientWrapper.requestSubmissionStatus(client, id).driverState
  }

  private def getSubmissionId(file: File, errorFile: File): String = {
    while (!file.exists() && !errorFile.exists()) {
      log.debug(s"Waiting for submission id in file: $file")
      Thread.sleep((2 seconds).toMillis)
    }
    if (errorFile.exists()) {
      val message = FileUtils.readFileToString(errorFile)
      throw new RuntimeException(s"Spark submission finished with error: $message")
    }
    FileUtils.readFileToString(file)
  }

  private val SPARK_BASE_PATH = YPath.simple("//sys/spark")
  private val SPYT_BASE_PATH = SPARK_BASE_PATH.child("spyt")
  private val RELEASES_SUBDIR = "releases"
  private val SNAPSHOTS_SUBDIR = "snapshots"

  def spytJarPath(spytVersion: String): YPath = {
    getSpytVersionPath(spytVersion).child("spark-yt-data-source.jar")
  }

  def spytPythonPath(spytVersion: String): YPath = {
    getSpytVersionPath(spytVersion).child("spyt.zip")
  }

  private def getSpytVersionPath(spytVersion: String): YPath = {
    SPYT_BASE_PATH.child(versionSubdir(spytVersion)).child(spytVersion)
  }

  private def versionSubdir(version: String): String = {
    if (version.contains("SNAPSHOT") || version.contains("beta")) SNAPSHOTS_SUBDIR else RELEASES_SUBDIR
  }
}
