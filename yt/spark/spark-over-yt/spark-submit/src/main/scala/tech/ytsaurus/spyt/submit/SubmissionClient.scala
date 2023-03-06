package tech.ytsaurus.spyt.submit

import io.netty.channel.DefaultEventLoopGroup
import org.apache.commons.io.FileUtils
import org.apache.spark.deploy.rest._
import org.apache.spark.launcher.InProcessLauncher
import org.slf4j.LoggerFactory
import tech.ytsaurus.spyt.wrapper.client.{YtClientConfiguration, YtClientProvider}
import tech.ytsaurus.spyt.wrapper.config.Utils.{parseRemoteConfig, remoteGlobalConfigPath, remoteVersionConfigPath}
import tech.ytsaurus.spyt.wrapper.discovery.CypressDiscoveryService
import tech.ytsaurus.core.cypress.YPath

import java.io.File
import java.util.UUID
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{Executors, ThreadFactory}
import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

class SubmissionClient(proxy: String,
                       discoveryPath: String,
                       spytVersion: String,
                       user: String,
                       token: String) {
  private val log = LoggerFactory.getLogger(getClass)

  private val eventLogPath = CypressDiscoveryService.eventLogPath(discoveryPath)

  private val cluster = new AtomicReference[SparkCluster](SparkCluster.get(proxy, discoveryPath, user, token))

  private val threadFactory = new ThreadFactory() {
    override def newThread(runnable: Runnable): Thread = {
      val thread = Executors.defaultThreadFactory().newThread(runnable)
      thread.setDaemon(true)
      thread
    }
  }
  private val loop = new DefaultEventLoopGroup(1, threadFactory).next()

  def newLauncher(): InProcessLauncher = new InProcessLauncher()

  // for Java
  def submit(launcher: InProcessLauncher): Try[String] = {
    submit(launcher, RetryConfig())
  }

  private def addConf(launcher: InProcessLauncher, config: Map[String, String]): Unit = {
    config.foldLeft(launcher) { case (res, (key, value)) => res.setConf(key, value) }
  }

  def submit(launcher: InProcessLauncher,
             retryConfig: RetryConfig): Try[String] = {
    val yt = YtClientProvider.ytClient(YtClientConfiguration.default(proxy, user, token))
    val remoteGlobalConfig = parseRemoteConfig(remoteGlobalConfigPath, yt)
    val remoteVersionConfig = parseRemoteConfig(remoteVersionConfigPath(cluster.get().version), yt)

    val jarCachingEnabled = remoteVersionConfig.get("spark.yt.jarCaching")
      .orElse(remoteGlobalConfig.get("spark.yt.jarCaching"))
      .exists(_.toBoolean)

    launcher.setDeployMode("cluster")

    addConf(launcher, remoteGlobalConfig)
    addConf(launcher, remoteVersionConfig)

    launcher.setConf("spark.master.rest.enabled", "true")
    launcher.setConf("spark.master.rest.failover", "false")
    launcher.setConf("spark.rest.client.awaitTermination.enabled", "false")

    launcher.setConf("spark.hadoop.yt.proxy", proxy)
    launcher.setConf("spark.hadoop.yt.user", user)
    launcher.setConf("spark.hadoop.yt.token", token)

    launcher.setConf("spark.yt.version", spytVersion)
    launcher.setConf("spark.yt.jars", wrapCachedJar(s"yt:/${spytJarPath(spytVersion)}", jarCachingEnabled))
    launcher.setConf("spark.yt.pyFiles", wrapCachedJar(s"yt:/${spytPythonPath(spytVersion)}", jarCachingEnabled))
    launcher.setConf("spark.eventLog.dir", "ytEventLog:/" + eventLogPath)

    if (useDedicatedDriverOp) {
      launcher.setConf("spark.driver.resource.driverop.amount", "1")
    }

    submitInner(launcher, retryConfig)
  }

  def getStatus(id: String): DriverState = {
    tryToGetStatus(id).getOrElse(DriverState.UNDEFINED)
  }

  def getApplicationStatus(driverId: String): ApplicationState = {
    tryToGetApplicationStatus(driverId).getOrElse(ApplicationState.UNDEFINED)
  }

  def getStringStatus(id: String): String = {
    getStatus(id).name()
  }

  def getStringApplicationStatus(driverId: String): String = {
    getApplicationStatus(driverId).name()
  }

  private def tryToGetApplicationStatus(id: String): Try[ApplicationState] = {
    val res = Try {
      val responseAppId = RestSubmissionClientWrapper.requestApplicationId(cluster.get().client, id)
      val responseAppStatus = RestSubmissionClientWrapper.requestApplicationStatus(cluster.get().client, responseAppId.appId)
      if (responseAppStatus.success) {
        ApplicationState.valueOf(responseAppStatus.appState)
      } else {
        ApplicationState.UNKNOWN
      }
    }
    if (res.isFailure) {
      log.warn(s"Failed to get status of application $id")
      forceClusterUpdate()
    }
    res
  }

  private def tryToGetStatus(id: String): Try[DriverState] = {
    val res = Try {
      val response = RestSubmissionClientWrapper.requestSubmissionStatus(cluster.get().client, id)
      if (response.success) {
        DriverState.valueOf(response.driverState)
      } else {
        DriverState.UNKNOWN // master restarted and can't find this submission id
      }
    }
    if (res.isFailure) {
      log.warn(s"Failed to get status of submission $id")
      forceClusterUpdate()
    }
    res
  }

  def getActiveDrivers: Seq[String] = {
    getActiveDriverInfos.map(_.driverId)
  }

  def getActiveDriverInfos: Seq[DriverInfo] = {
    getAllDriverInfos.filter(d => !DriverState.valueOf(d.status).isFinal)
  }

  def getCompletedDrivers: Seq[String] = {
    getCompletedDriverInfos.map(_.driverId)
  }

  def getCompletedDriverInfos: Seq[DriverInfo] = {
    getAllDriverInfos.filter(d => DriverState.valueOf(d.status).isFinal)
  }

  def getAllDrivers: Seq[String] = {
    getAllDriverInfos.map(_.driverId)
  }

  def getAllDriverInfos: Seq[DriverInfo] = {
    val response = MasterClient.allDrivers(cluster.get().masterHostAndPort)
    if (response.isFailure) {
      log.warn(s"Failed to get list of drivers")
      log.warn(response.failed.get.toString)
    }
    response.getOrElse(Nil)
  }

  def useDedicatedDriverOp: Boolean =
    MasterClient.activeWorkers(cluster.get().masterHostAndPort) match {
      case Failure(err) =>
        log.warn(s"Failed to query list of active workers", err)
        false
      case Success(workers) =>
        workers.exists(_.isDriverOp)
    }

  def kill(id: String): Boolean = {
    val response = RestSubmissionClientWrapper.killSubmission(cluster.get().client, id)
    response.success.booleanValue()
  }

  case class SubmissionFiles(id: File, error: File) {
    def delete(): Unit = {
      if (id.exists()) id.delete()
      if (error.exists()) error.delete()
    }
  }

  private def updateCluster(): Unit = {
    log.debug(s"Update cluster addresses from $discoveryPath")
    cluster.set(SparkCluster.get(proxy, discoveryPath, user, token))
  }

  private def forceClusterUpdate(): Unit = {
    loop.submit(new Runnable {
      override def run(): Unit = updateCluster()
    })
  }

  private def configureCluster(launcher: InProcessLauncher): Unit = {
    val clusterValue = cluster.get()
    launcher.setMaster(clusterValue.master)
    launcher.setConf("spark.rest.master", clusterValue.masterRest)
    launcher.setConf("spark.yt.cluster.version", clusterValue.version)
  }

  private def prepareSubmissionFiles(launcher: InProcessLauncher): SubmissionFiles = {
    val fileName = s"spark-submission-${UUID.randomUUID()}"
    val idFile = new File(FileUtils.getTempDirectory, s"$fileName-id")
    val errorFile = new File(FileUtils.getTempDirectory, s"$fileName-error")
    launcher.setConf("spark.rest.client.submissionIdFile", idFile.getAbsolutePath)
    launcher.setConf("spark.rest.client.submissionErrorFile", errorFile.getAbsolutePath)
    SubmissionFiles(idFile, errorFile)
  }

  @tailrec
  private def submitInner(launcher: InProcessLauncher,
                          retryConfig: RetryConfig = RetryConfig(),
                          retry: Int = 1): Try[String] = {
    val submissionFiles = prepareSubmissionFiles(launcher)
    val submissionId = Try {
      configureCluster(launcher)
      launcher.startApplication()
      getSubmissionId(submissionFiles)
    }
    submissionFiles.delete()

    submissionId match {
      case Failure(e) if !retryConfig.enableRetry =>
        forceClusterUpdate()
        Failure(new RuntimeException("Failed to submit job and retry is disabled", e))
      case Failure(e) if retry >= retryConfig.retryLimit =>
        forceClusterUpdate()
        Failure(new RuntimeException(s"Failed to submit job and retry limit ${retryConfig.retryLimit} exceeded", e))
      case Failure(e) =>
        forceClusterUpdate()
        log.warn("Failed to submit job and retry is enabled")
        log.warn(e.getMessage)
        log.info(s"Retry to submit job in ${retryConfig.retryInterval.toCoarsest}")
        Thread.sleep(retryConfig.retryInterval.toMillis)
        submitInner(launcher, retryConfig, retry + 1)
      case success => success
    }
  }

  @tailrec
  private def waitSubmissionResultFile(submissionFiles: SubmissionFiles, retryCount: Int = 4): Unit = {
    if (!submissionFiles.id.exists() && !submissionFiles.error.exists()) {
      if (retryCount <= 0) {
        throw new RuntimeException(s"Files with submission result were not created")
      } else {
        log.warn(s"Waiting for submission id in file: ${submissionFiles.id}")
        Thread.sleep((5 seconds).toMillis)
        waitSubmissionResultFile(submissionFiles, retryCount - 1)
      }
    }
  }

  private def getSubmissionId(submissionFiles: SubmissionFiles): String = {
    waitSubmissionResultFile(submissionFiles)
    if (submissionFiles.error.exists()) {
      val message = FileUtils.readFileToString(submissionFiles.error)
      throw new RuntimeException(s"Spark submission finished with error: $message")
    }
    FileUtils.readFileToString(submissionFiles.id)
  }

  private val SPARK_BASE_PATH = YPath.simple("//home/spark")
  private val SPYT_BASE_PATH = SPARK_BASE_PATH.child("spyt")
  private val RELEASES_SUBDIR = "releases"
  private val SNAPSHOTS_SUBDIR = "snapshots"

  private def wrapCachedJar(path: String, jarCachingEnabled: Boolean): String = {
    if (jarCachingEnabled && path.startsWith("yt:/")) {
      "ytCached:/" + path.drop(4)
    } else {
      path
    }
  }

  private def spytJarPath(spytVersion: String): YPath = {
    getSpytVersionPath(spytVersion).child("spark-yt-data-source.jar")
  }

  private def spytPythonPath(spytVersion: String): YPath = {
    getSpytVersionPath(spytVersion).child("spyt.zip")
  }

  private def getSpytVersionPath(spytVersion: String): YPath = {
    SPYT_BASE_PATH.child(versionSubdir(spytVersion)).child(spytVersion)
  }

  private def versionSubdir(version: String): String = {
    if (version.contains("SNAPSHOT") || version.contains("beta") || version.contains("dev")) SNAPSHOTS_SUBDIR
    else RELEASES_SUBDIR
  }
}
