package ru.yandex.spark.yt.test

import com.github.dockerjava.api.model.{Container, Network}
import com.github.dockerjava.core.DefaultDockerClientConfig
import com.whisk.docker.impl.dockerjava.{Docker, DockerJavaExecutorFactory}
import com.whisk.docker.{DockerFactory, DockerKit}
import io.circe.yaml.parser
import io.circe.{JsonObject, ParsingFailure}
import org.slf4j.LoggerFactory

import java.io.InputStreamReader
import scala.collection.JavaConverters._
import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{Await, Future}
import scala.language.postfixOps

trait DockerCompose extends DockerKit {
  private val log = LoggerFactory.getLogger(getClass)

  private val docker = new Docker(DefaultDockerClientConfig.createDefaultConfigBuilder().build())
  override implicit val dockerFactory: DockerFactory = new DockerJavaExecutorFactory(docker)
  override val StartContainersTimeout: FiniteDuration = 5 minutes

  protected def networkName: String

  protected def shutdownHook(): Unit = {}

  protected def cleanPrevEnvironment(): Unit = {
    if (!reuseDocker) deleteNetwork(networkName)
  }

  @volatile private var initialized = false

  protected val reuseDocker: Boolean = readConfig("reuseDocker").flatMap(_.asBoolean).getOrElse(false)

  private def allContainers: Seq[Container] = docker.client.listContainersCmd().withShowAll(true).exec().asScala

  private def runningNetworks: Seq[Network] = docker.client.listNetworksCmd().exec().asScala

  private def readConfig: JsonObject = {
    val in = getClass.getResourceAsStream("/test.yaml")
    parser.parse(new InputStreamReader(in)) match {
      case Left(ParsingFailure(message, underlying)) =>
        throw new IllegalArgumentException(s"Unable to parse yaml config, $message", underlying)
      case Right(value) => value.asObject
        .getOrElse(throw new IllegalArgumentException(s"Unable to parse yaml config"))
    }
  }


  def init(): Unit = synchronized {
    if (!initialized) {
      val ts = System.currentTimeMillis()
      cleanPrevEnvironment() // if previous run was killed and didn't stop containers and network
      log.info(s"Cleaned previous docker env in ${System.currentTimeMillis() - ts}ms")
      val ts2 = System.currentTimeMillis()
      startAllOrFail()
      log.info(s"Started docker env in ${System.currentTimeMillis() - ts2}ms")
      initialized = true
    }
  }

  // copypaste of parent method, close yt client before docker container shutdown
  override def startAllOrFail(): Unit = {
    Await.result(containerManager.pullImages(), PullImagesTimeout)
    createNetwork(networkName)
    val allRunning: Boolean = try {
      val future = containerManager.initReadyAll(StartContainersTimeout).map(_.map(_._2).forall(identity))
      sys.addShutdownHook {
        shutdown()
      }
      Await.result(future, StartContainersTimeout)
    } catch {
      case e: Exception =>
        log.error("Exception during container initialization", e)
        false
    }
    if (!allRunning) {
      shutdown()
      throw new RuntimeException("Cannot run all required containers")
    }
  }

  def shutdown(): Unit = {
    shutdownHook()

    if (!reuseDocker) {
      log.info("Clean docker environment")
      val future = Future.traverse(containerManager.states)(_.remove(force = true, removeVolumes = true))
        .map(_ => ())
      Await.ready(future, StopContainersTimeout)
      log.info("Stopped docker containers")
      deleteNetwork(networkName)
      dockerExecutor.close()
    }
  }

  private def createNetwork(name: String): Unit = {
    if (!reuseDocker || networkRunning(name).isEmpty) {
      val ts = System.currentTimeMillis()
      docker.client
        .createNetworkCmd()
        .withName(name)
        .withAttachable(true)
        .withDriver("bridge")
        .exec()
        .getId
      log.info(s"Created docker network $networkName in ${System.currentTimeMillis() - ts}ms")
    } else {
      log.info(s"Network $networkName is already exists, reusing")
    }
  }

  private def deleteNetwork(name: String): Unit = {
    networkRunning(name).foreach { network =>
      docker.client
        .removeNetworkCmd(network.getId)
        .exec()
      log.info(s"Removed network $name")
    }
  }

  protected def isRunning(c: Container): Boolean = {
    !c.getStatus.startsWith("Exited")
  }

  protected def isRunning(name: String): Boolean = {
    allContainers.find(hasName(_, name)).exists(isRunning)
  }

  private def hasName(c: Container, name: String): Boolean = c.getNames.contains(s"/$name")

  private def networkRunning(name: String): Option[Network] = {
    runningNetworks.find(_.getName == name)
  }

  protected def stopAndRmContainer(name: String): Unit = {
    allContainers.find(hasName(_, name)).foreach { c =>
      if (isRunning(c) && !reuseDocker) {
        docker.client
          .stopContainerCmd(name)
          .exec()
        log.info(s"Stopped container $name")
      }

      if (!isRunning(c)) {
        docker.client
          .removeContainerCmd(name)
          .exec()
        log.info(s"Removed container $name")
      }
    }
  }
}
