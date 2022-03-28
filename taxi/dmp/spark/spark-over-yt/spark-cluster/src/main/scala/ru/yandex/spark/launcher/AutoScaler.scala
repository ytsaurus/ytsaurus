package ru.yandex.spark.launcher

import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.slf4j.LoggerFactory
import ru.yandex.inside.yt.kosher.common.GUID
import ru.yandex.spark.yt.wrapper.client.YtClientConfiguration
import ru.yandex.spark.yt.wrapper.discovery.{DiscoveryService, OperationSet}
import ru.yandex.yt.ytclient.proxy.CompoundClient
import ru.yandex.yt.ytclient.proxy.request.GetOperation

import java.util.concurrent.{ScheduledFuture, ScheduledThreadPoolExecutor, TimeUnit}
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.language.postfixOps
import scala.sys.process.ProcessLogger

trait AutoScaler {
  def apply(): Unit
}

object AutoScaler {
  private val log = LoggerFactory.getLogger(getClass)
  private val processLogger = new ProcessLogger {
    override def out(s: => String): Unit = log.debug(s)
    override def err(s: => String): Unit = log.error(s)
    override def buffer[T](f: => T): T = f
  }

  private val executor = new ScheduledThreadPoolExecutor(1,
    new ThreadFactoryBuilder()
        .setDaemon(true)
        .setUncaughtExceptionHandler(
          (_: Thread, ex: Throwable) => log.error(s"Uncaught exception in autoscaler thread", ex)
        )
        .setNameFormat("auto-scaler-%d")
        .build()
  )

  trait ClusterStateService {
    def query: Option[State]
    def setUserSlots(count: Long): Unit
  }

  def clusterStateService(discoveryService: DiscoveryService, yt: CompoundClient,
                          ytConfig: YtClientConfiguration): ClusterStateService =
    new ClusterStateService {
      val sparkStateService: SparkStateService =
        SparkStateService.sparkStateService(discoveryService.discoverAddress().get.webUiHostAndPort,
          discoveryService.discoverAddress().get.restHostAndPort)

      override def query: Option[State] = {
        discoveryService.operations() match {
          case Some(OperationSet(_, children, _)) =>
            if (children.isEmpty) {
              log.error("Autoscaler operation with empty children ops called")
              None
            } else {
              import ru.yandex.spark.yt.wrapper.discovery.CypressDiscoveryService.YTreeNodeExt
              val workersOp = children.iterator.next() // just single children op supported now
              log.info(s"Worker operation $workersOp")
              val opStats = yt.getOperation(new GetOperation(GUID.valueOf(workersOp))).join()
              val totalJobs = opStats.longAttribute("spec", "tasks", "workers", "job_count")
              val runningJobs = opStats.longAttribute("brief_progress", "jobs", "running")
              val currentUserSlots = opStats.longAttribute("runtime_parameters",
                "scheduling_options_per_pool_tree", "physical", "resource_limits", "user_slots")
              val operationState = for {
                total <- totalJobs
                running <- runningJobs
                slots <- currentUserSlots.orElse(Some(total))
              } yield OperationState(total, running, Math.max(0L, slots - running))
              log.debug(s"operation $workersOp state: $operationState slots: $currentUserSlots")
              val sparkState = sparkStateService.query
              log.info(s"spark state: $sparkState")
              val state = for {
                operation <- operationState
                spark <- sparkState.toOption
              } yield State(operation, spark)
              log.info(s"result state: $state")
              state
            }
          case None =>
            log.error("Autoscaler not supported for single op mode")
            None
        }
      }

      override def setUserSlots(slots: Long): Unit = {
        val op = discoveryService.operations().get.children.iterator.next()
        val command = Seq("yt", "update-op-parameters", op,
          s"""{"scheduling_options_per_pool_tree" = {"physical" = {"resource_limits" = { "user_slots" = $slots }}}}""")
        log.debug(s"SHELL: $command")
        import sys.process._
        val extraEnv = Seq(("YT_TOKEN", ytConfig.token), ("YT_PROXY", ytConfig.proxy))
        Process(command, None, extraEnv: _*)! processLogger
      }
    }

  def autoScaleFunction(maxFreeWorkers: Long, slotsIncrementStep: Long): State => Action = {
    case State(operationState, sparkState) =>
      if (sparkState.freeWorkers > maxFreeWorkers && sparkState.waitingApps == 0) // always reduce planned jobs
        SetUserSlot(Math.max(sparkState.busyWorkers, maxFreeWorkers))
      else if (operationState.plannedJobs > 0) // wait till all jobs started
        DoNothing
      // no free workers all there are apps waiting
      else if (sparkState.freeWorkers == 0 || sparkState.waitingApps > 0) {
        if (operationState.freeJobs > 0 )     // increase jobs count if there are slots left
          SetUserSlot(Math.min(operationState.maxJobs, operationState.runningJobs + slotsIncrementStep))
        else // no free slots left
          DoNothing
      } else {
        DoNothing  // there are free workers left
      }
  }

  def build(discoveryService: DiscoveryService, yt: CompoundClient, ytConfig: YtClientConfiguration,
            maxFreeWorkers: Long = 1, slotsIncrementStep: Long = 1): AutoScaler = {
    val stateService = AutoScaler.clusterStateService(discoveryService, yt, ytConfig)
    autoScaler(stateService)(autoScaleFunction(maxFreeWorkers, slotsIncrementStep))
  }

  def autoScaler(clusterStateService: ClusterStateService)(f: State => Action): AutoScaler = () => {
    log.debug("Autoscaling...")
    clusterStateService.query
      .map(
        f(_) match {
          case SetUserSlot(count) =>
            log.info(s"Updating user slots: $count")
            clusterStateService.setUserSlots(count)
          case DoNothing =>
            log.info("Nothing to do")
        }
      )
  }

  def start(autoScaler: AutoScaler, period: FiniteDuration = 1.minute): ScheduledFuture[_] = {
    log.info(s"Starting autoscaler service: period = $period")
    executor.scheduleAtFixedRate(() => {
      log.info("Autoscaler called")
      autoScaler()
    }, period.toNanos, period.toNanos, TimeUnit.NANOSECONDS)
  }

  case class OperationState(maxJobs: Long, runningJobs: Long, plannedJobs: Long) {
    def freeJobs: Long = maxJobs - plannedJobs - runningJobs

    override def toString: String =
      s"OperationState(maxJobs=$maxJobs, runningJobs=$runningJobs, plannedJobs=$plannedJobs)"
  }

  case class SparkState(maxWorkers: Long, busyWorkers: Long, waitingApps: Long) {
    def freeWorkers: Long = maxWorkers - busyWorkers

    override def toString: String = s"SparkState(maxWorkers=$maxWorkers, busyWorkers=$busyWorkers, waitingApps=$waitingApps)"
  }

  case class State(operationState: OperationState, sparkState: SparkState)

  sealed trait Action
  case object DoNothing extends Action
  case class SetUserSlot(count: Long) extends Action
}
