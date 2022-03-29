package ru.yandex.spark.launcher

import com.codahale.metrics.{Histogram, MetricRegistry, UniformReservoir}
import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.slf4j.{Logger, LoggerFactory}
import ru.yandex.inside.yt.kosher.common.GUID
import ru.yandex.spark.launcher.rest.AutoScalerMetricsServer
import ru.yandex.spark.yt.wrapper.client.YtClientConfiguration
import ru.yandex.spark.yt.wrapper.discovery.{DiscoveryService, OperationSet}
import ru.yandex.yt.ytclient.proxy.CompoundClient
import ru.yandex.yt.ytclient.proxy.request.GetOperation

import java.io.Closeable
import java.util.concurrent.{ScheduledThreadPoolExecutor, TimeUnit}
import scala.concurrent.duration.{Duration, DurationInt, FiniteDuration}
import scala.language.postfixOps
import scala.sys.process.ProcessLogger

trait AutoScaler {
  def apply(): Unit
}

object AutoScaler {
  private val log: Logger = LoggerFactory.getLogger(getClass)
  private object Metrics {
    val metricRegistry: MetricRegistry = new MetricRegistry()
    val userSlots = new Histogram(new UniformReservoir())
    val runningJobs = new Histogram(new UniformReservoir())
    val plannedJobs = new Histogram(new UniformReservoir())
    val maxWorkers = new Histogram(new UniformReservoir())
    val busyWorkers = new Histogram(new UniformReservoir())
    val waitingApps = new Histogram(new UniformReservoir())
    val coresTotal = new Histogram(new UniformReservoir())
    val coresUsed = new Histogram(new UniformReservoir())
    val maxAppWaitTimeMs = new Histogram(new UniformReservoir())
    metricRegistry.register("user_slots", userSlots)
    metricRegistry.register("running_jobs", runningJobs)
    metricRegistry.register("planned_jobs", plannedJobs)
    metricRegistry.register("max_workers", maxWorkers)
    metricRegistry.register("busy_workers", busyWorkers)
    metricRegistry.register("waiting_apps", waitingApps)
    metricRegistry.register("cores_total", coresTotal)
    metricRegistry.register("cores_used", coresUsed)
    metricRegistry.register("max_wait_app_time_ms", maxAppWaitTimeMs)
  }

  def registerMetrics(state: State, userSlots: Long): Unit = state match {
    case State(
    OperationState(_, runningJobs, plannedJobs),
    SparkState(maxWorkers, busyWorkers, waitingApps, coresTotal, coresUsed, maxAppAwaitTimeMs),
    ) =>
      Metrics.userSlots.update(userSlots)
      Metrics.runningJobs.update(runningJobs)
      Metrics.plannedJobs.update(plannedJobs)
      Metrics.maxWorkers.update(maxWorkers)
      Metrics.busyWorkers.update(busyWorkers)
      Metrics.waitingApps.update(waitingApps)
      Metrics.coresTotal.update(coresTotal)
      Metrics.coresUsed.update(coresUsed)
      Metrics.maxAppWaitTimeMs.update(maxAppAwaitTimeMs)
  }


  private val processLogger = new ProcessLogger {
    override def out(s: => String): Unit = log.debug(s)
    override def err(s: => String): Unit = log.error(s)
    override def buffer[T](f: => T): T = f
  }

  private val scheduler = new ScheduledThreadPoolExecutor(1,
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
              for {
                st <- state
                sl <- currentUserSlots.orElse(Some(st.operationState.maxJobs))
              } registerMetrics(st, sl)
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

  def start(autoScaler: AutoScaler, conf: Option[AutoScaler.Conf]): Closeable = {
    conf.map { c =>
      log.info(s"Starting autoscaler service: period = ${c.period}")
      val f = scheduler.scheduleAtFixedRate(() => {
        log.info("Autoscaler called")
        autoScaler()
      }, c.period.toNanos, c.period.toNanos, TimeUnit.NANOSECONDS)
      val metricsServer = c.metricsPort.map(p => AutoScalerMetricsServer.start(p, Metrics.metricRegistry))
      val r: Closeable = () => {
        f.cancel(false)
        metricsServer.foreach(_.close())
      }
      r
    }.getOrElse(() => ())
  }

  case class OperationState(maxJobs: Long, runningJobs: Long, plannedJobs: Long) {
    def freeJobs: Long = maxJobs - plannedJobs - runningJobs

    override def toString: String =
      s"OperationState(maxJobs=$maxJobs, runningJobs=$runningJobs, plannedJobs=$plannedJobs)"
  }

  case class SparkState(maxWorkers: Long, busyWorkers: Long, waitingApps: Long, coresTotal: Long, coresUsed: Long,
                        maxAppAwaitTimeMs: Long) {
    def freeWorkers: Long = maxWorkers - busyWorkers

    override def toString: String =
      s"SparkState(maxWorkers=$maxWorkers, busyWorkers=$busyWorkers, waitingApps=$waitingApps)"
  }

  case class State(operationState: OperationState, sparkState: SparkState)

  sealed trait Action
  case object DoNothing extends Action
  case class SetUserSlot(count: Long) extends Action

  case class Conf(period: FiniteDuration, metricsPort: Option[Int])

  object Conf {
    def apply(props: Map[String, String]): Option[Conf] =
      props.map(p => (p._1.toLowerCase, p._2.toLowerCase))
        .get("spark.autoscaler.enabled")
        .filter(_ == "true")
        .map { _ =>
          val period = props.get("spark.autoscaler.period")
            .map(p => Duration.create(p))
            .collect { case d: FiniteDuration => d }
            .getOrElse(1.second)
          val port = props.get("spark.autoscaler.metrics.port").map(_.toInt)
          Conf(period, port)
        }
  }
}
