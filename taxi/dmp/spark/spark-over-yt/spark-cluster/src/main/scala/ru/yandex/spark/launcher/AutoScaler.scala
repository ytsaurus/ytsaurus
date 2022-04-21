package ru.yandex.spark.launcher

import com.codahale.metrics.{Gauge, MetricRegistry}
import com.google.common.util.concurrent.ThreadFactoryBuilder
import org.slf4j.{Logger, LoggerFactory}
import ru.yandex.inside.yt.kosher.common.GUID
import ru.yandex.spark.launcher.rest.AutoScalerMetricsServer
import ru.yandex.spark.yt.wrapper.discovery.{DiscoveryService, OperationSet}
import ru.yandex.yt.ytclient.proxy.CompoundClient
import ru.yandex.yt.ytclient.proxy.request.UpdateOperationParameters.{ResourceLimits, SchedulingOptions}
import ru.yandex.yt.ytclient.proxy.request.{GetOperation, UpdateOperationParameters}

import java.io.Closeable
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.{ScheduledThreadPoolExecutor, TimeUnit}
import scala.concurrent.duration.{Duration, DurationInt, FiniteDuration}
import scala.language.postfixOps
import scala.util.control.NonFatal

trait AutoScaler {
  def apply(): Unit
}

object AutoScaler {
  private val log: Logger = LoggerFactory.getLogger(getClass)

  private object Metrics {
    private case class MetricsState(state: State, userSlots: Long)
    private val currentState: AtomicReference[MetricsState] = new AtomicReference()

    val metricRegistry: MetricRegistry = new MetricRegistry()

    def updateState(state: State, userSlots: Long): Unit = {
      if (currentState.get() == null) {
        registerMetrics()
      }
      currentState.set(MetricsState(state, userSlots))
    }

    def registerMetrics(): Unit = {
        metricRegistry.register("user_slots", new Gauge[Long] {
            override def getValue: Long = currentState.get().userSlots
        })

        metricRegistry.register("running_jobs", new Gauge[Long] {
            override def getValue: Long = currentState.get().state.operationState.runningJobs
        })

        metricRegistry.register("planned_jobs", new Gauge[Long] {
            override def getValue: Long = currentState.get().state.operationState.plannedJobs
        })

        metricRegistry.register("max_workers", new Gauge[Long] {
            override def getValue: Long = currentState.get().state.sparkState.maxWorkers
        })

        metricRegistry.register("busy_workers", new Gauge[Long] {
            override def getValue: Long = currentState.get().state.sparkState.busyWorkers
        })

        metricRegistry.register("waiting_apps", new Gauge[Long] {
            override def getValue: Long = currentState.get().state.sparkState.waitingApps
        })

        metricRegistry.register("cores_total", new Gauge[Long] {
            override def getValue: Long = currentState.get().state.sparkState.coresTotal
        })

        metricRegistry.register("cores_used", new Gauge[Long] {
            override def getValue: Long = currentState.get().state.sparkState.coresUsed
        })

        metricRegistry.register("max_wait_app_time_ms", new Gauge[Long] {
            override def getValue: Long = currentState.get().state.sparkState.maxAppAwaitTimeMs
        })
    }

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

  def clusterStateService(discoveryService: DiscoveryService, yt: CompoundClient): ClusterStateService =
    new ClusterStateService {
      val sparkStateService: SparkStateService =
        SparkStateService.sparkStateService(discoveryService.discoverAddress().get.webUiHostAndPort,
          discoveryService.discoverAddress().get.restHostAndPort)

      override def query: Option[State] = {
        log.debug("Querying cluster state")
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
              } Metrics.updateState(st, sl)
              state
            }
          case None =>
            log.error("Autoscaler not supported for single op mode")
            None
        }
      }

      override def setUserSlots(slots: Long): Unit = {
        val op = GUID.valueOf(discoveryService.operations().get.children.iterator.next())
        val req = new UpdateOperationParameters(op)
          .addSchedulingOptions("physical",
              new SchedulingOptions().setResourceLimits(new ResourceLimits().setUserSlots(slots)))
        log.debug(s"Updating operation parameters: $req")
        yt.updateOperationParameters(req).join()
      }
    }

  def autoScaleFunctionSliding(conf: Conf)(f: State => Action): (Seq[Action], State) => (Seq[Action], Action) = {
      case (window, newState) =>
        log.debug(s"windowSize=${conf.slidingWindowSize} window=$window newState=$newState")
        val expectedAction = f(newState)
        val slots = window.flatMap {
          case SetUserSlot(count) => Some(count)
          case DoNothing => None
        }
        val action = expectedAction match {
          case DoNothing => DoNothing
          case SetUserSlot(count) if slots.nonEmpty && slots.last > count => DoNothing
          case action => action
        }
        val newWindow =
          if (conf.slidingWindowSize < 1) Seq()
          else if (window.size < conf.slidingWindowSize) window :+ action
          else window.tail :+ action
        (newWindow, action)
  }

  def autoScaleFunctionBasic(conf: Conf): State => Action = {
    case State(operationState, sparkState) =>
      if (sparkState.freeWorkers > conf.maxFreeWorkers && sparkState.waitingApps == 0) // always reduce planned jobs
        SetUserSlot(Math.max(sparkState.busyWorkers, conf.maxFreeWorkers))
      else if (operationState.plannedJobs > 0) // wait till all jobs started
        DoNothing
      // no free workers all there are apps waiting
      else if (sparkState.freeWorkers == 0 || sparkState.waitingApps > 0) {
        if (operationState.freeJobs > 0 )     // increase jobs count if there are slots left
          SetUserSlot(Math.min(operationState.maxJobs, operationState.runningJobs + conf.slotIncrementStep))
        else // no free slots left
          DoNothing
      } else {
        DoNothing  // there are free workers left
      }
  }

  def build(conf: Conf, discoveryService: DiscoveryService, yt: CompoundClient): AutoScaler = {
    val stateService = AutoScaler.clusterStateService(discoveryService, yt)
    autoScaler(stateService, Seq[Action]())(autoScaleFunctionSliding(conf)(autoScaleFunctionBasic(conf)))
  }

  def autoScaler[T](clusterStateService: ClusterStateService, zero: T)(f: (T, State) => (T, Action)): AutoScaler = {
      val st = new AtomicReference(zero)
      () => {
        log.debug("Autoscaling...")
        clusterStateService.query
          .map { clusterState =>
            val (newState, action) = f(st.get, clusterState)
            st.set(newState)
            action match {
              case SetUserSlot(count) =>
                log.info(s"Updating user slots: $count")
                clusterStateService.setUserSlots(count)
              case DoNothing =>
                log.info("Nothing to do")
            }
          }
    }
  }

  def start(autoScaler: AutoScaler, c: Conf): Closeable = {
    log.info(s"Starting autoscaler service: period = ${c.period}")
    val f = scheduler.scheduleAtFixedRate(() => {
      try {
        log.debug("Autoscaler called")
        autoScaler()
      } catch {
        case NonFatal(e) =>
          log.error("Autoscaler failed", e)
      }
    }, c.period.toNanos, c.period.toNanos, TimeUnit.NANOSECONDS)
    val metricsServer = c.metricsPort.map(p => AutoScalerMetricsServer.start(p, Metrics.metricRegistry))
    val r: Closeable = () => {
      f.cancel(false)
      metricsServer.foreach(_.close())
    }
    r
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

  case class Conf(period: FiniteDuration, metricsPort: Option[Int], slidingWindowSize: Int,
                  maxFreeWorkers: Long, slotIncrementStep: Long)

  object Conf {
    def apply(props: Map[String, String]): Option[Conf] = {
      props.map(p => (p._1.toLowerCase, p._2.toLowerCase))
        .get("spark.autoscaler.enabled")
        .filter(_ == "true")
        .map { _ =>
          val period = props.get("spark.autoscaler.period")
            .map(p => Duration.create(p))
            .collect { case d: FiniteDuration => d }
            .getOrElse(1.second)
          Conf(
            period = period,
            metricsPort = props.get("spark.autoscaler.metrics.port").map(_.toInt),
            slidingWindowSize = props.get("spark.autoscaler.sliding_window_size").map(_.toInt).getOrElse(1),
            maxFreeWorkers = props.get("spark.autoscaler.max_free_workers").map(_.toLong).getOrElse(1),
            slotIncrementStep = props.get("spark.autoscaler.slots_increment_step").map(_.toLong).getOrElse(1)
          )
        }
    }
  }
}
