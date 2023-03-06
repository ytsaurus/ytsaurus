package tech.ytsaurus.spark.launcher

import com.codahale.metrics.{Gauge, MetricRegistry}
import org.slf4j.{Logger, LoggerFactory}
import ClusterStateService.State
import tech.ytsaurus.spyt.wrapper.discovery.DiscoveryService
import tech.ytsaurus.client.CompoundClient
import tech.ytsaurus.spark.launcher.ClusterStateService.State

import java.io.Closeable
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}
import java.util.concurrent.{Executors, ScheduledThreadPoolExecutor, ThreadFactory, TimeUnit}
import scala.concurrent.duration.{Duration, DurationInt, FiniteDuration}
import scala.language.postfixOps
import scala.util.control.NonFatal

trait AutoScaler {
  def apply(): Unit
}

object AutoScaler {
  private val log: Logger = LoggerFactory.getLogger(getClass)

  private object Metrics {
    private val currentState: AtomicReference[State] = new AtomicReference()
    val metricRegistry: AtomicReference[MetricRegistry] = new AtomicReference()

    def updateState(state: State): Unit = {
      if (currentState.get() == null) {
        register(metricRegistry.get())
      }
      currentState.set(state)
    }

    def register(metricRegistry: MetricRegistry): Unit = {
        metricRegistry.register("autoscaler.user_slots", new Gauge[Long] {
            override def getValue: Long = currentState.get().userSlots
        })

        metricRegistry.register("autoscaler.running_jobs", new Gauge[Long] {
            override def getValue: Long = currentState.get().operationState.runningJobs
        })

        metricRegistry.register("autoscaler.planned_jobs", new Gauge[Long] {
            override def getValue: Long = currentState.get().operationState.plannedJobs
        })

        metricRegistry.register("autoscaler.max_workers", new Gauge[Long] {
            override def getValue: Long = currentState.get().sparkState.maxWorkers
        })

        metricRegistry.register("autoscaler.busy_workers", new Gauge[Long] {
            override def getValue: Long = currentState.get().sparkState.busyWorkers
        })

        metricRegistry.register("autoscaler.waiting_apps", new Gauge[Long] {
            override def getValue: Long = currentState.get().sparkState.waitingApps
        })

        metricRegistry.register("autoscaler.cores_total", new Gauge[Long] {
            override def getValue: Long = currentState.get().sparkState.coresTotal
        })

        metricRegistry.register("autoscaler.cores_used", new Gauge[Long] {
            override def getValue: Long = currentState.get().sparkState.coresUsed
        })

        metricRegistry.register("autoscaler.max_wait_app_time_ms", new Gauge[Long] {
            override def getValue: Long = currentState.get().sparkState.maxAppAwaitTimeMs
        })
    }
  }

  private val threadFactory = new ThreadFactory() {
    private val count: AtomicLong = new AtomicLong(0L)

    override def newThread(runnable: Runnable): Thread = {
      val thread = Executors.defaultThreadFactory().newThread(runnable)
      thread.setDaemon(true)
      thread.setUncaughtExceptionHandler(
        (_: Thread, ex: Throwable) => log.error(s"Uncaught exception in autoscaler thread", ex)
      )
      thread.setName("auto-scaler-%s".format(count.getAndIncrement))
      thread
    }
  }
  private val scheduler = new ScheduledThreadPoolExecutor(1, threadFactory)

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
    case State(operationState, sparkState, slots) =>
      val expectedWorkers = sparkState.busyWorkers + operationState.plannedJobs
      log.debug(s"Expected workers count: $expectedWorkers," +
        s" freeWorkers=${sparkState.freeWorkers}, freeJobs=${operationState.freeJobs}")
      if (sparkState.freeWorkers > conf.maxFreeWorkers && sparkState.waitingApps == 0) { // always reduce planned jobs
        val optimalSlots = Math.max(sparkState.busyWorkers, conf.maxFreeWorkers)
        if (optimalSlots < slots)
          SetUserSlot(slots - conf.slotDecrementStep)
        else
          DoNothing
      }
      // no free workers
      else if (sparkState.freeWorkers < conf.minFreeWorkers && operationState.freeJobs > 0)
        SetUserSlot(Math.min(operationState.maxJobs, slots + conf.slotIncrementStep))
      else
        DoNothing  // there are free workers left
  }

  def build(conf: Conf, discoveryService: DiscoveryService, yt: CompoundClient): AutoScaler = {
    val stateService = ClusterStateService(discoveryService, yt)
    autoScaler(stateService, Seq[Action]())(autoScaleFunctionSliding(conf)(autoScaleFunctionBasic(conf)))
  }

  def jobsToStop(clusterStateService: ClusterStateService, currentSlots: Long, newSlots: Long): Set[String] =
    if (newSlots < currentSlots)
      clusterStateService.idleJobs.toSet.take((currentSlots - newSlots).toInt)
    else
      Set()


  def autoScaler[T](clusterStateService: ClusterStateService, zero: T)(f: (T, State) => (T, Action)): AutoScaler = {
      val st = new AtomicReference(zero)
      () => {
        log.debug("Autoscaling...")
        clusterStateService.query
          .foreach { clusterState =>
            val (newState, action) = f(st.get, clusterState)
            st.set(newState)
            Metrics.updateState(clusterState)
            action match {
              case SetUserSlot(count) =>
                val toStop = jobsToStop(clusterStateService, clusterState.userSlots, count)
                log.info(s"Updating user slots: $count, stop following jobs: [${toStop.mkString(", ")}]")
                clusterStateService.setUserSlots(count, toStop)
              case DoNothing =>
                log.info("Nothing to do")
            }
          }
    }
  }

  def start(autoScaler: AutoScaler, c: Conf, metricRegistry: MetricRegistry): Closeable = {
    log.info(s"Starting autoscaler service: period = ${c.period}")
    Metrics.metricRegistry.set(metricRegistry)
    val f = scheduler.scheduleAtFixedRate(() => {
      try {
        log.debug("Autoscaler called")
        autoScaler()
      } catch {
        case NonFatal(e) =>
          log.error("Autoscaler failed", e)
      }
    }, c.period.toNanos, c.period.toNanos, TimeUnit.NANOSECONDS)
    () => {
      f.cancel(false)
    }
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

  sealed trait Action
  case object DoNothing extends Action
  case class SetUserSlot(count: Long) extends Action

  case class Conf(period: FiniteDuration, slidingWindowSize: Int, maxFreeWorkers: Long, minFreeWorkers: Long,
                  slotIncrementStep: Long, slotDecrementStep: Long)

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
            slidingWindowSize = props.get("spark.autoscaler.sliding_window_size").map(_.toInt).getOrElse(1),
            maxFreeWorkers = props.get("spark.autoscaler.max_free_workers").map(_.toLong).getOrElse(1),
            minFreeWorkers = props.get("spark.autoscaler.min_free_workers").map(_.toLong).getOrElse(1),
            slotIncrementStep = props.get("spark.autoscaler.slots_increment_step").map(_.toLong).getOrElse(1),
            slotDecrementStep = props.get("spark.autoscaler.slots_decrement_step").map(_.toLong).getOrElse(1)
          )
        }
    }
  }
}
