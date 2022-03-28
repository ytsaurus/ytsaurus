package ru.yandex.spark.launcher

import com.google.common.net.HostAndPort
import org.scalatest.{FlatSpec, Matchers}
import ru.yandex.spark.launcher.AutoScaler._
import ru.yandex.spark.launcher.SparkStateService.{MasterStats, WorkerInfo, WorkerStats}

import scala.util.Success

//noinspection UnstableApiUsage
class AutoScalerTest extends FlatSpec with Matchers  {
  behavior of "AutoScaler"

  it should "correctly assign action according to cluster state" in {
    val f = AutoScaler.autoScaleFunction(1, 1)
    f(State(OperationState(10, 3, 0), SparkState(3, 3, 0))) shouldEqual SetUserSlot(4)
    f(State(OperationState(10, 3, 0), SparkState(3, 2, 0))) shouldEqual DoNothing
    f(State(OperationState(10, 3, 1), SparkState(3, 3, 0))) shouldEqual DoNothing
    f(State(OperationState(10, 5, 0), SparkState(3, 1, 0))) shouldEqual SetUserSlot(1)
    f(State(OperationState(10, 5, 0), SparkState(3, 1, 1))) shouldEqual SetUserSlot(6)
    f(State(OperationState(10, 5, 0), SparkState(3, 0, 0))) shouldEqual SetUserSlot(1)
    f(State(OperationState(10, 10, 0), SparkState(10, 10, 0))) shouldEqual DoNothing
  }

  it should "correctly parse worker metrics" in {
    val metrics =
      """
        |metrics_worker_coresFree_Value{type="gauges"} 2
        |metrics_worker_coresUsed_Value{type="gauges"} 4
        |metrics_worker_executors_Value{type="gauges"} 1
        |metrics_worker_memFree_MB_Value{type="gauges"} 0
        |metrics_worker_memUsed_MB_Value{type="gauges"} 4096
        |metrics_worker_custom_disk_Used_Bytes_Value{type="gauges"} 274803614
        |metrics_worker_custom_tmpfs_Limit_Free_Bytes_Value{type="gauges"} 9099825141
        |metrics_worker_custom_tmpfs_Limit_Used_Bytes_Value{type="gauges"} 888795147
        |metrics_worker_custom_tmpfs_Total_Free_Bytes_Value{type="gauges"} 9099825141
        |metrics_worker_custom_tmpfs_Total_Used_Bytes_Value{type="gauges"} 5183762443
        |""".stripMargin
    val srv = SparkStateService.sparkStateService(HostAndPort.fromString("localhost:8080"),
        HostAndPort.fromString("localhost:8081"))
    val res = srv.parseWorkerMetrics(metrics, WorkerInfo("id", "localhost", 8082, 6, 1024, "test", alive = true, Map()))
    res shouldEqual Success(WorkerStats(6, 4, 1, 0, 4096))
    res.map(_.coresFree) shouldEqual Success(2)
  }

  it should "correctly parse master metrics" in {
    val metrics =
      """
        |metrics_master_aliveWorkers_Value{type="gauges"} 3
        |metrics_master_apps_Value{type="gauges"} 2
        |metrics_master_waitingApps_Value{type="gauges"} 1
        |metrics_master_workers_Value{type="gauges"} 4
        |""".stripMargin
    val srv = SparkStateService.sparkStateService(HostAndPort.fromString("localhost:8080"),
      HostAndPort.fromString("localhost:8081"))
    val res = srv.parseMasterMetrics(metrics)
    res shouldEqual Success(MasterStats(3, 4, 2, 1))
  }
}
