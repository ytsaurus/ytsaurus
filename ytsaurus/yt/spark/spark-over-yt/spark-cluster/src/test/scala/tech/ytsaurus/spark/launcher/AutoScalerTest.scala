package tech.ytsaurus.spark.launcher

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import tech.ytsaurus.spyt.HostAndPort
import tech.ytsaurus.spark.launcher.AutoScaler._
import tech.ytsaurus.spark.launcher.ClusterStateService.State
import tech.ytsaurus.spark.launcher.SparkStateService.{AppStats, MasterStats, WorkerInfo, WorkerStats}
import tech.ytsaurus.spyt.HostAndPort

import scala.concurrent.duration.DurationInt
import scala.util.Success

class AutoScalerTest extends AnyFlatSpec with Matchers  {
  behavior of "AutoScaler"

  val simpleConf: Conf = AutoScaler.Conf(1.minute, 0, 1, 1, 1, 1)
  val sliding1Conf: Conf = AutoScaler.Conf(1.minute, 1, 1, 1, 1, 1)
  val sliding3Conf: Conf = AutoScaler.Conf(1.minute, 3, 1, 1, 1, 1)

  it should "correctly assign action according to cluster state" in {
    val f = AutoScaler.autoScaleFunctionBasic(simpleConf)
    f(State(OperationState(10, 3, 0), SparkState(3, 3, 0, 12, 12, 0L), 3)) shouldEqual SetUserSlot(4)
    f(State(OperationState(10, 3, 0), SparkState(3, 2, 0, 12, 8, 0L), 3)) shouldEqual DoNothing
    f(State(OperationState(10, 3, 1), SparkState(3, 3, 0, 12, 12, 0L), 3)) shouldEqual SetUserSlot(4)
    f(State(OperationState(10, 5, 0), SparkState(3, 1, 0, 12, 4, 0L), 5)) shouldEqual SetUserSlot(4)
    f(State(OperationState(10, 5, 0), SparkState(3, 1, 1, 12, 8, 0L), 5)) shouldEqual DoNothing
    f(State(OperationState(10, 5, 0), SparkState(3, 0, 0, 12, 0, 0L), 5)) shouldEqual SetUserSlot(4)
    f(State(OperationState(10, 10, 0), SparkState(10, 10, 0, 40, 40, 0L), 10)) shouldEqual DoNothing
  }

  it should "use sliding average values for autoscaling" in {
      val f = AutoScaler.autoScaleFunctionBasic(simpleConf)
      val st1 = State(OperationState(10, 3, 0), SparkState(3, 3, 0, 12, 12, 0L), 3)
      val st2 = State(OperationState(10, 5, 0), SparkState(3, 1, 0, 12, 4, 0L), 5)

      val sliding = AutoScaler.autoScaleFunctionSliding(simpleConf)(f)
      sliding(Seq[Action](), st1) shouldEqual (Seq(), SetUserSlot(4))
      sliding(Seq[Action](), st2) shouldEqual (Seq(), SetUserSlot(4))

      val sliding1 = AutoScaler.autoScaleFunctionSliding(sliding1Conf)(f)
      sliding1(Seq[Action](), st1) shouldEqual (Seq(SetUserSlot(4)), SetUserSlot(4))
      sliding1(Seq(SetUserSlot(4)), st2) shouldEqual (Seq(SetUserSlot(4)), SetUserSlot(4))
      sliding1(Seq(DoNothing), st2) shouldEqual (Seq(SetUserSlot(4)), SetUserSlot(4))
      sliding1(Seq(SetUserSlot(1)), st1) shouldEqual (Seq(SetUserSlot(4)), SetUserSlot(4))

      val sliding3 = AutoScaler.autoScaleFunctionSliding(sliding3Conf)(f)
      sliding3(Seq[Action](), st1) shouldEqual (Seq(SetUserSlot(4)), SetUserSlot(4))
      sliding3(Seq(SetUserSlot(4)), st2) shouldEqual (Seq(SetUserSlot(4), SetUserSlot(4)), SetUserSlot(4))
      sliding3(Seq(SetUserSlot(4), DoNothing), st2) shouldEqual (Seq(SetUserSlot(4), DoNothing, SetUserSlot(4)),
        SetUserSlot(4))
      sliding3(Seq(SetUserSlot(4), DoNothing, DoNothing), st2) shouldEqual (Seq(DoNothing, DoNothing, SetUserSlot(4)),
        SetUserSlot(4))
      sliding3(Seq(DoNothing, DoNothing, DoNothing), st2) shouldEqual (Seq(DoNothing, DoNothing, SetUserSlot(4)),
          SetUserSlot(4))
      sliding3(Seq(DoNothing, DoNothing, SetUserSlot(1)), st1) shouldEqual (Seq(DoNothing, SetUserSlot(1),
        SetUserSlot(4)), SetUserSlot(4))
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

  it should "correctly parse app metrics" in {
    val metrics =
      """
        |metrics_application_loop_smoke_test_py_1647589577877_cores_Value{type="gauges"} 4
        |metrics_application_loop_smoke_test_py_1647589577877_runtime_ms_Value{type="gauges"} 121836
        |metrics_application_loop_smoke_test_py_1647589684254_cores_Value{type="gauges"} 0
        |metrics_application_loop_smoke_test_py_1647589684254_runtime_ms_Value{type="gauges"} 15458
        |metrics_jvm_G1_Old_Generation_count_Value{type="gauges"} 0
        |metrics_jvm_G1_Old_Generation_time_Value{type="gauges"} 0
        |metrics_jvm_G1_Young_Generation_count_Value{type="gauges"} 6
        |metrics_jvm_G1_Young_Generation_time_Value{type="gauges"} 172
        |metrics_jvm_direct_capacity_Value{type="gauges"} 84599582
        |metrics_jvm_direct_count_Value{type="gauges"} 85
        |""".stripMargin
    val srv = SparkStateService.sparkStateService(HostAndPort.fromString("localhost:8080"),
      HostAndPort.fromString("localhost:8081"))
    val res = srv.parseAppMetrics(metrics).map(_.toSet)
    res shouldEqual Success(Set(
      AppStats("loop_smoke_test_py_1647589577877", 4L, 121836.millis),
      AppStats("loop_smoke_test_py_1647589684254", 0L, 15458.millis),
    ))
  }
}
