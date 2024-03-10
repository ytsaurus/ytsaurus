package org.apache.spark.deploy.worker

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.deploy.worker.Worker.{ENDPOINT_NAME, SYSTEM_NAME, log, startRpcEnvAndEndpoint}
import org.apache.spark.deploy.worker.ui.WorkerWebUI
import org.apache.spark.internal.{Logging, config}
import org.apache.spark.internal.config.Worker.SPARK_WORKER_RESOURCE_FILE
import org.apache.spark.rpc.{RpcAddress, RpcEnv}
import org.apache.spark.util.{SparkUncaughtExceptionHandler, Utils}
import tech.ytsaurus.spark.launcher.AddressUtils

class YtWorker(rpcEnv: RpcEnv,
               webUiPort: Int,
               cores: Int,
               memory: Int,
               masterRpcAddresses: Array[RpcAddress],
               endpointName: String,
               workDir: String,
               conf: SparkConf,
               securityMgr: SecurityManager,
               resourceFileOpt: Option[String]
              ) extends Worker(rpcEnv, webUiPort, cores, memory, masterRpcAddresses, endpointName,
  workDir, conf, securityMgr, resourceFileOpt) {

  override def onStart(): Unit = {
    val baseClass = this.getClass.getSuperclass
    val publicAddressField = baseClass.getDeclaredField("org$apache$spark$deploy$worker$Worker$$publicAddress")
    publicAddressField.setAccessible(true)
    val rawHost = publicAddressField.get(this).asInstanceOf[String]
    val publicAddress = if (rawHost.contains(":")) s"[$rawHost]" else rawHost
    publicAddressField.set(this, publicAddress)
    publicAddressField.setAccessible(false)

    super.onStart()

    val webUiField = baseClass.getDeclaredField("org$apache$spark$deploy$worker$Worker$$webUi")
    webUiField.setAccessible(true)
    val webUi = webUiField.get(this).asInstanceOf[WorkerWebUI]
    webUiField.setAccessible(false)
    AddressUtils.writeAddressToFile("worker", rpcEnv.address.host, webUi.boundPort, None, None)
  }
}

object YtWorker extends Logging {

  def main(argStrings: Array[String]): Unit = {
    // almost exact copy of org.apache.spark.deploy.worker.Worker main method with slight changes
    Thread.setDefaultUncaughtExceptionHandler(new SparkUncaughtExceptionHandler(
      exitOnUncaughtException = false))
    Utils.initDaemon(log)
    val conf = new SparkConf
    val args = new WorkerArguments(argStrings, conf)
    val systemName = SYSTEM_NAME
    val securityMgr = new SecurityManager(conf)
    val rpcEnv = RpcEnv.create(systemName, args.host, args.port, conf, securityMgr)
    val masterAddresses = args.masters.map(RpcAddress.fromSparkURL)
    rpcEnv.setupEndpoint(ENDPOINT_NAME, new YtWorker(rpcEnv, args.webUiPort, args.cores, args.memory,
      masterAddresses, ENDPOINT_NAME, args.workDir, conf, securityMgr, conf.get(SPARK_WORKER_RESOURCE_FILE)))

    val externalShuffleServiceEnabled = conf.get(config.SHUFFLE_SERVICE_ENABLED)
    val sparkWorkerInstances = scala.sys.env.getOrElse("SPARK_WORKER_INSTANCES", "1").toInt
    require(!externalShuffleServiceEnabled || sparkWorkerInstances <= 1,
      "Starting multiple workers on one host is failed because we may launch no more than one " +
        "external shuffle service on each host, please set spark.shuffle.service.enabled to " +
        "false or set SPARK_WORKER_INSTANCES to 1 to resolve the conflict.")
    rpcEnv.awaitTermination()
  }
}