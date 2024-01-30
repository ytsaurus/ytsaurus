
package org.apache.spark.executor

import org.apache.spark.SparkEnv
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.rpc.RpcEnv

import java.net.URL

private[spark] class YTsaurusCoarseGrainedExecutorBackend(
  rpcEnv: RpcEnv,
  driverUrl: String,
  executorId: String,
  bindAddress: String,
  hostname: String,
  cores: Int,
  userClassPath: Seq[URL],
  env: SparkEnv,
  resourcesFile: Option[String],
  resourceProfile: ResourceProfile
) extends CoarseGrainedExecutorBackend(
  rpcEnv,
  driverUrl,
  executorId,
  bindAddress,
  hostname,
  cores,
  userClassPath,
  env,
  resourcesFile,
  resourceProfile) {

}

object YTsaurusCoarseGrainedExecutorBackend {
  def main(args: Array[String]): Unit = {
    val createFn: (RpcEnv, CoarseGrainedExecutorBackend.Arguments, SparkEnv, ResourceProfile) =>
      CoarseGrainedExecutorBackend = {
      case (rpcEnv, arguments, env, resourceProfile) =>
        val ytTaskJobIndex = System.getenv("YT_TASK_JOB_INDEX")
        new YTsaurusCoarseGrainedExecutorBackend(rpcEnv, arguments.driverUrl,
          ytTaskJobIndex, arguments.bindAddress, arguments.hostname, arguments.cores,
          arguments.userClassPath, env, arguments.resourcesFileOpt, resourceProfile)
    }
    val backendArgs = CoarseGrainedExecutorBackend.parseArguments(args,
      this.getClass.getCanonicalName.stripSuffix("$"))
    CoarseGrainedExecutorBackend.run(backendArgs, createFn)
    System.exit(0)
  }
}