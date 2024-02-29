
package org.apache.spark.deploy.ytsaurus

import org.apache.spark.internal.config.ConfigBuilder

object Config {
  val GLOBAL_CONFIG_PATH = ConfigBuilder("spark.ytsaurus.config.global.path")
    .doc("Path to global Spark configuration for the whole YTsaurus cluster")
    .version("3.2.2")
    .stringConf
    .createWithDefault("//home/spark/conf/global")

  val RELEASE_CONFIG_PATH = ConfigBuilder("spark.ytsaurus.config.releases.path")
    .doc("Root path for SPYT releases configuration")
    .version("3.2.2")
    .stringConf
    .createWithDefault("//home/spark/conf/releases")

  val RELEASE_SPYT_PATH = ConfigBuilder("spark.ytsaurus.spyt.releases.path")
    .doc("Root path for SPYT releases configuration")
    .version("3.2.2")
    .stringConf
    .createWithDefault("//home/spark/spyt/releases")

  val LAUNCH_CONF_FILE = ConfigBuilder("spark.ytsaurus.config.launch.file")
    .doc("SPYT release configuration file name")
    .version("3.2.2")
    .stringConf
    .createWithDefault("spark-launch-conf")

  val SPYT_VERSION = ConfigBuilder("spark.ytsaurus.spyt.version")
    .doc("SPYT version to use on cluster")
    .version("3.2.2")
    .stringConf
    .createOptional

  val MAX_DRIVER_FAILURES = ConfigBuilder("spark.ytsaurus.driver.maxFailures")
    .doc("Maximum driver task failures before operation failure")
    .version("3.2.2")
    .intConf
    .createWithDefault(5)

  val MAX_EXECUTOR_FAILURES = ConfigBuilder("spark.ytsaurus.executor.maxFailures")
    .doc("Maximum executor task failures before operation failure")
    .version("3.2.2")
    .intConf
    .createWithDefault(10)

  val EXECUTOR_OPERATION_SHUTDOWN_DELAY = ConfigBuilder("spark.ytsaurus.executor.operation.shutdown.delay")
    .doc("Time for executors to shutdown themselves before terminating the executor operation, milliseconds")
    .version("3.2.2")
    .longConf
    .createWithDefault(10000)

  val DRIVER_OPERATION_ID = "spark.ytsaurus.driver.operation.id"
  val EXECUTOR_OPERATION_ID = "spark.ytsaurus.executor.operation.id"
  val SPARK_PRIMARY_RESOURCE = "spark.ytsaurus.primary.resource"
}
