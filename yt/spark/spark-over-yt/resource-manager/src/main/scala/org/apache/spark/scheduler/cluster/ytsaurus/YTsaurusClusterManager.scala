
package org.apache.spark.scheduler.cluster.ytsaurus

import org.apache.spark.SparkContext

import org.apache.spark.deploy.ytsaurus.YTsaurusUtils
import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.{ExternalClusterManager, SchedulerBackend, TaskScheduler, TaskSchedulerImpl}

private[spark] class YTsaurusClusterManager extends ExternalClusterManager with Logging {

  override def canCreate(masterURL: String): Boolean = {
    masterURL.startsWith(YTsaurusUtils.URL_PREFIX)
  }

  override def createTaskScheduler(sc: SparkContext, masterURL: String): TaskScheduler = {
    new TaskSchedulerImpl(sc)
  }

  override def createSchedulerBackend(
    sc: SparkContext,
    masterURL: String,
    scheduler: TaskScheduler): SchedulerBackend = {
    logInfo("Creating YTsaurus scheduler backend")

    val ytProxy = YTsaurusUtils.parseMasterUrl(masterURL)

    val operationManager = YTsaurusOperationManager.create(ytProxy, sc.conf)

    new YTsaurusSchedulerBackend(scheduler.asInstanceOf[TaskSchedulerImpl], sc, operationManager)
  }

  override def initialize(scheduler: TaskScheduler, backend: SchedulerBackend): Unit = {
    logInfo("Initializing YTsaurus scheduler backend")
    scheduler.asInstanceOf[TaskSchedulerImpl].initialize(backend)
    backend.asInstanceOf[YTsaurusSchedulerBackend].initialize()
  }
}
