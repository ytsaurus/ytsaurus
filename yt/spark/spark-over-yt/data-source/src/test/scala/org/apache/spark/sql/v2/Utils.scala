package org.apache.spark.sql.v2

import org.apache.spark.sql.connector.read.Statistics
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.{Column, DataFrame}
import tech.ytsaurus.spyt.common.utils.TuplePoint
import tech.ytsaurus.spyt.format.YtPartitionedFile


object Utils {
  def getParsedKeys(task: DataFrame): Seq[(TuplePoint, TuplePoint)] = {
    getRawKeys(task).map {
      case (a, b) => (a.get, b.get)
    }
  }

  def getStatistics(task: DataFrame): Statistics = {
    task.collect()
    extractYtScan(task.queryExecution.executedPlan).estimateStatistics()
  }

  private def getRawKeys(task: DataFrame): Seq[(Option[TuplePoint], Option[TuplePoint])] = {
    task.collect()

    val ytScan = extractYtScan(task.queryExecution.executedPlan)
    val partitions = ytScan.tryKeyPartitioning().getOrElse(ytScan)
      .getPartitions.flatMap(f => extractRawKeys(f.files))
    partitions
  }

  def extractRawKeys(files: Seq[PartitionedFile]): Seq[(Option[TuplePoint], Option[TuplePoint])] = {
    files.map {
      case file: YtPartitionedFile =>
        (file.beginPoint, file.endPoint)
    }
  }

  def extractYtScan(plan: SparkPlan): YtScan = {
    plan.collectFirst {
      case BatchScanExec(_, scan: YtScan, _) => scan
    }.get
  }
}
