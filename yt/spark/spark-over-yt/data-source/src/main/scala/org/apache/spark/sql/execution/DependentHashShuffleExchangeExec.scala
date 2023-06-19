package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec.createShuffleWriteProcessor
import org.apache.spark.sql.execution.exchange.{REPARTITION_BY_NUM, ShuffleExchangeExec, ShuffleOrigin}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics, SQLShuffleWriteMetricsReporter}
import org.apache.spark.util.MutablePair
import org.apache.spark.{Partitioner, ShuffleDependency}
import tech.ytsaurus.spyt.common.utils.TuplePoint
import tech.ytsaurus.spyt.common.utils.ExpressionTransformer

// child data is divided by rules from dependent hash partitioning, sorting is not provided
class DependentHashShuffleExchangeExec(dependentPartitioning: Partitioning,
                                       child: SparkPlan,
                                       shuffleOrigin: ShuffleOrigin = REPARTITION_BY_NUM)
  extends ShuffleExchangeExec(dependentPartitioning, child, shuffleOrigin) {

  override def output: Seq[Attribute] = child.output

  override def nodeName: String = "DependentHashExchange"

  def this(expressions: Seq[Expression],
           pivots: Seq[TuplePoint],
           child: SparkPlan) {
    // We don't know num partitions when child is PlanLater, so here we get 0 anyway
    this(DependentHashPartitioning(expressions, pivots), child)
  }

  private val serializer: Serializer =
    new UnsafeRowSerializer(child.output.size, longMetric("dataSize"))

  @transient
  override lazy val shuffleDependency : ShuffleDependency[Int, InternalRow, InternalRow] = {
    val dep = prepareShuffleDependency(
      inputRDD,
      child.output,
      dependentPartitioning,
      serializer,
      metrics)
    metrics("numPartitions").set(dep.partitioner.numPartitions)
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLMetrics.postDriverMetricUpdates(
      sparkContext, executionId, metrics("numPartitions") :: Nil)
    dep
  }

  def prepareShuffleDependency(rdd: RDD[InternalRow],
                               outputAttributes: Seq[Attribute],
                               newPartitioning: Partitioning,
                               serializer: Serializer,
                               writeMetrics: Map[String, SQLMetric]
                              ): ShuffleDependency[Int, InternalRow, InternalRow] = {
    val part: Partitioner = newPartitioning match {
      case d: DependentHashPartitioning =>
        new Partitioner {
          override def numPartitions: Int = d.numPartitions

          override def getPartition(key: Any): Int = {
            val keyTuplePoint = key.asInstanceOf[TuplePoint]
            val res = d.pivots.zipWithIndex.find {
              case (pivotTuplePoint, _) =>
                pivotTuplePoint > keyTuplePoint
            }.map {
              case (_, index) => index
            }.getOrElse(d.pivots.length)
            res
          }
        }
      case _ => sys.error(s"Exchange not implemented for $newPartitioning")
    }
    def getPartitionKeyExtractor(): InternalRow => Any = newPartitioning match {
      case DependentHashPartitioning(expressions, _) =>
        val projection = UnsafeProjection.create(expressions, outputAttributes)
        row => {
          val list = projection(row).toSeq(expressions.map(_.dataType)).toList
          val tuplePointO = ExpressionTransformer.parseLiteralList(list)
          TuplePoint(tuplePointO.get)
        }
      case _ => sys.error(s"Exchange not implemented for $newPartitioning")
    }

    val rddWithPartitionIds: RDD[Product2[Int, InternalRow]] = {
      rdd.mapPartitionsWithIndexInternal((_, iter) => {
        val getPartitionKey = getPartitionKeyExtractor()
        val mutablePair = new MutablePair[Int, InternalRow]()
        iter.map { row => mutablePair.update(part.getPartition(getPartitionKey(row)), row) }
      }, isOrderSensitive = false)
    }

    // Now, we manually create a ShuffleDependency. Because pairs in rddWithPartitionIds
    // are in the form of (partitionId, row) and every partitionId is in the expected range
    // [0, part.numPartitions - 1]. The partitioner of this is a PartitionIdPassthrough.
    val dependency =
      new ShuffleDependency[Int, InternalRow, InternalRow](
        rddWithPartitionIds,
        new PartitionIdPassthrough(part.numPartitions),
        serializer,
        shuffleWriterProcessor = createShuffleWriteProcessor(writeMetrics))

    dependency
  }

  override protected def withNewChildInternal(newChild: SparkPlan): DependentHashShuffleExchangeExec =
    new DependentHashShuffleExchangeExec(dependentPartitioning, newChild)
}
