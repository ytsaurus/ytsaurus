package tech.ytsaurus.spyt.streaming

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.types.StructType
import org.apache.spark.{Partition, SparkContext, TaskContext}
import tech.ytsaurus.spyt.fs.YtClientConfigurationConverter.ytClientConfiguration
import tech.ytsaurus.spyt.serializers.UnversionedRowsetDeserializer
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.wrapper.client.YtClientProvider

import java.util.UUID


class YtQueueRDD(sc: SparkContext,
                 schema: StructType,
                 consumerPath: String,
                 queuePath: String,
                 ranges: Seq[YtQueueRange]) extends RDD[InternalRow](sc, Nil) {
  private val classId: String = s"YtQueueRDD-${UUID.randomUUID()}"
  private val configuration = ytClientConfiguration(sc.hadoopConfiguration)

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val yt = YtClientProvider.ytClient(configuration, classId)

    val partition = split.asInstanceOf[YtQueueRange]
    val size = partition.upperIndex - partition.lowerIndex
    if (size > 0) {
      val proj = UnsafeProjection.create(schema)
      val deserializer = new UnversionedRowsetDeserializer(schema)
      val rowsets =
        YtWrapper.pullConsumerStrict(consumerPath, queuePath, partition.tabletIndex, partition.lowerIndex, size)(yt)
      rowsets
        .map(deserializer.deserializeRowset)
        .foldLeft(Iterator[InternalRow]())(_ ++ _)
        .map(proj)
    } else {
      Seq.empty.iterator
    }
  }

  override protected def getPartitions: Array[Partition] = ranges.sortBy(_.tabletIndex).toArray
}
