package tech.ytsaurus.spyt.wrapper.dyntable

import tech.ytsaurus.core.tables.TableSchema
import tech.ytsaurus.typeinfo.TiType
import tech.ytsaurus.ysontree.YTreeNode

case class ConsumerRow(queueCluster: String, queuePath: String, partitionIndex: Int, offset: Long)

object ConsumerUtils {
  object Columns {
    val QUEUE_CLUSTER = "queue_cluster"
    val QUEUE_PATH = "queue_path"
    val PARTITION_INDEX = "partition_index"
    val OFFSET = "offset"
  }

  import Columns._

  val CONSUMER_SCHEMA: TableSchema = TableSchema.builder()
    .addKey(QUEUE_CLUSTER, TiType.string())
    .addKey(QUEUE_PATH, TiType.string())
    .addKey(PARTITION_INDEX, TiType.uint64())
    .addValue(OFFSET, TiType.uint64())
    .build()

  def fromYTree(node: YTreeNode): ConsumerRow = {
    val mp = node.asMap()
    ConsumerRow(
      mp.get(QUEUE_CLUSTER).stringValue(),
      mp.get(QUEUE_PATH).stringValue(),
      mp.get(PARTITION_INDEX).intValue(),
      mp.get(OFFSET).longValue()
    )
  }
}
