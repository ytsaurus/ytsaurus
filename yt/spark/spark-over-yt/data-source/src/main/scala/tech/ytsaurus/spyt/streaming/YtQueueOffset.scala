package tech.ytsaurus.spyt.streaming

import io.circe._
import io.circe.generic.semiauto.{deriveDecoder, deriveEncoder}
import io.circe.parser._
import io.circe.syntax._
import org.apache.spark.sql.execution.streaming.{Offset, SerializedOffset}
import tech.ytsaurus.client.CompoundClient
import tech.ytsaurus.core.cypress.YPath
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.wrapper.dyntable.ConsumerUtils

import scala.collection.SortedMap
import scala.concurrent.duration.DurationInt
import scala.util.Try

// Partitions are specified in Spark format, i.e. values is last read row index.
case class YtQueueOffset(cluster: String, path: String, partitions: SortedMap[Int, Long]) extends Offset {
  require(partitions.size == 1 + YtQueueOffset.getSafeMax(partitions.keys).getOrElse(-1),
    "Partitions must be numbered without skips")

  def >=(other: YtQueueOffset): Boolean = {
    partitions.forall { case (index, value) => value >= other.partitions.getOrElse(index, -1L) }
  }

  override def json(): String = YtQueueOffset.toJsonString(this)
}

object YtQueueOffset {
  private implicit val offsetEncoder: Encoder[YtQueueOffset] = deriveEncoder[YtQueueOffset]

  private implicit val offsetDecoder: Decoder[YtQueueOffset] = deriveDecoder[YtQueueOffset]

  def apply(offset: Offset): YtQueueOffset = offset match {
    case v: YtQueueOffset => v
    case sv: SerializedOffset =>
      decode(sv.json) match {
        case Left(error) => throw error
        case Right(value) => value
      }
    case _ => throw new IllegalArgumentException("Unsupported offset format")
  }

  private def toJsonString(offset: YtQueueOffset): String = offset.asJson.noSpaces

  def getSafeMax[T](values: Iterable[T])(implicit ordering: Ordering[T]): Option[T] = {
    if (values.nonEmpty) Some(values.max) else None
  }

  def getMaxOffset(cluster: String, queuePath: String)(implicit client: CompoundClient): Try[YtQueueOffset] = {
    import scala.collection.JavaConverters._
    val partitionsAttribute = YtWrapper.attribute(queuePath, "queue_partitions")
    Try {
      val partitionSeq = partitionsAttribute.asList().asScala.zipWithIndex.map {
        case (node, index) =>
          val map = node.asMap()
          if (map.containsKey("upper_row_index")) {
            (index, map.get("upper_row_index").longValue() - 1)
          } else if (map.containsKey("error")) {
            throw new IllegalStateException(f"Error while parsing partition info: ${map.get("error")}")
          } else {
            throw new IllegalStateException(f"Unknown error while parsing partition info")
          }
      }
      YtQueueOffset(cluster, queuePath, SortedMap(partitionSeq: _*))
    }
  }

  def getCurrentOffset(cluster: String, consumerPath: String, queuePath: String)
                      (implicit client: CompoundClient): YtQueueOffset = {
    import tech.ytsaurus.spyt.wrapper.dyntable.ConsumerUtils.Columns._
    val rows = YtWrapper.selectRows(consumerPath,
      Some(s"""$QUEUE_CLUSTER = "$cluster" and $QUEUE_PATH = "$queuePath"""")).map(ConsumerUtils.fromYTree)
    val sparsePartitions = rows.map(row => (row.partitionIndex, row.offset)).toMap
    require(rows.length == sparsePartitions.size,
      "Corrupted partition list. Probably the consumer table has different queues with the same name")
    val partitionCount = 1 + getSafeMax(sparsePartitions.keys).getOrElse(-1)
    val partitionSeq = (0 until partitionCount).map(index => (index, sparsePartitions.getOrElse(index, 0L) - 1))
    YtQueueOffset(cluster, queuePath, SortedMap(partitionSeq: _*))
  }

  def getRanges(start: YtQueueOffset, end: YtQueueOffset): Seq[YtQueueRange] = {
    end.partitions.toSeq.map { case (index, rawUpperIndex) =>
      val lowerIndex = start.partitions.getOrElse(index, -1L) + 1
      val upperIndex = rawUpperIndex + 1
      require(upperIndex >= lowerIndex, f"Partition $index has corrupted read segment [$lowerIndex;$upperIndex)")
      YtQueueRange(index, lowerIndex, upperIndex)
    }
  }

  def advance(consumerPath: String, newOffset: YtQueueOffset)(implicit client: CompoundClient): Unit = {
    val transaction = YtWrapper.createTransaction(None, timeout = 1 minute, sticky = true)
    try {
      newOffset.partitions.foreach { case (index, offset) =>
        if (offset >= 0) {
          YtWrapper.advanceConsumer(YPath.simple(consumerPath), YPath.simple(newOffset.path), index, offset + 1,
            transaction)
        }
      }
      transaction.commit().join()
    } catch {
      case e: Throwable =>
        transaction.abort().join()
        throw e
    }
  }
}
