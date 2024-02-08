package tech.ytsaurus.spyt.streaming

import org.apache.spark.Partition

// Indexes are specified in YTsaurus format. [lowerIndex, upperIndex) segment will be read.
case class YtQueueRange(tabletIndex: Int, lowerIndex: Long, upperIndex: Long) extends Partition {
  override def index: Int = tabletIndex

  override def equals(o: Any): Boolean = {
    o match {
      case YtQueueRange(tabletIndex2, lowerIndex2, upperIndex2) =>
        tabletIndex == tabletIndex2 && lowerIndex == lowerIndex2 && upperIndex == upperIndex2
      case _ => false
    }
  }
}
