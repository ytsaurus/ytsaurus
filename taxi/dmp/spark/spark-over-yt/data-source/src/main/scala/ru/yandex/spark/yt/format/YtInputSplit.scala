package ru.yandex.spark.yt.format

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.spark.sql.types.StructType
import ru.yandex.inside.yt.kosher.cypress.{RangeLimit, YPath}
import ru.yandex.inside.yt.kosher.impl.ytree.{YTreeDoubleNodeImpl, YTreeEntityNodeImpl, YTreeIntegerNodeImpl, YTreeStringNodeImpl}
import ru.yandex.inside.yt.kosher.ytree.YTreeNode
import ru.yandex.spark.yt.common.utils._
import ru.yandex.spark.yt.format.YtInputSplit._
import ru.yandex.spark.yt.format.conf.FilterPushdownConfig
import ru.yandex.spark.yt.fs.YPathEnriched.ypath
import ru.yandex.spark.yt.serializers.PivotKeysConverter.toRangeLimit
import ru.yandex.spark.yt.serializers.SchemaConverter.MetadataFields

import scala.annotation.tailrec


case class YtInputSplit(file: YtPartitionedFile, schema: StructType,
                        pushedFilters: SegmentSet = SegmentSet(),
                        filterPushdownConfig: FilterPushdownConfig) extends InputSplit {
  override def getLength: Long = file.endRow - file.beginRow

  override def getLocations: Array[String] = Array.empty

  private val originalFieldNames = schema.fields.map(x => x.metadata.getString(MetadataFields.ORIGINAL_NAME))
  private val basePath: YPath = ypath(new Path(file.path)).toYPath.withColumns(originalFieldNames: _*)
  private val keys = schema.fields
    .map(x => (x.metadata.getLong(MetadataFields.KEY_ID), x.metadata.getString(MetadataFields.ORIGINAL_NAME)))
    .filter { case (key_id, _) => key_id >= 0 }.sorted.map { case (_, name) => name }

  lazy val ytPath: YPath = calculateYtPath(pushing = false)
  lazy val ytPathWithFiltersDetailed: YPath = calculateYtPath(pushing = true, union = false)
  lazy val ytPathWithFilters: YPath = calculateYtPath(pushing = true, union = true)

  private def calculateYtPath(pushing: Boolean, union: Boolean = true): YPath = {
    if (file.isDynamic) {
      basePath.withRange(toRangeLimit(file.beginKey, file.keyColumns), toRangeLimit(file.endKey, file.keyColumns))
    } else {
      if (pushing && filterPushdownConfig.enabled) {
        getYPath(union && filterPushdownConfig.unionEnabled)
      } else {
        basePath.withRange(file.beginRow, file.endRow)
      }
    }
  }

  private def getYPath(single: Boolean): YPath = {
    getYPathImpl(single, pushedFilters, keys, filterPushdownConfig, basePath, file)
  }
}

object YtInputSplit {
  private[format] def getYPathImpl(single: Boolean, pushedFilters: SegmentSet, keys: Seq[String],
                   filterPushdownConfig: FilterPushdownConfig,
                   basePath: YPath, file: YtPartitionedFile): YPath = {
    val rawYPathFilterSegments = getKeyFilterSegments(
      if (single) pushedFilters.simplifySegments else pushedFilters,
      keys.toList, filterPushdownConfig.ytPathCountLimit)
    if (rawYPathFilterSegments == List(Nil)) {
      basePath.withRange(file.beginRow, file.endRow)
    } else {
      rawYPathFilterSegments.foldLeft(basePath) {
        case (ypath, segment) =>
          ypath.withRange(
            getRangeLimit(prepareKeys(getLeftPoints(segment)), file.beginRow),
            getRangeLimit(prepareKeys(getRightPoints(segment)) :+ getMaximumKey, file.endRow)
          )
      }
    }
  }

  private[format] def getKeyFilterSegments(filterSegments: SegmentSet,
                           keys: List[String],
                           pathCountLimit: Int,
                        ): List[List[(String, Segment)]] = {
    recursiveGetFilterSegmentsImpl(filterSegments, keys, pathCountLimit)
  }

  @tailrec
  private def recursiveGetFilterSegmentsImpl(filterSegments: SegmentSet,
                                             keys: List[String], pathCountLimit: Int,
                                             result: List[List[(String, Segment)]] = List(Nil)
                              ): List[List[(String, Segment)]] = {
    keys match {
      case headKey :: tailKeys =>
        val segments = filterSegments.map.get(headKey) match {
          case None => List(Segment(MInfinity(), PInfinity()))
          case Some(segments) => segments
        }
        if (segments.size * result.size > pathCountLimit) {
          result.map(_.reverse)
        } else {
          recursiveGetFilterSegmentsImpl(filterSegments, tailKeys, pathCountLimit,
            result.flatMap(res => segments.map((headKey, _) +: res)))
        }
      case Nil =>
        result.map(_.reverse)
    }
  }

  private def getRangeLimit(keys: Seq[YTreeNode], rowIndex: Long = -1): RangeLimit = {
    import scala.collection.JavaConverters._
    new RangeLimit(keys.toList.asJava, rowIndex, -1)
  }

  private def getSpecifiedEntity(value: String): YTreeNode = {
    new YTreeEntityNodeImpl(java.util.Map.of("type", new YTreeStringNodeImpl(value, null)))
  }

  private def getMinimumKey: YTreeNode = getSpecifiedEntity("min")

  private def getMaximumKey: YTreeNode = getSpecifiedEntity("max")

  private def prepareKeys(array: Seq[Point]): Seq[YTreeNode] = {
    array.map {
      case MInfinity() => getMinimumKey
      case PInfinity() => getMaximumKey
      case rValue: RealValue[_] if rValue.value.isInstanceOf[Double] =>
        new YTreeDoubleNodeImpl(rValue.value.asInstanceOf[Double], null)
      case rValue: RealValue[_] if rValue.value.isInstanceOf[Long] =>
        new YTreeIntegerNodeImpl(true, rValue.value.asInstanceOf[Long], null)
      case rValue: RealValue[String] => new YTreeStringNodeImpl(rValue.value, null)
    }
  }

  private def getLeftPoints(array: Seq[(String, Segment)]): Seq[Point] = {
    array.map { case (_, Segment(left, _)) => left }
  }

  private def getRightPoints(array: Seq[(String, Segment)]): Seq[Point] = {
    array.map { case (_, Segment(_, right)) => right }
  }
}
