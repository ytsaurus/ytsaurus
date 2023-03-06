package tech.ytsaurus.spyt.format

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.log4j.Level
import org.apache.spark.sql.types.StructType
import tech.ytsaurus.spyt.common.utils.Segment.Segment
import tech.ytsaurus.spyt.common.utils.TupleSegment.TupleSegment
import tech.ytsaurus.spyt.common.utils._
import tech.ytsaurus.spyt.format.YPathUtils.RichRangeCriteria
import tech.ytsaurus.spyt.format.YtInputSplit._
import tech.ytsaurus.spyt.fs.path.YPathEnriched.ypath
import tech.ytsaurus.spyt.logger.YtLogger
import tech.ytsaurus.core.cypress.{Exact, Range, RangeCriteria, RangeLimit, YPath}
import tech.ytsaurus.spyt.serializers.PivotKeysConverter.{prepareKey, toList, toRangeLimit}
import tech.ytsaurus.spyt.serializers.SchemaConverter.MetadataFields
import tech.ytsaurus.core.cypress.{RangeLimit, YPath}
import tech.ytsaurus.spyt.common.utils.{AbstractSegment, ExpressionTransformer, MInfinity, PInfinity, Point, RealValue, SegmentSet}
import tech.ytsaurus.spyt.format.conf.FilterPushdownConfig
import tech.ytsaurus.spyt.logger.{YtDynTableLogger, YtDynTableLoggerConfig}
import tech.ytsaurus.spyt.serializers.SchemaConverter
import tech.ytsaurus.ysontree.{YTreeBooleanNodeImpl, YTreeBuilder, YTreeDoubleNodeImpl, YTreeEntityNodeImpl, YTreeIntegerNodeImpl, YTreeNode, YTreeStringNodeImpl}

import scala.annotation.tailrec


case class YtInputSplit(file: YtPartitionedFile, schema: StructType,
                        pushedFilters: SegmentSet = SegmentSet(),
                        filterPushdownConfig: FilterPushdownConfig,
                        ytLoggerConfig: Option[YtDynTableLoggerConfig]) extends InputSplit {
  private implicit lazy val ytLog: YtLogger = YtDynTableLogger.pushdown(ytLoggerConfig)
  private val logMessageInfo = Map(
    "segments" -> pushedFilters.toString,
    "keysInSchema" -> SchemaConverter.keys(schema).mkString(", ")
  )

  override def getLength: Long = 1

  override def getLocations: Array[String] = Array.empty

  private val originalFieldNames = schema.fields.map(x => x.metadata.getString(MetadataFields.ORIGINAL_NAME))
  private val basePath: YPath = file.ypath.withAdditionalAttributes(java.util.Map.of("columns",
    new YTreeBuilder().value(java.util.List.of(originalFieldNames: _*)).build()))

  lazy val ytPath: YPath = calculateYtPath(pushing = false)
  lazy val ytPathWithFiltersDetailed: YPath = calculateYtPath(pushing = true, union = false)
  lazy val ytPathWithFilters: YPath = calculateYtPath(pushing = true, union = true)

  private def calculateYtPath(pushing: Boolean, union: Boolean = true): YPath = {
    val tableKeys = SchemaConverter.keys(schema)
    if (pushing && filterPushdownConfig.enabled && tableKeys.nonEmpty) {
      val res = pushdownFiltersToYPath(
        union, pushedFilters, SchemaConverter.keys(schema), filterPushdownConfig, basePath)

      if (tableKeys.length > 1 || tableKeys.contains(None)) {
        ytLog.warn("YtInputSplit pushed filters with more than one key column", logMessageInfo ++
          Map("union" -> filterPushdownConfig.unionEnabled.toString, "ypath" -> res.toString))
      }
      if (pushedFilters.map.nonEmpty) {
        ytLog.logYt("YtInputSplit pushed filters to ypath", logMessageInfo ++
          Map("union" -> union.toString, "ypath" -> res.toString), level = if (file.isDynamic) Level.WARN else Level.INFO)
      }
      res
    } else {
      basePath
    }
  }
}

object YtInputSplit {
  private[format] def pushdownFiltersToYPath(single: Boolean, pushedFilters: SegmentSet, keys: Seq[Option[String]],
                                             filterPushdownConfig: FilterPushdownConfig, basePath: YPath) = {
    val pushdownCriteria = getCriteriaSeq(single && filterPushdownConfig.unionEnabled, pushedFilters,
      keys, filterPushdownConfig)
    applySegmentsToYPath(pushdownCriteria, basePath)
  }

  private[format] def applySegmentsToYPath(tupleSegments: Seq[TupleSegment], ypath: YPath): YPath = {
    import scala.collection.JavaConverters._

    val newRanges = ypath.getRanges.asScala.flatMap {
      criteria =>
        val range = criteria.toRange
        val beginKey = range.lower.key.asScala
        val endKey = range.upper.key.asScala
        if (beginKey.forall(ExpressionTransformer.isSupportedNodeType) &&
          endKey.forall(ExpressionTransformer.isSupportedNodeType)) {
          if (tupleSegments.isEmpty) {
            // Filters selects no data.
            Seq(emptyRange)
          } else {
            val begin = TuplePoint(beginKey.map(ExpressionTransformer.nodeToPoint))
            // Otherwise, empty end key will be recognized like -infinity.
            val end = if (endKey.nonEmpty) {
              TuplePoint(endKey.map(ExpressionTransformer.nodeToPoint))
            } else {
              TuplePoint(Seq(PInfinity()))
            }
            val intercept = AbstractSegment.intercept(Seq(Seq(TupleSegment(begin, end)), tupleSegments))
            if (intercept.isEmpty) {
              // Segment filters with chunk limits selects no data.
              Seq(emptyRange)
            } else {
              intercept.map {
                segmentList =>
                  val newLower = range.lower.toBuilder.setKey(prepareKey(segmentList.left): _*).build()
                  val newUpper = range.upper.toBuilder.setKey(prepareKey(segmentList.right): _*).build()
                  new Range(newLower, newUpper)
              }
            }
          }
        } else {
          Seq(criteria)
        }
    }

    ypath.ranges(newRanges : _*)
  }

  private[format] def getCriteriaSeq(single: Boolean, pushedFilters: SegmentSet,
                                     keys: Seq[Option[String]], filterPushdownConfig: FilterPushdownConfig)
                                    (implicit ytLog: YtLogger = YtLogger.noop): Seq[TupleSegment] = {

    val rawYPathFilterSegments = getKeyFilterSegments(
      preparePushedFilters(single, pushedFilters), keys.toList.flatten, filterPushdownConfig.ytPathCountLimit)(ytLog)
      .map(_.toMap)

    getTupleSegmentRanges(rawYPathFilterSegments, keys)
  }

  private[format] def getTupleSegmentRanges(segments: List[Map[String, Segment]],
                                            keys: Seq[Option[String]]): Seq[TupleSegment] = {
    val ranges = segments.map {
      map =>
        TupleSegment(
          TuplePoint(getLeftPoints(map, keys)),
          // Upper limit must be excluded.
          TuplePoint(getRightPoints(map, keys) :+ PInfinity())
        )
    }
    AbstractSegment.union(ranges.map(Seq(_)))
  }

  private lazy val emptyRange: RangeCriteria = {
    new Range(getRangeLimit(TuplePoint(Seq(PInfinity()))), getRangeLimit(TuplePoint(Seq(MInfinity()))))
  }

  private def preparePushedFilters(single: Boolean, pushedFilters: SegmentSet): SegmentSet = {
    if (single) {
      pushedFilters.simplifySegments
    } else {
      pushedFilters
    }
  }

  private[format] def getKeyFilterSegments(filterSegments: SegmentSet,
                                           keys: List[String],
                                           pathCountLimit: Int)
                                          (implicit ytLog: YtLogger = YtLogger.noop): List[List[(String, Segment)]] = {
    recursiveGetFilterSegmentsImpl(filterSegments, keys, pathCountLimit)(ytLog)
  }

  @tailrec
  private def recursiveGetFilterSegmentsImpl(filterSegments: SegmentSet,
                                             keys: List[String], pathCountLimit: Int,
                                             result: List[List[(String, Segment)]] = List(Nil))
                                            (implicit ytLog: YtLogger = YtLogger.noop): List[List[(String, Segment)]] = {
    keys match {
      case headKey :: tailKeys =>
        filterSegments.map.get(headKey) match {
          case None =>
            recursiveGetFilterSegmentsImpl(filterSegments, tailKeys, pathCountLimit, result)
          case Some(segments) =>
            if (segments.size * result.size > pathCountLimit) {
              ytLog.debug(s"YtInputSplit got more than ${pathCountLimit} segments and stopped")
              result.map(_.reverse)
            } else {
              recursiveGetFilterSegmentsImpl(filterSegments, tailKeys, pathCountLimit,
                result.flatMap(res => segments.map((headKey, _) +: res)))
            }
        }
      case Nil =>
        result.map(_.reverse)
    }
  }

  private def getRangeLimit(point: TuplePoint, rowIndex: Long = -1): RangeLimit = {
    getRangeLimit(prepareKey(point), rowIndex)
  }

  private def prepareKey(point: TuplePoint): Seq[YTreeNode] = {
    point.points.map(prepareKey)
  }

  private def getRangeLimit(keys: Seq[YTreeNode], rowIndex: Long): RangeLimit = {
    import scala.collection.JavaConverters._
    RangeLimit.builder().setKey(keys.toList.asJava).setRowIndex(rowIndex).build()
  }

  private def getSpecifiedEntity(value: String): YTreeNode = {
    new YTreeEntityNodeImpl(java.util.Map.of("type", new YTreeStringNodeImpl(value, null)))
  }

  private lazy val minimumKey: YTreeNode = getSpecifiedEntity("min")

  private lazy val maximumKey: YTreeNode = getSpecifiedEntity("max")

  def prepareKey(point: Point): YTreeNode = point match {
    case MInfinity() => minimumKey
    case PInfinity() => maximumKey
    case rValue: RealValue[_] if rValue.value == null =>
      new YTreeEntityNodeImpl(null)
    case rValue: RealValue[_] if rValue.value.isInstanceOf[Double] =>
      new YTreeDoubleNodeImpl(rValue.value.asInstanceOf[Double], null)
    case rValue: RealValue[_] if rValue.value.isInstanceOf[Long] =>
      new YTreeIntegerNodeImpl(true, rValue.value.asInstanceOf[Long], null)
    case rValue: RealValue[_] if rValue.value.isInstanceOf[Boolean] =>
      new YTreeBooleanNodeImpl(rValue.value.asInstanceOf[Boolean], null)
    case rValue: RealValue[_] if rValue.value.isInstanceOf[String] =>
      new YTreeStringNodeImpl(rValue.value.asInstanceOf[String], null)
  }

  private def getKeys(array: Map[String, Segment], keys: Seq[Option[String]],
                      segmentToPoint: Segment => Point, default: Point): Seq[Point] = {
    keys.map {
      case None => default
      case Some(key) => array.get(key).map(segmentToPoint).getOrElse(default)
    }
  }

  private def getLeftPoints(array: Map[String, Segment], keys: Seq[Option[String]]): Seq[Point] = {
    getKeys(array, keys, s => s.left, MInfinity())
  }

  private def getRightPoints(array: Map[String, Segment], keys: Seq[Option[String]]): Seq[Point] = {
    getKeys(array, keys, s => s.right, PInfinity())
  }
}
