package ru.yandex.spark.yt.format

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.log4j.Level
import org.apache.spark.sql.types.StructType
import ru.yandex.inside.yt.kosher.cypress.{RangeLimit, YPath}
import ru.yandex.inside.yt.kosher.impl.ytree.{YTreeDoubleNodeImpl, YTreeEntityNodeImpl, YTreeIntegerNodeImpl, YTreeStringNodeImpl}
import ru.yandex.inside.yt.kosher.ytree.YTreeNode
import ru.yandex.spark.yt.common.utils.Segment.Segment
import ru.yandex.spark.yt.common.utils.TupleSegment.TupleSegment
import ru.yandex.spark.yt.common.utils._
import ru.yandex.spark.yt.format.YtInputSplit._
import ru.yandex.spark.yt.format.conf.FilterPushdownConfig
import ru.yandex.spark.yt.fs.YPathEnriched.ypath
import ru.yandex.spark.yt.logger.{YtDynTableLogger, YtDynTableLoggerConfig, YtLogger}
import ru.yandex.spark.yt.serializers.PivotKeysConverter.{toList, toRangeLimit}
import ru.yandex.spark.yt.serializers.SchemaConverter
import ru.yandex.spark.yt.serializers.SchemaConverter.MetadataFields

import scala.annotation.tailrec


case class YtInputSplit(file: YtPartitionedFile, schema: StructType,
                        pushedFilters: SegmentSet = SegmentSet(),
                        filterPushdownConfig: FilterPushdownConfig,
                        ytLoggerConfig: Option[YtDynTableLoggerConfig]) extends InputSplit {
  private implicit lazy val ytLog: YtLogger = YtDynTableLogger.pushdown(ytLoggerConfig)
  private val logMessageInfo = Map(
    "segments" -> pushedFilters.toString,
    "keysInFile" -> file.keyColumns.mkString(", "),
    "keysInSchema" -> SchemaConverter.keys(schema).mkString(", ")
  )

  override def getLength: Long = file.endRow - file.beginRow

  override def getLocations: Array[String] = Array.empty

  private val originalFieldNames = schema.fields.map(x => x.metadata.getString(MetadataFields.ORIGINAL_NAME))
  private val basePath: YPath = ypath(new Path(file.path)).toYPath.withColumns(originalFieldNames: _*)

  lazy val ytPath: YPath = calculateYtPath(pushing = false)
  lazy val ytPathWithFiltersDetailed: YPath = calculateYtPath(pushing = true, union = false)
  lazy val ytPathWithFilters: YPath = calculateYtPath(pushing = true, union = true)

  private def calculateYtPath(pushing: Boolean, union: Boolean = true): YPath = {
    val tableKeys = SchemaConverter.keys(schema)
    if (pushing && filterPushdownConfig.enabled && tableKeys.nonEmpty) {
      val res = if (file.isDynamic) {
        getYPathDynamic(union && filterPushdownConfig.unionEnabled)
      } else {
        getYPathStatic(union && filterPushdownConfig.unionEnabled)
      }

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
      if (file.isDynamic) {
        basePath.withRange(toRangeLimit(file.beginKey, file.keyColumns), toRangeLimit(file.endKey, file.keyColumns))
      } else {
        basePath.withRange(file.beginRow, file.endRow)
      }
    }
  }

  private def getYPathStatic(single: Boolean): YPath = {
    getYPathStaticImpl(single, pushedFilters, SchemaConverter.keys(schema), filterPushdownConfig, basePath, file)
  }

  private def getYPathDynamic(single: Boolean): YPath = {
    getYPathDynamicImpl(single, pushedFilters, SchemaConverter.keys(schema), filterPushdownConfig, basePath, file)
  }
}

object YtInputSplit {
  private[format] def getYPathStaticImpl(single: Boolean, pushedFilters: SegmentSet, keys: Seq[Option[String]],
                                         filterPushdownConfig: FilterPushdownConfig,
                                         basePath: YPath, file: YtPartitionedFile)
                                        (implicit ytLog: YtLogger = YtLogger.noop): YPath = {
    val rawYPathFilterSegments = getKeyFilterSegments(
      preparePushedFilters(single, pushedFilters), keys.toList.flatten, filterPushdownConfig.ytPathCountLimit)(ytLog)
      .map(_.toMap)

    ytLog.debug("YtInputSplit got key segments for static table from filters", Map(
      "union" -> single.toString,
      "key segments" -> rawYPathFilterSegments.mkString(", "),
      "segments" -> pushedFilters.toString,
      "keysInFile" -> file.keyColumns.mkString(", "),
      "keysInSchema" -> keys.mkString(", ")
    ))

    if (rawYPathFilterSegments.isEmpty) {
      // no data need
      basePath.withRange(file.beginRow, file.beginRow)
    } else {
      val filterRanges = getTupleSegmentRanges(rawYPathFilterSegments, keys)
      filterRanges.foldLeft(basePath) {
        case (ypath, segmentList) =>
          ypath.withRange(getRangeLimit(segmentList.left, file.beginRow), getRangeLimit(segmentList.right, file.endRow))
      }
    }
  }

  private[format] def getTupleSegmentRanges(segments: List[Map[String, Segment]],
                                            keys: Seq[Option[String]]): Seq[TupleSegment] = {
    val ranges = segments.map {
      map =>
        TupleSegment(
          TuplePoint(getLeftPoints(map, keys)),
          // upper limit is excluded
          TuplePoint(getRightPoints(map, keys) :+ PInfinity())
        )
    }
    AbstractSegment.union(ranges.map(Seq(_)))
  }

  private def addEmptyRange(path: YPath): YPath = {
    path.withRange(getRangeLimit(TuplePoint(Seq(PInfinity()))), getRangeLimit(TuplePoint(Seq(MInfinity()))))
  }

  private[format] def getYPathDynamicImpl(single: Boolean, pushedFilters: SegmentSet, keys: Seq[Option[String]],
                                          filterPushdownConfig: FilterPushdownConfig,
                                          basePath: YPath, file: YtPartitionedFile)
                                         (implicit ytLog: YtLogger = YtLogger.noop): YPath = {
    val logInfo = Map(
      "union" -> single.toString,
      "segments" -> pushedFilters.toString,
      "keysInFile" -> file.keyColumns.mkString(", "),
      "keysInSchema" -> keys.mkString(", "),
      "basePath" -> basePath.toString
    )

    ytLog.info("YtInputSplit tries to push filters to dynamic table read", logInfo)
    val beginKey = toList(file.beginKey, file.keyColumns)
    val endKey = toList(file.endKey, file.keyColumns)
    if (beginKey.forall(ExpressionTransformer.isSupportedNodeType) &&
      endKey.forall(ExpressionTransformer.isSupportedNodeType)) {
      val rawYPathFilterSegments = getKeyFilterSegments(
        preparePushedFilters(single, pushedFilters), keys.toList.flatten, filterPushdownConfig.ytPathCountLimit).map(_.toMap)

      ytLog.debug("YtInputSplit got key segments for dynamic table from filters", Map(
        "union" -> single.toString,
        "key segments" -> rawYPathFilterSegments.mkString(", "),
        "segments" -> pushedFilters.toString,
        "keysInFile" -> file.keyColumns.mkString(", "),
        "keysInSchema" -> keys.mkString(", ")
      ))

      if (rawYPathFilterSegments.isEmpty) {
        // filters selects no data
        addEmptyRange(basePath)
      } else {
        val begin = TuplePoint(beginKey.map(ExpressionTransformer.nodeToPoint))
        // empty end key will be recognized like -infinity
        val end = if (endKey.nonEmpty) {
          TuplePoint(endKey.map(ExpressionTransformer.nodeToPoint))
        } else {
          TuplePoint(Seq(PInfinity()))
        }
        val filterRanges = getTupleSegmentRanges(rawYPathFilterSegments, keys)
        val intercept = AbstractSegment.intercept(Seq(Seq(TupleSegment(begin, end)), filterRanges))
        if (intercept.isEmpty) {
          // segment filters with chunk limits selects no data
          addEmptyRange(basePath)
        } else {
          intercept.foldLeft(basePath) {
            case (ypath, segmentList) =>
              ypath.withRange(getRangeLimit(segmentList.left), getRangeLimit(segmentList.right))
          }
        }
      }
    } else {
      ytLog.info("YtInputSplit couldn't push filters to dynamic table read", logInfo)
      basePath.withRange(toRangeLimit(file.beginKey, file.keyColumns), toRangeLimit(file.endKey, file.keyColumns))
    }
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
    getRangeLimit(point.points.map(prepareKey), rowIndex)
  }

  private def getRangeLimit(keys: Seq[YTreeNode], rowIndex: Long): RangeLimit = {
    import scala.collection.JavaConverters._
    new RangeLimit(keys.toList.asJava, rowIndex, -1)
  }

  private def getSpecifiedEntity(value: String): YTreeNode = {
    new YTreeEntityNodeImpl(java.util.Map.of("type", new YTreeStringNodeImpl(value, null)))
  }

  private lazy val minimumKey: YTreeNode = getSpecifiedEntity("min")

  private lazy val maximumKey: YTreeNode = getSpecifiedEntity("max")

  private def prepareKey(point: Point): YTreeNode = point match {
    case MInfinity() => minimumKey
    case PInfinity() => maximumKey
    case rValue: RealValue[_] if rValue.value.isInstanceOf[Double] =>
      new YTreeDoubleNodeImpl(rValue.value.asInstanceOf[Double], null)
    case rValue: RealValue[_] if rValue.value.isInstanceOf[Long] =>
      new YTreeIntegerNodeImpl(true, rValue.value.asInstanceOf[Long], null)
    case rValue: RealValue[String] => new YTreeStringNodeImpl(rValue.value, null)
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
