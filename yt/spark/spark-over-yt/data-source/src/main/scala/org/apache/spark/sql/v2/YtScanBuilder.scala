package org.apache.spark.sql.v2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.{Scan, SupportsPushDownFilters}
import org.apache.spark.sql.execution.datasources.PartitioningAwareFileIndex
import org.apache.spark.sql.execution.datasources.v2.FileScanBuilder
import org.apache.spark.sql.sources.{Filter, IsNotNull}
import org.apache.spark.sql.types.{ArrayType, DataType, MapType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.v2.YtScanBuilder.pushStructMetadata
import tech.ytsaurus.spyt.common.utils.ExpressionTransformer.filtersToSegmentSet
import tech.ytsaurus.spyt.common.utils.SegmentSet
import tech.ytsaurus.spyt.format.conf.SparkYtConfiguration.Read.KeyColumnsFilterPushdown
import tech.ytsaurus.spyt.format.conf.YtTableSparkSettings
import tech.ytsaurus.spyt.fs.YtHadoopPath
import tech.ytsaurus.spyt.logger.{YtDynTableLogger, YtLogger}
import tech.ytsaurus.spyt.serializers.SchemaConverter
import tech.ytsaurus.spyt.wrapper.table.OptimizeMode

import scala.collection.JavaConverters._

case class YtScanBuilder(sparkSession: SparkSession,
                         fileIndex: PartitioningAwareFileIndex,
                         schema: StructType,
                         dataSchema: StructType,
                         options: CaseInsensitiveStringMap)
  extends FileScanBuilder(sparkSession, fileIndex, dataSchema) with SupportsPushDownFilters {
  lazy val hadoopConf = {
    val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
    // Hadoop Configurations are case sensitive.
    sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
  }
  lazy val optimizedForScan: Boolean = fileIndex.allFiles().forall { fileStatus =>
    YtHadoopPath.fromPath(fileStatus.getPath) match {
      case yp: YtHadoopPath => !yp.meta.isDynamic && yp.meta.optimizeMode == OptimizeMode.Scan
      case _ => false
    }
  }

  override def pruneColumns(requiredSchema: StructType): Unit = {
    this.requiredSchema = pushStructMetadata(requiredSchema, dataSchema)
  }

  private var pushedFilterSegments: SegmentSet = SegmentSet()
  private var filters: Array[Filter] = Array.empty

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    implicit val ytLog: YtLogger = YtDynTableLogger.pushdown(sparkSession)

    this.filters = filters
    this.pushedFilterSegments = filtersToSegmentSet(filters)

    logPushdownDetails()

    this.filters
  }

  private def logPushdownDetails()(implicit ytLog: YtLogger): Unit = {
    import tech.ytsaurus.spyt.fs.conf._

    val pushdownEnabled = sparkSession.ytConf(KeyColumnsFilterPushdown.Enabled)
    val keyColumns = SchemaConverter.keys(schema)
    val keySet = keyColumns.flatten.toSet

    val logInfo = Map(
      "filters" -> filters.mkString(", "),
      "keyColumns" -> keyColumns.mkString(", "),
      "segments" -> pushedFilterSegments.toString,
      "pushdownEnabled" -> pushdownEnabled.toString,
      "paths" -> options.get("paths")
    )

    val importantFilters = filters.filter(!_.isInstanceOf[IsNotNull])

    if (importantFilters.exists(_.references.exists(keySet.contains))) {
      if (pushedFilterSegments.map.nonEmpty) {
        ytLog.info("Pushing filters in YtScanBuilder, filters contain some key columns", logInfo)
      } else {
        ytLog.debug("Pushing filters in YtScanBuilder, filters contain some key columns", logInfo)
      }
    } else if (importantFilters.nonEmpty) {
      ytLog.trace("Pushing filters in YtScanBuilder, filters don't contain key columns", logInfo)
    }
  }

  override def pushedFilters(): Array[Filter] = {
    pushedFilterSegments.toFilters
  }

  override def build(): Scan = {
    var opts = options.asScala
    opts = opts + (YtTableSparkSettings.OptimizedForScan.name -> optimizedForScan.toString)
    YtScan(sparkSession, hadoopConf, fileIndex, dataSchema, readDataSchema(), readPartitionSchema(),
      new CaseInsensitiveStringMap(opts.asJava), pushedFilterSegments = pushedFilterSegments,
      pushedFilters = pushedFilters())
  }
}

object YtScanBuilder {
  private[v2] def pushStructMetadata(source: StructType, meta: StructType): StructType = {
    source.copy(fields = source.fields.map {
      f =>
        val opt = meta.fields.find(sf => sf.name == f.name)
        opt match {
          case None => f
          case Some(v) => f.copy(dataType = pushMetadata(f.dataType, v.dataType),
            metadata = v.metadata)
        }
    })
  }

  private def pushMapMetadata(source: MapType, meta: MapType): MapType = {
    source.copy(keyType = pushMetadata(source.keyType, meta.keyType),
      valueType = pushMetadata(source.valueType, meta.valueType))
  }

  private def pushArrayMetadata(source: ArrayType, meta: ArrayType): ArrayType = {
    source.copy(elementType = pushMetadata(source.elementType, meta.elementType))
  }

  private def pushMetadata(source: DataType, meta: DataType): DataType = {
    if (source.getClass != meta.getClass) {
      source
    } else {
      source match {
        case s: StructType => pushStructMetadata(s, meta.asInstanceOf[StructType])
        case m: MapType => pushMapMetadata(m, meta.asInstanceOf[MapType])
        case a: ArrayType => pushArrayMetadata(a, meta.asInstanceOf[ArrayType])
        case other => other
      }
    }
  }
}
