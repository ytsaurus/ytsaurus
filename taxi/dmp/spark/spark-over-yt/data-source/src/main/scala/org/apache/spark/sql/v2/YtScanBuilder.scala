package org.apache.spark.sql.v2

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.{Scan, SupportsPushDownFilters}
import org.apache.spark.sql.execution.datasources.{InMemoryFileIndex, PartitioningAwareFileIndex}
import org.apache.spark.sql.execution.datasources.v2.FileScanBuilder
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import ru.yandex.spark.yt.format.conf.YtTableSparkSettings
import ru.yandex.spark.yt.fs.{YtDynamicPath, YtPath, YtStaticPath}

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
    YtPath.fromPath(fileStatus.getPath) match {
      case _: YtDynamicPath => false
      case yp: YtStaticPath => yp.optimizedForScan
      case _ => false
    }
  }

  override protected val supportsNestedSchemaPruning: Boolean = true

  private var filters: Array[Filter] = Array.empty

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    this.filters = filters
    this.filters
  }

  override def pushedFilters(): Array[Filter] = Array.empty

  override def build(): Scan = {
    var opts = options.asScala
    opts = opts + (YtTableSparkSettings.OptimizedForScan.name -> optimizedForScan.toString)
    YtScan(sparkSession, hadoopConf, fileIndex, dataSchema, readDataSchema(), readPartitionSchema(),
      new CaseInsensitiveStringMap(opts.asJava))
  }
}
