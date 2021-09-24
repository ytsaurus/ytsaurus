package org.apache.spark.sql.v2

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.vectorized.YtFileFormat
import ru.yandex.spark.yt.format.conf.SparkYtConfiguration.GlobalTransaction
import ru.yandex.spark.yt.fs.YPathEnriched.{YtTimestampPath, YtRootPath, YtTransactionPath}

class YtDataSourceV2 extends FileDataSourceV2 {
  private val defaultOptions: Map[String, String] = Map(
    "recursiveFileLookup" -> "true"
  )

  override def fallbackFileFormat: Class[_ <: FileFormat] = classOf[YtFileFormat]

  override def shortName(): String = "yt"

  override protected def getPaths(options: CaseInsensitiveStringMap): Seq[String] = {
    import ru.yandex.spark.yt.format.conf.YtTableSparkSettings._
    import ru.yandex.spark.yt.fs.conf._

    val paths = super.getPaths(options)
    val transaction = options.getYtConf(Transaction).orElse(sparkSession.getYtConf(GlobalTransaction.Id))
    val timestamp = options.getYtConf(Timestamp)

    paths.map { s =>
      val path = new Path(s)
      val transactionYPath = transaction.map(YtTransactionPath(path, _)).getOrElse(YtRootPath(path))
      val timestampYPath = timestamp.map(YtTimestampPath(transactionYPath, _)).getOrElse(transactionYPath)
      timestampYPath.toPath.toString
    }
  }

  private def getOptions(options: CaseInsensitiveStringMap): CaseInsensitiveStringMap = {
    import scala.collection.JavaConverters._
    val opts = defaultOptions ++ options.asScala
    new CaseInsensitiveStringMap(opts.asJava)
  }

  override def getTable(options: CaseInsensitiveStringMap): Table = {
    val paths = getPaths(options)
    val tableName = getTableName(options, paths)
    YtTable(tableName, sparkSession, getOptions(options), paths, None, fallbackFileFormat)
  }

  override def getTable(options: CaseInsensitiveStringMap, schema: StructType): Table = {
    val paths = getPaths(options)
    val tableName = getTableName(options, paths)
    YtTable(tableName, sparkSession, getOptions(options), paths, Some(schema), fallbackFileFormat)
  }
}

