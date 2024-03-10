package org.apache.spark.sql.v2

import org.apache.hadoop.fs.FileStatus
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, Write, WriteBuilder}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.v2.FileTable
import org.apache.spark.sql.execution.streaming.MetadataLogFileIndex
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.yson.YsonType
import tech.ytsaurus.spyt.format.conf.SparkYtConfiguration.Schema.ForcingNullableIfNoMetadata
import tech.ytsaurus.spyt.fs.conf.SparkYtSparkSession

import scala.annotation.tailrec
import scala.collection.JavaConverters._

case class YtTable(name: String,
                   sparkSession: SparkSession,
                   options: CaseInsensitiveStringMap,
                   paths: Seq[String],
                   userSpecifiedSchema: Option[StructType],
                   fallbackFileFormat: Class[_ <: FileFormat])
  extends FileTable(sparkSession, options, paths, userSpecifiedSchema) {

  override lazy val fileIndex: YtInMemoryFileIndex = {
    val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
    // Hadoop Configurations are case sensitive.
    val hadoopConf = sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
    // This is a non-streaming file based datasource.
    val rootPathsSpecified = DataSource.checkAndGlobPathIfNecessary(paths, hadoopConf,
      checkEmptyGlobPath = true, checkFilesExist = true, enableGlobbing = true)
    val fileStatusCache = FileStatusCache.getOrCreate(sparkSession)
    new YtInMemoryFileIndex(sparkSession, rootPathsSpecified, caseSensitiveMap, userSpecifiedSchema, fileStatusCache)
  }

  // Almost exact copy of super.dataSchema field with addition of ForcingNullableIfNoMetadata option
  override lazy val dataSchema: StructType = {
    val schema = userSpecifiedSchema.map { schema =>
      val partitionSchema = fileIndex.partitionSchema
      val resolver = sparkSession.sessionState.conf.resolver
      StructType(schema.filterNot(f => partitionSchema.exists(p => resolver(p.name, f.name))))
    }.orElse {
      inferSchema(fileIndex.allFiles())
    }.getOrElse {
      throw QueryCompilationErrors.dataSchemaNotSpecifiedError(formatName)
    }
    fileIndex match {
      case _ if !sparkSession.getYtConf(ForcingNullableIfNoMetadata).get => schema
      case _ => schema.asNullable
    }
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): YtScanBuilder =
    YtScanBuilder(sparkSession, fileIndex, schema, dataSchema, options)

  override def inferSchema(files: Seq[FileStatus]): Option[StructType] =
    YtUtils.inferSchema(sparkSession, options.asScala.toMap, files)

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder =
    new WriteBuilder {
      override def build(): Write = YtWrite(paths, formatName, supportsDataType, info)
    }

  override def supportsDataType(dataType: DataType): Boolean = YtTable.supportsDataType(dataType)

  override def formatName: String = "YT"
}

object YtTable {
  @tailrec
  def supportsDataType(dataType: DataType): Boolean = dataType match {
    case _: NullType => true

    case _: AtomicType => true

    case st: StructType => st.forall { f => supportsInnerDataType(f.dataType) }

    case ArrayType(elementType, _) => supportsInnerDataType(elementType)

    case MapType(keyType, valueType, _) =>
      supportsInnerDataType(keyType) && supportsInnerDataType(valueType)

    case udt: UserDefinedType[_] => supportsDataType(udt.sqlType)

    case _ => false
  }

  private def supportsInnerDataType(dataType: DataType): Boolean = dataType match {
    case YsonType => false
    case _ => supportsDataType(dataType)
  }
}
