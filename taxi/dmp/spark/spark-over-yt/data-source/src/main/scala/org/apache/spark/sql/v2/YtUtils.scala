package org.apache.spark.sql.v2

import org.apache.hadoop.fs.FileStatus
import org.apache.spark.SparkException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{BinaryType, StructType}
import org.apache.spark.sql.yson.YsonType
import ru.yandex.spark.yt.fs.YtClientConfigurationConverter.ytClientConfiguration
import ru.yandex.spark.yt.fs.YtPath
import ru.yandex.spark.yt.serializers.SchemaConverter
import ru.yandex.spark.yt.wrapper.YtWrapper
import ru.yandex.spark.yt.wrapper.client.YtClientProvider
import ru.yandex.yt.ytclient.proxy.CompoundClient

object YtUtils {
  private def getFilePath(fileStatus: FileStatus): String = {
    fileStatus.getPath match {
      case ytPath: YtPath => ytPath.stringPath
      case p => YtPath.basePath(p)
    }
  }

  private def getOldFormatSchema(schema: StructType): StructType = {
    StructType(schema.map {
      case f if f.dataType == YsonType => f.copy(dataType = BinaryType)
      case f => f
    })
  }

  private def getCompatibleSchema(sparkSession: SparkSession, schema: StructType): StructType = {
    // backward compatibility
    if (sparkSession.conf.getOption("spark.yt.cluster.version").exists(_ < "3.0.1-1.1.1+yandex")) {
      getOldFormatSchema(schema)
    } else {
      schema
    }
  }

  private def getSchema(sparkSession: SparkSession, path: String, parameters: Map[String, String])
                       (implicit client: CompoundClient): StructType = {
    val schemaHint = SchemaConverter.schemaHint(parameters)
    val schemaTree = YtWrapper.attribute(path, "schema")
    val sparkSchema = SchemaConverter.sparkSchema(schemaTree, schemaHint)
    getCompatibleSchema(sparkSession, sparkSchema)
  }

  private def getClient(sparkSession: SparkSession): CompoundClient = {
    YtClientProvider.ytClient(ytClientConfiguration(sparkSession))
  }

  private case class FileWithSchema(file: FileStatus, schema: StructType)

  def inferSchema(sparkSession: SparkSession,
                  parameters: Map[String, String],
                  files: Seq[FileStatus])
                 (implicit client: CompoundClient = getClient(sparkSession)): Option[StructType] = {
    val enableMerge = parameters.get("mergeschema")
      .orElse(sparkSession.conf.getOption("spark.sql.yt.mergeSchema")).exists(_.toBoolean)
    val (_, allSchemas) = files.foldLeft((Set.empty[String], List.empty[FileWithSchema])) {
      case ((curSet, schemas), fileStatus) =>
        val path = getFilePath(fileStatus)
        if (curSet.contains(path)) {
          (curSet, schemas)
        } else {
          (curSet + path, FileWithSchema(fileStatus, getSchema(sparkSession, path, parameters)) +: schemas)
        }
    }
    if (enableMerge) {
      allSchemas.map(_.schema).reduceOption((x, y) => x.merge(y))
    } else {
      val firstDifferent = allSchemas.find(_.schema != allSchemas.head.schema)
      firstDifferent match {
        case None => allSchemas.headOption.map(_.schema)
        case Some(FileWithSchema(file, schema)) => throw new SparkException(
          s"Schema merging is turned off but given tables have different schemas:\n" +
            s"${file.getPath}: ${schema.fields.map(x => s"${x.name}[${x.dataType.simpleString}]").mkString(",")}\n" +
            s"${allSchemas.head.file.getPath}: ${allSchemas.head.schema.fields.map(x => s"${x.name}[${x.dataType.simpleString}]").mkString(",")}\n" +
            "Merging can be enabled by `mergeschema` option or `spark.sql.yt.mergeSchema` spark setting"
        )
      }
    }
  }
}
