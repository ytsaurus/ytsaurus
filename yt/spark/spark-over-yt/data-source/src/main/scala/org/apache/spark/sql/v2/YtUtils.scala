package org.apache.spark.sql.v2

import org.apache.hadoop.fs.{FileStatus, FileSystem}
import org.apache.spark.SparkException
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{MetadataBuilder, StructField, StructType}
import org.apache.spark.util.SerializableConfiguration
import org.slf4j.LoggerFactory
import tech.ytsaurus.spyt.fs.YtClientConfigurationConverter.ytClientConfiguration
import tech.ytsaurus.spyt.fs.{YtFileSystemBase, YtHadoopPath}
import tech.ytsaurus.spyt.fs.path.YPathEnriched
import tech.ytsaurus.spyt.fs.path.YPathEnriched.ypath
import tech.ytsaurus.spyt.serializers.SchemaConverter.MetadataFields
import tech.ytsaurus.spyt.serializers.SchemaConverterConfig
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.client.CompoundClient
import tech.ytsaurus.core.cypress.YPath
import tech.ytsaurus.spyt.serializers.{SchemaConverter, SchemaConverterConfig}
import tech.ytsaurus.spyt.wrapper.client.YtClientProvider

object YtUtils {
  object Options {
    val MERGE_SCHEMA = "mergeschema"
    val PARSING_TYPE_V3 = "parsing_type_v3"
    val CONSUMER_PATH = "consumer_path"
    val QUEUE_PATH = "path"
  }

  private val log = LoggerFactory.getLogger(getClass)

  def inferSchema(sparkSession: SparkSession,
                  parameters: Map[String, String],
                  files: Seq[FileStatus])
                 (implicit client: CompoundClient = getClient(sparkSession)): Option[StructType] = {
    val enableMerge = parameters.get(Options.MERGE_SCHEMA)
      .orElse(sparkSession.conf.getOption("spark.sql.yt.mergeSchema")).exists(_.toBoolean)
    val allSchemas = getFilesSchemas(sparkSession, parameters, files)
    mergeFileSchemas(allSchemas, enableMerge)
  }


  private def getTablePath(fileStatus: FileStatus): YPathEnriched = {
    fileStatus.getPath match {
      case ytPath: YtHadoopPath => ytPath.ypath
      case p => ypath(p.getParent)
    }
  }

  private def getSchema(sparkSession: SparkSession, path: YPathEnriched, parameters: Map[String, String])
                       (implicit client: CompoundClient): StructType = {
    getSchema(sparkSession, path.toYPath, path.transaction, parameters)
  }

  def getSchema(sparkSession: SparkSession, path: YPath, transaction: Option[String], parameters: Map[String, String])
               (implicit client: CompoundClient = getClient(sparkSession)): StructType = {
    val config = SchemaConverterConfig(sparkSession)
    val parsingTypeV3 = parameters.get(Options.PARSING_TYPE_V3).map(_.toBoolean).getOrElse(config.parsingTypeV3)
    val schemaHint = SchemaConverter.schemaHint(parameters)
    val schemaTree = YtWrapper.attribute(path, "schema", transaction)
    SchemaConverter.sparkSchema(schemaTree, schemaHint, parsingTypeV3)
  }

  private def getClient(sparkSession: SparkSession): CompoundClient = {
    YtClientProvider.ytClient(ytClientConfiguration(sparkSession))
  }

  private[v2] case class FileWithSchema(file: FileStatus, schema: StructType)

  private[v2] case class SchemaDiscrepancy(expected: FileWithSchema,
                                           actual: FileWithSchema) {
    def format(file: FileWithSchema): String = {
      s"${file.file.getPath}: ${file.schema.fields.map(x => s"${x.name}[${x.dataType.simpleString}]").mkString(",")}"
    }

    def logWarning(): Unit = {
      log.warn(
        s"""Given tables have different schemas,
           |${format(expected)}
           |${format(actual)},
           |will try ignore key columns""".stripMargin
      )
    }

    def exception(): SparkException = {
      new SparkException(
        s"""Schema merging is turned off but given tables have different schemas:
           |${format(expected)}
           |${format(actual)}
           |Merging can be enabled by `${Options.MERGE_SCHEMA}` option
           |or `spark.sql.yt.mergeSchema` spark setting""".stripMargin
      )
    }
  }

  private[v2] def checkAllEquals(schemas: Seq[FileWithSchema]): Either[SchemaDiscrepancy, Option[StructType]] = {
    schemas.headOption.map { headSchema =>
      val headSchemaColumns = headSchema.schema.fields.toSet
      schemas.find(_.schema.fields.toSet != headSchemaColumns) match {
        case Some(schemaNotEqual) => Left(SchemaDiscrepancy(headSchema, schemaNotEqual))
        case None => Right(Some(headSchema.schema))
      }
    }.getOrElse(Right(None))
  }

  private[v2] def dropKeyFieldsMetadata(schema: StructType): StructType = {
    schema.copy(fields = schema.fields.map(_.withKeyId(-1)))
  }

  private[v2] def getFilesSchemas(sparkSession: SparkSession,
                                  parameters: Map[String, String],
                                  files: Seq[FileStatus])
                                 (implicit client: CompoundClient = getClient(sparkSession)): Seq[FileWithSchema] = {
    val (_, allSchemas) = files.foldLeft((Set.empty[YPathEnriched], List.empty[FileWithSchema])) {
      case ((curSet, schemas), fileStatus) =>
        val path = getTablePath(fileStatus)
        if (curSet.contains(path)) {
          (curSet, schemas)
        } else {
          (curSet + path, FileWithSchema(fileStatus, getSchema(sparkSession, path, parameters)) +: schemas)
        }
    }
    allSchemas
  }

  private def getKeys(fileSchema: FileWithSchema): Seq[StructField] = {
    fileSchema.schema.fields
      .filter(_.metadata.getLong(MetadataFields.KEY_ID) >= 0)
      .sortBy(_.metadata.getLong(MetadataFields.KEY_ID))
  }

  private[v2] def mergeFileSchemas(fileSchemas: Seq[FileWithSchema],
                                   enableMerge: Boolean): Option[StructType] = {
    if (enableMerge) {
      fileSchemas.headOption.map {
        head =>
          val keys = getKeys(head)
          val res = fileSchemas.map(_.schema).reduce((x, y) => x.merge(y))
          if (fileSchemas.forall(fs => getKeys(fs) == keys)) {
            res
          } else {
            dropKeyFieldsMetadata(res)
          }
      }
    } else {
      checkAllEquals(fileSchemas) match {
        case Left(discrepancy) =>
          discrepancy.logWarning()
          val schemasWithoutKeys = fileSchemas.map { fileSchema =>
            fileSchema.copy(schema = dropKeyFieldsMetadata(fileSchema.schema))
          }
          checkAllEquals(schemasWithoutKeys) match {
            case Left(discrepancy) => throw discrepancy.exception()
            case Right(schema) => schema
          }
        case Right(schema) => schema
      }
    }
  }

  implicit class RichStructField(field: StructField) {
    def withKeyId(keyId: Int): StructField = {
      val newMetadata = new MetadataBuilder()
        .withMetadata(field.metadata)
        .putLong(MetadataFields.KEY_ID, keyId)
        .build()

      field.copy(metadata = newMetadata)
    }

    def setNullable(value: Boolean = true): StructField = field.copy(nullable = value)
  }

  def bytesReadReporter(conf: Broadcast[SerializableConfiguration]): Long => Unit = {
    // TODO(alex-shishkin): Extracting FS every read report
    val fsScheme = FileSystem.getDefaultUri(conf.value.value).getScheme
    fsScheme match {
      case scheme if scheme == "yt" || scheme == "ytTable" =>
        bytesRead =>
          FileSystem.get(conf.value.value).asInstanceOf[YtFileSystemBase].internalStatistics.incrementBytesRead(bytesRead)
      case scheme =>
        log.warn(s"Unsupported uri: $scheme")
        _ => () //noop
    }
  }
}
