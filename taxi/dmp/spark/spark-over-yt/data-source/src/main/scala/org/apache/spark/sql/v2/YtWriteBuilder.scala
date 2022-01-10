package org.apache.spark.sql.v2

import org.apache.hadoop.mapreduce.{Job, TaskAttemptContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.write.LogicalWriteInfo
import org.apache.spark.sql.execution.datasources.v2.FileWriteBuilder
import org.apache.spark.sql.execution.datasources.{OutputWriter, OutputWriterFactory}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructType}
import ru.yandex.spark.yt.format.conf.{SparkYtWriteConfiguration, YtTableSparkSettings}
import ru.yandex.spark.yt.format.{YtOutputCommitter, YtOutputWriter}
import ru.yandex.spark.yt.fs.YtClientConfigurationConverter.ytClientConfiguration
import ru.yandex.spark.yt.serializers.SchemaConverter

class YtWriteBuilder(paths: Seq[String],
                     formatName: String,
                     supportsDataType: DataType => Boolean,
                     info: LogicalWriteInfo)
  extends FileWriteBuilder(paths, formatName, supportsDataType, info) with Logging {

  private def addWriteOptions(options: Map[String, String],
                              writeConfiguration: SparkYtWriteConfiguration): Map[String, String] = {
    import YtTableSparkSettings.WriteTypeV3
    if (options.contains(WriteTypeV3.name)) options
    else options + (WriteTypeV3.name -> writeConfiguration.typeV3Format.toString)
  }

  override def prepareWrite(sqlConf: SQLConf,
                            job: Job,
                            options: Map[String, String],
                            dataSchema: StructType): OutputWriterFactory = {
    SchemaConverter.checkSchema(dataSchema)

    val ytClientConf = ytClientConfiguration(sqlConf)
    val writeConfiguration = SparkYtWriteConfiguration(sqlConf)
    val updatedOptions = addWriteOptions(options, writeConfiguration)
    YtTableSparkSettings.serialize(updatedOptions, dataSchema, job.getConfiguration)

    new OutputWriterFactory {
      override def getFileExtension(context: TaskAttemptContext): String = ""

      override def newInstance(path: String, dataSchema: StructType, context: TaskAttemptContext): OutputWriter = {
        val transaction = YtOutputCommitter.getWriteTransaction(context.getConfiguration)
        new YtOutputWriter(path, dataSchema, ytClientConf, writeConfiguration, transaction, updatedOptions)
      }
    }
  }
}

