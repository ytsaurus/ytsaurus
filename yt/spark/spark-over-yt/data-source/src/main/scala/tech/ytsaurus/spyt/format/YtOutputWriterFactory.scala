package tech.ytsaurus.spyt.format

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.sql.execution.datasources.{OutputWriter, OutputWriterFactory}
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory
import tech.ytsaurus.client.CompoundClient
import tech.ytsaurus.spyt.format.conf.{SparkYtWriteConfiguration, YtTableSparkSettings}
import tech.ytsaurus.spyt.serializers.SchemaConverter
import tech.ytsaurus.spyt.wrapper.YtWrapper
import tech.ytsaurus.spyt.wrapper.client.{YtClientConfiguration, YtClientProvider}

class YtOutputWriterFactory(ytClientConf: YtClientConfiguration,
                            writeConfiguration: SparkYtWriteConfiguration,
                            options: Map[String, String]) extends OutputWriterFactory {
  import YtOutputWriterFactory._

  override def getFileExtension(context: TaskAttemptContext): String = ""

  override def newInstance(path: String, dataSchema: StructType, context: TaskAttemptContext): OutputWriter = {
    log.debug(s"[${Thread.currentThread().getName}] Creating new output writer for ${context.getTaskAttemptID.getTaskID} at path $path")

    implicit val ytClient: CompoundClient = YtClientProvider.ytClient(ytClientConf)

    if (YtWrapper.isDynamicTable(path)) {
      new YtDynamicTableWriter(path, dataSchema, writeConfiguration, options)
    } else {
      val transaction = YtOutputCommitter.getWriteTransaction(context.getConfiguration)
      new YtOutputWriter(path, dataSchema, writeConfiguration, transaction, options)
    }
  }
}

object YtOutputWriterFactory {
  private val log = LoggerFactory.getLogger(getClass)

  def create(writeConfiguration: SparkYtWriteConfiguration,
             ytClientConf: YtClientConfiguration,
             options: Map[String, String],
             dataSchema: StructType,
             jobConfiguration: Configuration): YtOutputWriterFactory = {
    SchemaConverter.checkSchema(dataSchema, options)

    val updatedOptions = addWriteOptions(options, writeConfiguration)
    YtTableSparkSettings.serialize(updatedOptions, dataSchema, jobConfiguration)

    new YtOutputWriterFactory(ytClientConf, writeConfiguration, updatedOptions)
  }

  private def addWriteOptions(options: Map[String, String],
                              writeConfiguration: SparkYtWriteConfiguration): Map[String, String] = {
    import YtTableSparkSettings.WriteTypeV3
    if (options.contains(WriteTypeV3.name)) options
    else options + (WriteTypeV3.name -> writeConfiguration.typeV3Format.toString)
  }
}
