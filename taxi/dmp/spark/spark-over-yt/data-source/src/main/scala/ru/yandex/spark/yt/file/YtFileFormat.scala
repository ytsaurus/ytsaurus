package ru.yandex.spark.yt.file

import java.net.URI

import net.sf.saxon.`type`.AtomicType
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.execution.datasources.{FileFormat, OutputWriterFactory, PartitionedFile, RecordReaderIterator}
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.{DataSourceRegister, Filter}
import org.apache.spark.sql.types.{StringType, StructType}
import ru.yandex.spark.yt.{DefaultSourceParameters, SchemaConverter, TableIterator, YtClientProvider}
import ru.yandex.yt.ytclient.proxy.request.ReadTable
import ru.yandex.yt.ytclient.rpc.RpcCredentials

class YtFileFormat extends FileFormat with DataSourceRegister with Serializable {
  override def inferSchema(sparkSession: SparkSession,
                           options: Map[String, String],
                           files: Seq[FileStatus]): Option[StructType] = {
    files match {
      case fileStatus :: _ =>
        val schemaHint = DefaultSourceParameters.schemaHint(options)
        val parameters = DefaultSourceParameters(options)
        import parameters._
        val client = YtClientProvider.ytClient(proxy, new RpcCredentials(user, token), 1)
        val schemaTree = client.getNode(s"/${fileStatus.getPath.toUri.getPath}/@schema").join()
        Some(SchemaConverter.sparkSchema(schemaTree, schemaHint))
      case Nil => None
    }
  }


  override def vectorTypes(requiredSchema: StructType, partitionSchema: StructType, sqlConf: SQLConf): Option[Seq[String]] = {
    Option(Seq.fill(requiredSchema.length)(classOf[OnHeapColumnVector].getName))
  }

  override def buildReaderWithPartitionValues(sparkSession: SparkSession,
                                              dataSchema: StructType,
                                              partitionSchema: StructType,
                                              requiredSchema: StructType,
                                              filters: Seq[Filter],
                                              options: Map[String, String],
                                              hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    val u = hadoopConf.get("yt.user")
    val t = hadoopConf.get("yt.token")
    val readBatch = supportBatch(sparkSession, requiredSchema)
    (file: PartitionedFile) => {
      val parameters = DefaultSourceParameters(options, u, t)
      import parameters._
      val client = YtClientProvider.ytClient(proxy, new RpcCredentials(user, token), 1)
      val split = YtInputSplit(new URI(file.filePath).getPath, file.start, file.length, requiredSchema)
      if (readBatch) {
        val taskContext = Option(TaskContext.get())
        val ytVectorizedReader = new YtVectorizedReader(client)
        val iter = new RecordReaderIterator(ytVectorizedReader)
        if (readBatch) ytVectorizedReader.enableBatch()
        taskContext.foreach(_.addTaskCompletionListener[Unit](_ => iter.close()))
        ytVectorizedReader.initialize(split, null)

        iter.asInstanceOf[Iterator[InternalRow]]
      } else {
        val client = YtClientProvider.ytClient(proxy, new RpcCredentials(user, token), 1)
        val request = new ReadTable(split.getFullPath, new InternalRowDeserializer(requiredSchema))
        val reader = client.readTable(request).join()
        val unsafeProjection = UnsafeProjection.create(requiredSchema)
        new TableIterator(reader).map(unsafeProjection(_))
      }
    }
  }

  override def prepareWrite(sparkSession: SparkSession, job: Job, options: Map[String, String], dataSchema: StructType): OutputWriterFactory = ???

  override def shortName(): String = "yt"

  override def isSplitable(sparkSession: SparkSession, options: Map[String, String], path: Path): Boolean = {
    true
  }

  override def supportBatch(sparkSession: SparkSession, dataSchema: StructType): Boolean = {
    dataSchema.forall(f => f.dataType.isInstanceOf[AtomicType] || f.dataType.isInstanceOf[StringType])
  }
}
