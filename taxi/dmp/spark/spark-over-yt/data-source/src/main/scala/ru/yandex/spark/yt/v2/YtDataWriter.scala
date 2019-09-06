package ru.yandex.spark.yt.v2

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.writer.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types.StructType
import ru.yandex.spark.yt.{DefaultSourceParameters, YsonRowConverter, YtClientProvider}
import ru.yandex.yt.ytclient.proxy.TableWriter
import ru.yandex.yt.ytclient.proxy.request.WriteTable
import ru.yandex.yt.ytclient.rpc.RpcCredentials

class YtDataWriter(params: DefaultSourceParameters, schema: StructType) extends DataWriter[InternalRow] {
  val client = YtClientProvider.ytClient(params.proxy, new RpcCredentials(params.user, params.token), 1)
  val serializer = new YsonRowConverter(schema)
  val request = new WriteTable[Row](s"""<"append"=%true>${params.path}""", serializer)
  val writer = client.writeTable(request).join()

  override def write(record: InternalRow): Unit = {
    //writer.write(record)
    ???
  }

  override def commit(): WriterCommitMessage = {
    writer.close().join()
    null
  }

  override def abort(): Unit = {
    writer.close().join()
  }
}
