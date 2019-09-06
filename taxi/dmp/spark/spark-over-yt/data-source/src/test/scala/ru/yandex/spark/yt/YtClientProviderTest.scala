package ru.yandex.spark.yt

import org.scalatest.{FlatSpec, Matchers}
import ru.yandex.inside.yt.kosher.impl.ytree.`object`.annotation.YTreeObject
import ru.yandex.inside.yt.kosher.impl.ytree.`object`.serializers.YTreeObjectSerializerFactory
import ru.yandex.yt.ytclient.`object`.UnversionedRowDeserializer
import ru.yandex.yt.ytclient.proxy.request.ReadTable

class YtClientProviderTest extends FlatSpec with Matchers {

  behavior of "YtClientProviderTest"

  import DefaultRpcCredentials._

  it should "ytClient" ignore {
    import scala.collection.JavaConverters._

    val client = YtClientProvider.ytClient("hume", credentials, 1)
    val stats = client.getNode("//tmp/sashbel/@").join()
    val rowCount = client.getNode("//tmp/sashbel/@row_count").join().longValue()

    stats.asMap().getOrThrow("schema").asList().get(0).asMap().getOrThrow("name").stringValue()
    val request = new ReadTable("//tmp/sashbel", new UnversionedRowDeserializer)
    val requestWithSchema = new ReadTable[EventLog]("//tmp/sashbel[:#1]", YTreeObjectSerializerFactory.forClass(classOf[EventLog]))
    val ytReader = client.readTable(requestWithSchema).join()

    try {
      println(ytReader.getTableSchema)
      println(ytReader.getTotalRowCount)
      while (ytReader.canRead) {
        var chunk = ytReader.read()
        while (chunk != null) {
          println(s"Chunk: ${chunk.size()}")
          chunk.iterator().asScala.foreach(row =>
            println(s"Row: $row")
          )
          chunk = ytReader.read()
        }
        ytReader.readyEvent().join()
      }
    } finally {
      ytReader.close()
    }
  }

}

@YTreeObject
case class EventLog(event_type: String,
                    timestamp: String,
                    id: Long)
