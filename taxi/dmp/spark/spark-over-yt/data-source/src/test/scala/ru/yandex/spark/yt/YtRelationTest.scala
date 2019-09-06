package ru.yandex.spark.yt

import org.apache.spark.sql.types.{LongType, Metadata, MetadataBuilder, StringType, StructField, StructType}
import org.scalatest.{FlatSpec, Matchers}

class YtRelationTest extends FlatSpec with Matchers with LocalSpark {

  behavior of "YtRelationTest"


  private def metadata(fieldName: String): Metadata = {
    val metadata = new MetadataBuilder()
    metadata.putString("original_name", fieldName)
    metadata.build()
  }

  private val parameters = DefaultSourceParameters(
    path = "//tmp/sashbel",
    proxy = "hume",
    user = DefaultRpcCredentials.user,
    token = DefaultRpcCredentials.token,
    partitions = Some(3),
    isSchemaFull = false
  )

  it should "get schema without hints" ignore {
    val relation = new YtRelation(spark.sqlContext, parameters, schemaHint = None)
    println(relation.schema)
    relation.schema.fields should contain theSameElementsAs Seq(
      StructField("event_type", StringType, metadata = metadata("event_type")),
      StructField("timestamp", StringType, metadata = metadata("timestamp")),
      StructField("id", LongType, metadata = metadata("id"))
    )
  }

  it should "get schema with hints" ignore {
    val schemaHint = StructType(Seq(StructField("id", StringType)))
    val relation = new YtRelation(spark.sqlContext, parameters, schemaHint = Some(schemaHint))
    println(relation.schema)
    relation.schema should contain theSameElementsAs Seq(
      StructField("event_type", StringType, metadata = metadata("event_type")),
      StructField("timestamp", StringType, metadata = metadata("timestamp")),
      StructField("id", StringType, metadata = metadata("id"))
    )
  }

}
