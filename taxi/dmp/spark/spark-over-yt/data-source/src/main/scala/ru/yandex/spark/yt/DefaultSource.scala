package ru.yandex.spark.yt

import java.time.Duration

import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import ru.yandex.inside.yt.kosher.cypress.YPath
import ru.yandex.inside.yt.kosher.impl.ytree.builder.YTreeBuilder
import ru.yandex.yt.ytclient.proxy.YtClient
import ru.yandex.yt.ytclient.proxy.request.ObjectType

class DefaultSource extends RelationProvider with CreatableRelationProvider with SchemaRelationProvider {
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): YtRelation = {
    val sourceParameters = DefaultSourceParameters(parameters)
    new YtRelation(sqlContext, sourceParameters, None)
  }

  private def createTable(path: String, yt: YtClient, schema: StructType, options: Map[String, String]): Unit = {
    import scala.collection.JavaConverters._
    val ytOptions = options.collect { case (key, value) if DefaultSource.tableOptions.contains(key) =>
      val builder = new YTreeBuilder()
      builder.onString(value)
      key -> builder.build()
    } + ("schema" -> SchemaConverter.ytSchema(schema))

    yt.createNode(path, ObjectType.Table, ytOptions.asJava).join()
  }

  private def removeTable(path: String, yt: YtClient): Unit = {
    yt.removeNode(path).join()
  }

  private def tableExists(path: String, yt: YtClient): Boolean = {
    try {
      yt.getNode(YPath.simple(path).allAttributes().toString).join()
      true
    } catch {
      case _: Throwable => false
    }
  }

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame): BaseRelation = {
    val sourceParameters = DefaultSourceParameters(parameters)
    val relation = new YtRelation(sqlContext, sourceParameters.copy(isSchemaFull = true), Some(data.schema))
    val timeout = Duration.ofMinutes(sqlContext.getConf("spark.yt.timeout.minutes").toLong)
    val yt = YtClientProvider.ytClient(sourceParameters, timeout)
    mode match {
      case SaveMode.Append =>
        if (!tableExists(sourceParameters.path, yt)) {
          createTable(sourceParameters.path, yt, data.schema, parameters)
        }
        relation.insert(data, overwrite = false)
      case SaveMode.ErrorIfExists =>
        if (tableExists(sourceParameters.path, yt)) {
          throw new IllegalArgumentException(s"Path ${sourceParameters.path} in ${sourceParameters.proxy} already exists")
        }
        createTable(sourceParameters.path, yt, data.schema, parameters)
        relation.insert(data, overwrite = false)
      case SaveMode.Overwrite =>
        if (tableExists(sourceParameters.path, yt)) {
          removeTable(sourceParameters.path, yt)
        }
        createTable(sourceParameters.path, yt, data.schema, parameters)
        relation.insert(data, overwrite = false)
      case SaveMode.Ignore =>
        if (!tableExists(sourceParameters.path, yt)) {
          createTable(sourceParameters.path, yt, data.schema, parameters)
          relation.insert(data, overwrite = false)
        }
    }
    relation
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType): BaseRelation = {
    val sourceParameters = DefaultSourceParameters(parameters)

    new YtRelation(sqlContext, sourceParameters, Some(schema))
  }
}

object DefaultSource {
  val tableOptions: Set[String] = Set("optimize_for")
}
