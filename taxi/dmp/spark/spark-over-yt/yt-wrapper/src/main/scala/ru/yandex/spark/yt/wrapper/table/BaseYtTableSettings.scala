package ru.yandex.spark.yt.wrapper.table
import ru.yandex.inside.yt.kosher.ytree.YTreeNode
import ru.yandex.yt.ytclient.tables.TableSchema

import scala.collection.JavaConverters._

import java.util.{Map => JMap}

class BaseYtTableSettings(schema: TableSchema,
                          options: JMap[String, Object]) extends YtTableSettings {
  override def ytSchema: YTreeNode = schema.toYTree

  override def optionsAny: Map[String, Any] = options.asScala.toMap
}
