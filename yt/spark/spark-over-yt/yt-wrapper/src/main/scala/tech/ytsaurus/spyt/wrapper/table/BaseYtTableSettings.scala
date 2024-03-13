package tech.ytsaurus.spyt.wrapper.table

import tech.ytsaurus.core.tables.TableSchema
import tech.ytsaurus.ysontree.YTreeNode

import scala.collection.JavaConverters._
import java.util.{Map => JMap}

class BaseYtTableSettings(schema: TableSchema,
                          options: JMap[String, Object] = JMap.of()) extends YtTableSettings {
  override def ytSchema: YTreeNode = schema.toYTree

  override def optionsAny: Map[String, Any] = options.asScala.toMap
}
