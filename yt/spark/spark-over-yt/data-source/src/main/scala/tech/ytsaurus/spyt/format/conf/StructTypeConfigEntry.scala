package tech.ytsaurus.spyt.format.conf

import org.apache.spark.sql.types.StructType
import tech.ytsaurus.spyt.fs.conf.ConfigEntry
import tech.ytsaurus.spyt.serializers.SchemaConverter

class StructTypeConfigEntry(name: String) extends ConfigEntry[StructType](name, None) {
  override def get(value: String): StructType = SchemaConverter.stringToSparkType(value).asInstanceOf[StructType]

  override def set(value: StructType): String = SchemaConverter.sparkTypeToString(value)
}
