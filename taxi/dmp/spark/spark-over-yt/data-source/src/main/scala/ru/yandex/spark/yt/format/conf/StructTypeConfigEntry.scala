package ru.yandex.spark.yt.format.conf

import org.apache.spark.sql.types.StructType
import ru.yandex.spark.yt.fs.conf.ConfigEntry
import ru.yandex.spark.yt.serializers.SchemaConverter

class StructTypeConfigEntry(name: String) extends ConfigEntry[StructType](name, None) {
  override def get(value: String): StructType = SchemaConverter.sparkType(value).asInstanceOf[StructType]

  override def set(value: StructType): String = SchemaConverter.stringType(value)
}
