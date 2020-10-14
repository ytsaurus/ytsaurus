package ru.yandex.spark.yt.format.conf

import ru.yandex.spark.yt.fs.conf.ConfigEntry
import ru.yandex.spark.yt.serializers.YtLogicalType

class YtLogicalTypeMapConfigEntry(name: String, default: Option[Map[String, YtLogicalType]] = None)
  extends ConfigEntry[Map[String, YtLogicalType]](name, default) {
  override def get(value: String): Map[String, YtLogicalType] = {
    fromJsonTyped[Map[String, String]](value).mapValues(YtLogicalType.fromName)
  }

  override def set(value: Map[String, YtLogicalType]): String = {
    toJsonTyped[Map[String, String]](value.mapValues(_.name))
  }
}