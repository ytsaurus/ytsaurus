package ru.yandex.spark.yt.format.conf

import ru.yandex.inside.yt.kosher.impl.ytree.serialization.YTreeTextSerializer
import ru.yandex.spark.yt.fs.conf.ConfigEntry
import ru.yandex.spark.yt.serializers.YtLogicalType
import ru.yandex.spark.yt.serializers.YtLogicalTypeSerializer.{deserializeTypeV3, serializeTypeV3}

class YtLogicalTypeMapConfigEntry(name: String, default: Option[Map[String, YtLogicalType]] = None)
  extends ConfigEntry[Map[String, YtLogicalType]](name, default) {
  override def get(value: String): Map[String, YtLogicalType] = {
    fromJsonTyped[Map[String, String]](value).mapValues(t => deserializeTypeV3(YTreeTextSerializer.deserialize(t)))
  }

  override def set(value: Map[String, YtLogicalType]): String = {
    toJsonTyped[Map[String, String]](value.mapValues(t => YTreeTextSerializer.serialize(serializeTypeV3(t))))
  }
}