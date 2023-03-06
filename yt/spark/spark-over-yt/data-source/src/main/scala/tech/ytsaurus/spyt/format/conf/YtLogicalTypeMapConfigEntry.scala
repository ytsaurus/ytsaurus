package tech.ytsaurus.spyt.format.conf

import tech.ytsaurus.spyt.serializers.YtLogicalTypeSerializer.{deserializeTypeV3, serializeTypeV3}
import tech.ytsaurus.spyt.fs.conf.ConfigEntry
import tech.ytsaurus.spyt.serializers.YtLogicalType
import tech.ytsaurus.ysontree.YTreeTextSerializer

class YtLogicalTypeMapConfigEntry(name: String, default: Option[Map[String, YtLogicalType]] = None)
  extends ConfigEntry[Map[String, YtLogicalType]](name, default) {
  override def get(value: String): Map[String, YtLogicalType] = {
    fromJsonTyped[Map[String, String]](value).mapValues(t => deserializeTypeV3(YTreeTextSerializer.deserialize(t)))
  }

  override def set(value: Map[String, YtLogicalType]): String = {
    toJsonTyped[Map[String, String]](value.mapValues(t => YTreeTextSerializer.serialize(serializeTypeV3(t))))
  }
}