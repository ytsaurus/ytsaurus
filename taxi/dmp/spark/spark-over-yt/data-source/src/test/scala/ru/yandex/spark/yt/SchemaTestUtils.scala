package ru.yandex.spark.yt

import org.apache.spark.sql.types.{DataType, MetadataBuilder, StructField}
import ru.yandex.spark.yt.serializers.SchemaConverter.MetadataFields

trait SchemaTestUtils {
  def structField(name: String, dataType: DataType,
                  originalName: Option[String] = None,
                  keyId: Long = -1,
                  metadata: MetadataBuilder = new MetadataBuilder,
                  nullable: Boolean = true): StructField = {
    StructField(name, dataType, nullable = nullable,
      metadata
        .putString(MetadataFields.ORIGINAL_NAME, originalName.getOrElse(name))
        .putLong(MetadataFields.KEY_ID, keyId)
        .build())
  }
}
