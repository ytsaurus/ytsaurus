package ru.yandex.spark.yt.serializers

import org.apache.spark.sql.types.Metadata
import ru.yandex.inside.yt.kosher.impl.ytree.builder.{YTree, YTreeBuilder}
import ru.yandex.inside.yt.kosher.ytree.{YTreeMapNode, YTreeNode, YTreeStringNode}

import scala.collection.JavaConverters.asScalaBufferConverter

object YtLogicalTypeSerializer {
  private def serializeTupleField(ytType: YtLogicalType, meta: Metadata,
                                  innerForm: Boolean): YTreeNode = {
    val builder = YTree.builder.beginMap
    builder.key("metadata").value(meta.json)
    builder.key("type").value(serializeTypeV3(ytType, innerForm)).buildMap
  }

  private def serializeStructField(name: String, ytType: YtLogicalType, meta: Metadata,
                                   innerForm: Boolean): YTreeNode = {
    val builder = YTree.builder.beginMap.key("name").value(name)
    builder.key("metadata").value(meta.json)
    builder.key("type").value(serializeTypeV3(ytType, innerForm)).buildMap
  }

  private def serializeElements(builder: YTreeBuilder, elements: Seq[(YtLogicalType, Metadata)],
                                innerForm: Boolean): Unit = {
    builder.key("elements").beginList()
    elements.foreach { case (e, meta) =>
      builder.onListItem()
      builder.value(serializeTupleField(e, meta, innerForm))
    }
    builder.endList()
  }

  private def serializeMembers(builder: YTreeBuilder, members: Seq[(String, YtLogicalType, Metadata)],
                               innerForm: Boolean): Unit = {
    builder.key("members").beginList()
    members.foreach { case (name, ytType, meta) =>
      builder.onListItem()
      builder.value(serializeStructField(name, ytType, meta, innerForm))
    }
    builder.endList()
  }

  def serializeTypeV3(ytType: YtLogicalType, innerForm: Boolean = false): YTreeNode = ytType match {
    case a: AtomicYtLogicalType =>
      YTree.builder().value(a.getNameV3(innerForm)).build()
    case composite: CompositeYtLogicalType =>
      val builder = YTree.builder
        .beginMap
        .key("type_name").value(composite.getNameV3(innerForm))
      composite match {
        case opt: YtLogicalType.Optional =>
          builder.key("item").value(serializeTypeV3(opt.inner, innerForm))
        case dec: YtLogicalType.Decimal =>
          builder.key("precision").value(dec.precision)
            .key("scale").value(dec.scale)
        case list: YtLogicalType.Array =>
          builder.key("item").value(serializeTypeV3(list.inner, innerForm))
        case dict: YtLogicalType.Dict =>
          builder.key("key").value(serializeTypeV3(dict.dictKey, innerForm))
            .key("value").value(serializeTypeV3(dict.dictValue, innerForm))
        case struct: YtLogicalType.Struct =>
          serializeMembers(builder, struct.fields, innerForm)
        case tuple: YtLogicalType.Tuple =>
          serializeElements(builder, tuple.elements, innerForm)
        case tagged: YtLogicalType.Tagged =>
          builder.key("item").value(serializeTypeV3(tagged.inner, innerForm))
            .key("tag").value(tagged.tag)
        case variantOverTuple: YtLogicalType.VariantOverTuple =>
          serializeElements(builder, variantOverTuple.fields, innerForm)
        case variantOverStruct: YtLogicalType.VariantOverStruct =>
          serializeMembers(builder, variantOverStruct.fields, innerForm)
      }
      builder.buildMap
  }

  def serializeType(ytType: YtLogicalType, isColumnType: Boolean = false): YTreeNode = {
    YTree.builder().value(ytType.getName(isColumnType)).build()
  }

  private def deserializeMembers(m: YTreeMapNode): Seq[(String, YtLogicalType, Metadata)] = {
    m.getOrThrow("members").asList().asScala.map { m =>
      val member = m.mapNode()
      (
        member.getOrThrow("name").stringValue(),
        deserializeTypeV3(member.getOrThrow("type")),
        member.getStringO("metadata").map[Metadata](Metadata.fromJson).orElse(Metadata.empty)
      )
    }
  }

  private def deserializeElements(m: YTreeMapNode): Seq[(YtLogicalType, Metadata)] = {
    m.getOrThrow("elements").asList().asScala.map { e =>
      val element = e.mapNode()
      (
        deserializeTypeV3(element.mapNode().getOrThrow("type")),
        element.getStringO("metadata").map[Metadata](Metadata.fromJson).orElse(Metadata.empty)
      )
    }
  }

  def deserializeTypeV3(node: YTreeNode): YtLogicalType = node match {
    case m: YTreeMapNode =>
      val alias = YtLogicalType.fromCompositeName(m.getOrThrow("type_name").stringValue())
      alias match {
        case YtLogicalType.Optional =>
          YtLogicalType.Optional(deserializeTypeV3(m.getOrThrow("item")))
        case YtLogicalType.Decimal =>
          YtLogicalType.Decimal(
            m.getOrThrow("precision").intValue(),
            m.getOrThrow("scale").intValue()
          )
        case YtLogicalType.Dict =>
          YtLogicalType.Dict(
            deserializeTypeV3(m.getOrThrow("key")),
            deserializeTypeV3(m.getOrThrow("value")))
        case YtLogicalType.Array =>
          YtLogicalType.Array(deserializeTypeV3(m.getOrThrow("item")))
        case YtLogicalType.Struct =>
          YtLogicalType.Struct(deserializeMembers(m))
        case YtLogicalType.Tuple =>
          YtLogicalType.Tuple(deserializeElements(m))
        case YtLogicalType.Tagged =>
          YtLogicalType.Tagged(
            deserializeTypeV3(m.getOrThrow("item")), m.getOrThrow("tag").stringValue()
          )
        case YtLogicalType.Variant =>
          if (m.containsKey("members")) {
            YtLogicalType.VariantOverStruct(deserializeMembers(m))
          } else if (m.containsKey("elements")) {
            YtLogicalType.VariantOverTuple(deserializeElements(m))
          } else {
            throw new NoSuchElementException("Incorrect variant format")
          }
      }
    case s: YTreeStringNode =>
      YtLogicalType.fromName(s.stringValue())
  }
}
