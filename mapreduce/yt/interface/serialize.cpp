#include "serialize.h"

#include "fluent.h"

#include <mapreduce/yt/node/serialize.h>

#include <util/generic/string.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
void Deserialize(TMaybe<T>& value, const TNode& node)
{
    value.ConstructInPlace();
    Deserialize(value.GetRef(), node);
}

template <class T>
void Deserialize(TVector<T>& value, const TNode& node)
{
    for (const auto& element : node.AsList()) {
        value.emplace_back();
        Deserialize(value.back(), element);
    }
}

template <class T>
void Deserialize(THashMap<TString, T>& value, const TNode& node)
{
    for (const auto& item : node.AsMap()) {
        Deserialize(value[item.first], item.second);
    }
}

////////////////////////////////////////////////////////////////////////////////

// const auto& nodeMap = node.AsMap();
#define DESERIALIZE_ITEM(NAME, MEMBER) \
    if (const auto* item = nodeMap.FindPtr(NAME)) { \
        Deserialize(MEMBER, *item); \
    }

// const auto& attributesMap = node.GetAttributes().AsMap();
#define DESERIALIZE_ATTR(NAME, MEMBER) \
    if (const auto* attr = attributesMap.FindPtr(NAME)) { \
        Deserialize(MEMBER, *attr); \
    }

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TKey& key, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer).List(key.Parts_);
}

void Serialize(const TKeyColumns& keyColumns, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer).List(keyColumns.Parts_);
}

template <class T>
void Deserialize(TOneOrMany<T>& key, const TNode& node)
{
    Deserialize(key.Parts_, node);
}

////////////////////////////////////////////////////////////////////////////////

void Deserialize(EValueType& valueType, const TNode& node)
{
    const auto& nodeStr = node.AsString();
    static const THashMap<TString, EValueType> str2ValueType = {
        {"int8",  VT_INT8},
        {"int16", VT_INT16},
        {"int32", VT_INT32},
        {"int64", VT_INT64},

        {"uint8",   VT_UINT8},
        {"uint16",  VT_UINT16},
        {"uint32",  VT_UINT32},
        {"uint64",  VT_UINT64},

        {"boolean", VT_BOOLEAN},
        {"double",  VT_DOUBLE},

        {"string", VT_STRING},
        {"utf8",   VT_UTF8},

        {"any", VT_ANY},
    };

    auto it = str2ValueType.find(nodeStr);
    if (it == str2ValueType.end()) {
        ythrow yexception() << "Invalid value type '" << nodeStr << "'";
    }

    valueType = it->second;
}

void Deserialize(ESortOrder& sortOrder, const TNode& node)
{
    sortOrder = FromString<ESortOrder>(node.AsString());
}

void Deserialize(EOptimizeForAttr& optimizeFor, const TNode& node)
{
    optimizeFor = FromString<EOptimizeForAttr>(node.AsString());
}

void Deserialize(EErasureCodecAttr& erasureCodec, const TNode& node)
{
    erasureCodec = FromString<EErasureCodecAttr>(node.AsString());
}

void Serialize(const TColumnSchema& columnSchema, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer).BeginMap()
        .Item("name").Value(columnSchema.Name_)
        .DoIf(!columnSchema.RawTypeV2_.Defined(), [&] (TFluentMap fluent) {
            fluent.Item("type").Value(NDetail::ToString(columnSchema.Type_));
            fluent.Item("required").Value(columnSchema.Required_);
        })
        .DoIf(columnSchema.RawTypeV2_.Defined(), [&] (TFluentMap fluent) {
            fluent.Item("type_v2").Value(*columnSchema.RawTypeV2_);
        })
        .DoIf(columnSchema.SortOrder_.Defined(), [&] (TFluentMap fluent) {
            fluent.Item("sort_order").Value(::ToString(*columnSchema.SortOrder_));
        })
        .DoIf(columnSchema.Lock_.Defined(), [&] (TFluentMap fluent) {
            fluent.Item("lock").Value(*columnSchema.Lock_);
        })
        .DoIf(columnSchema.Expression_.Defined(), [&] (TFluentMap fluent) {
            fluent.Item("expression").Value(*columnSchema.Expression_);
        })
        .DoIf(columnSchema.Aggregate_.Defined(), [&] (TFluentMap fluent) {
            fluent.Item("aggregate").Value(*columnSchema.Aggregate_);
        })
        .DoIf(columnSchema.Group_.Defined(), [&] (TFluentMap fluent) {
            fluent.Item("group").Value(*columnSchema.Group_);
        })
    .EndMap();
}

void Deserialize(TColumnSchema& columnSchema, const TNode& node)
{
    const auto& nodeMap = node.AsMap();
    DESERIALIZE_ITEM("name", columnSchema.Name_);
    DESERIALIZE_ITEM("type", columnSchema.Type_);
    DESERIALIZE_ITEM("required", columnSchema.Required_);
    DESERIALIZE_ITEM("type_v2", columnSchema.RawTypeV2_);
    DESERIALIZE_ITEM("sort_order", columnSchema.SortOrder_);
    DESERIALIZE_ITEM("lock", columnSchema.Lock_);
    DESERIALIZE_ITEM("expression", columnSchema.Expression_);
    DESERIALIZE_ITEM("aggregate", columnSchema.Aggregate_);
    DESERIALIZE_ITEM("group", columnSchema.Group_);
}

void Serialize(const TTableSchema& tableSchema, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer).BeginAttributes()
        .Item("strict").Value(tableSchema.Strict_)
        .Item("unique_keys").Value(tableSchema.UniqueKeys_)
    .EndAttributes()
    .List(tableSchema.Columns_);
}

void Deserialize(TTableSchema& tableSchema, const TNode& node)
{
    const auto& attributesMap = node.GetAttributes().AsMap();
    DESERIALIZE_ATTR("strict", tableSchema.Strict_);
    DESERIALIZE_ATTR("unique_keys", tableSchema.UniqueKeys_);
    Deserialize(tableSchema.Columns_, node);
}

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TReadLimit& readLimit, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer).BeginMap()
        .DoIf(readLimit.Key_.Defined(), [&] (TFluentMap fluent) {
            fluent.Item("key").Value(*readLimit.Key_);
        })
        .DoIf(readLimit.RowIndex_.Defined(), [&] (TFluentMap fluent) {
            fluent.Item("row_index").Value(*readLimit.RowIndex_);
        })
        .DoIf(readLimit.Offset_.Defined(), [&] (TFluentMap fluent) {
            fluent.Item("offset").Value(*readLimit.Offset_);
        })
    .EndMap();
}

void Deserialize(TReadLimit& readLimit, const TNode& node)
{
    const auto& nodeMap = node.AsMap();
    DESERIALIZE_ITEM("key", readLimit.Key_);
    DESERIALIZE_ITEM("row_index", readLimit.RowIndex_);
    DESERIALIZE_ITEM("offset", readLimit.Offset_);
}

void Serialize(const TReadRange& readRange, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer).BeginMap()
        .DoIf(!IsTrivial(readRange.LowerLimit_), [&] (TFluentMap fluent) {
            fluent.Item("lower_limit").Value(readRange.LowerLimit_);
        })
        .DoIf(!IsTrivial(readRange.UpperLimit_), [&] (TFluentMap fluent) {
            fluent.Item("upper_limit").Value(readRange.UpperLimit_);
        })
        .DoIf(!IsTrivial(readRange.Exact_), [&] (TFluentMap fluent) {
            fluent.Item("exact").Value(readRange.Exact_);
        })
    .EndMap();
}

void Deserialize(TReadRange& readRange, const TNode& node)
{
    const auto& nodeMap = node.AsMap();
    DESERIALIZE_ITEM("lower_limit", readRange.LowerLimit_);
    DESERIALIZE_ITEM("upper_limit", readRange.UpperLimit_);
    DESERIALIZE_ITEM("exact", readRange.Exact_);
}

void Serialize(const THashMap<TString, TString>& renameColumns, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .DoMapFor(renameColumns, [] (TFluentMap fluent, const auto& item) {
            fluent.Item(item.first).Value(item.second);
        });
}

void Serialize(const TRichYPath& path, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer).BeginAttributes()
        .DoIf(!path.Ranges_.empty(), [&] (TFluentAttributes fluent) {
            fluent.Item("ranges").List(path.Ranges_);
        })
        .DoIf(path.Columns_.Defined(), [&] (TFluentAttributes fluent) {
            fluent.Item("columns").Value(*path.Columns_);
        })
        .DoIf(path.Append_.Defined(), [&] (TFluentAttributes fluent) {
            fluent.Item("append").Value(*path.Append_);
        })
        .DoIf(!path.SortedBy_.Parts_.empty(), [&] (TFluentAttributes fluent) {
            fluent.Item("sorted_by").Value(path.SortedBy_);
        })
        .DoIf(path.Teleport_.Defined(), [&] (TFluentAttributes fluent) {
            fluent.Item("teleport").Value(*path.Teleport_);
        })
        .DoIf(path.Primary_.Defined(), [&] (TFluentAttributes fluent) {
            fluent.Item("primary").Value(*path.Primary_);
        })
        .DoIf(path.Foreign_.Defined(), [&] (TFluentAttributes fluent) {
            fluent.Item("foreign").Value(*path.Foreign_);
        })
        .DoIf(path.RowCountLimit_.Defined(), [&] (TFluentAttributes fluent) {
            fluent.Item("row_count_limit").Value(*path.RowCountLimit_);
        })
        .DoIf(path.FileName_.Defined(), [&] (TFluentAttributes fluent) {
            fluent.Item("file_name").Value(*path.FileName_);
        })
        .DoIf(path.OriginalPath_.Defined(), [&] (TFluentAttributes fluent) {
            fluent.Item("original_path").Value(*path.OriginalPath_);
        })
        .DoIf(path.Executable_.Defined(), [&] (TFluentAttributes fluent) {
            fluent.Item("executable").Value(*path.Executable_);
        })
        .DoIf(path.Format_.Defined(), [&] (TFluentAttributes fluent) {
            fluent.Item("format").Value(*path.Format_);
        })
        .DoIf(path.Schema_.Defined(), [&] (TFluentAttributes fluent) {
            fluent.Item("schema").Value(*path.Schema_);
        })
        .DoIf(path.Timestamp_.Defined(), [&] (TFluentAttributes fluent) {
            fluent.Item("timestamp").Value(*path.Timestamp_);
        })
        .DoIf(path.CompressionCodec_.Defined(), [&] (TFluentAttributes fluent) {
            fluent.Item("compression_codec").Value(*path.CompressionCodec_);
        })
        .DoIf(path.ErasureCodec_.Defined(), [&] (TFluentAttributes fluent) {
            fluent.Item("erasure_codec").Value(::ToString(*path.ErasureCodec_));
        })
        .DoIf(path.OptimizeFor_.Defined(), [&] (TFluentAttributes fluent) {
            fluent.Item("optimize_for").Value(::ToString(*path.OptimizeFor_));
        })
        .DoIf(path.TransactionId_.Defined(), [&] (TFluentAttributes fluent) {
            fluent.Item("transaction_id").Value(GetGuidAsString(*path.TransactionId_));
        })
        .DoIf(path.RenameColumns_.Defined(), [&] (TFluentAttributes fluent) {
            fluent.Item("rename_columns").Value(*path.RenameColumns_);
        })
    .EndAttributes()
    .Value(path.Path_);
}

void Deserialize(TRichYPath& path, const TNode& node)
{
    const auto& attributesMap = node.GetAttributes().AsMap();
    DESERIALIZE_ATTR("ranges", path.Ranges_);
    DESERIALIZE_ATTR("columns", path.Columns_);
    DESERIALIZE_ATTR("append", path.Append_);
    DESERIALIZE_ATTR("sorted_by", path.SortedBy_);
    DESERIALIZE_ATTR("teleport", path.Teleport_);
    DESERIALIZE_ATTR("primary", path.Primary_);
    DESERIALIZE_ATTR("foreign", path.Foreign_);
    DESERIALIZE_ATTR("row_count_limit", path.RowCountLimit_);
    DESERIALIZE_ATTR("file_name", path.FileName_);
    DESERIALIZE_ATTR("original_path", path.OriginalPath_);
    DESERIALIZE_ATTR("executable", path.Executable_);
    DESERIALIZE_ATTR("format", path.Format_);
    DESERIALIZE_ATTR("schema", path.Schema_);
    DESERIALIZE_ATTR("timestamp", path.Timestamp_);
    DESERIALIZE_ATTR("compression_codec", path.CompressionCodec_);
    DESERIALIZE_ATTR("erasure_codec", path.ErasureCodec_);
    DESERIALIZE_ATTR("optimize_for", path.OptimizeFor_);
    DESERIALIZE_ATTR("transaction_id", path.TransactionId_);
    DESERIALIZE_ATTR("rename_columns", path.RenameColumns_);
    Deserialize(path.Path_, node);
}

void Serialize(const TAttributeFilter& filter, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer).List(filter.Attributes_);
}

void Deserialize(TVector<TTableColumnarStatistics>& statisticsList, const TNode& node)
{
    for (const auto& element : node.AsList()) {
        const auto& nodeMap = element.AsMap();
        statisticsList.emplace_back();
        auto& statistics = statisticsList.back();

        DESERIALIZE_ITEM("column_data_weights", statistics.ColumnDataWeight);
        DESERIALIZE_ITEM("legacy_chunks_data_weight", statistics.LegacyChunksDataWeight);
        DESERIALIZE_ITEM("timestamp_total_weight", statistics.TimestampTotalWeight);
    }
}

void Serialize(const TGUID& value, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer).Value(GetGuidAsString(value));
}

void Deserialize(TGUID& value, const TNode& node)
{
    value = GetGuid(node.AsString());
}

#undef DESERIALIZE_ITEM
#undef DESERIALIZE_ATTR

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
