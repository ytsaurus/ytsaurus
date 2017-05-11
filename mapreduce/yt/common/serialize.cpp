#include "serialize.h"

#include "node_visitor.h"

#include "helpers.h"
#include "fluent.h"
#include <library/yson/consumer.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TString& value, IYsonConsumer* consumer)
{
    consumer->OnStringScalar(value);
}

void Serialize(const TStringBuf& value, IYsonConsumer* consumer)
{
    consumer->OnStringScalar(value);
}

void Serialize(const char* value, IYsonConsumer* consumer)
{
    consumer->OnStringScalar(value);
}

void Deserialize(TString& value, const TNode& node)
{
    value = node.AsString();
}

#define SERIALIZE_SIGNED(type) \
void Serialize(type value, IYsonConsumer* consumer) \
{ \
    consumer->OnInt64Scalar(static_cast<i64>(value)); \
}

#define SERIALIZE_UNSIGNED(type) \
void Serialize(type value, IYsonConsumer* consumer) \
{ \
    consumer->OnUint64Scalar(static_cast<ui64>(value)); \
}

SERIALIZE_SIGNED(signed char);
SERIALIZE_SIGNED(short);
SERIALIZE_SIGNED(int);
SERIALIZE_SIGNED(long);
SERIALIZE_SIGNED(long long);

SERIALIZE_UNSIGNED(unsigned char);
SERIALIZE_UNSIGNED(unsigned short);
SERIALIZE_UNSIGNED(unsigned int);
SERIALIZE_UNSIGNED(unsigned long);
SERIALIZE_UNSIGNED(unsigned long long);

#undef SERIALIZE_SIGNED
#undef SERIALIZE_UNSIGNED

void Deserialize(i64& value, const TNode& node)
{
    value = node.AsInt64();
}

void Deserialize(ui64& value, const TNode& node)
{
    value = node.AsUint64();
}

void Serialize(double value, IYsonConsumer* consumer)
{
    consumer->OnDoubleScalar(value);
}

void Deserialize(double& value, const TNode& node)
{
    value = node.AsDouble();
}

void Serialize(bool value, IYsonConsumer* consumer)
{
    consumer->OnBooleanScalar(value);
}

void Deserialize(bool& value, const TNode& node)
{
    value = node.AsBool();
}

void Serialize(const TNode& node, IYsonConsumer* consumer)
{
    TNodeVisitor visitor(consumer);
    visitor.Visit(node);
}

void Deserialize(TNode& value, const TNode& node)
{
    value = node;
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
void Deserialize(TMaybe<T>& value, const TNode& node)
{
    value.ConstructInPlace();
    Deserialize(value.GetRef(), node);
}

template <class T>
void Deserialize(yvector<T>& value, const TNode& node)
{
    for (const auto& element : node.AsList()) {
        value.emplace_back();
        Deserialize(value.back(), element);
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
void Deserialize(TKeyBase<T>& key, const TNode& node)
{
    Deserialize(key.Parts_, node);
}

////////////////////////////////////////////////////////////////////////////////

TString ToString(EValueType type)
{
    switch (type) {
        case VT_INT64: return "int64";
        case VT_UINT64: return "uint64";
        case VT_DOUBLE: return "double";
        case VT_BOOLEAN: return "boolean";
        case VT_STRING: return "string";
        case VT_ANY: return "any";
        default:
            ythrow yexception() << "Invalid value type " << static_cast<int>(type);
    }
}

void Deserialize(EValueType& valueType, const TNode& node)
{
    const auto& nodeStr = node.AsString();
    if (nodeStr == "int64") {
        valueType = VT_INT64;
    } else if (nodeStr == "uint64") {
        valueType = VT_UINT64;
    } else if (nodeStr == "double") {
        valueType = VT_DOUBLE;
    } else if (nodeStr == "boolean") {
        valueType = VT_BOOLEAN;
    } else if (nodeStr == "string") {
        valueType = VT_STRING;
    } else if (nodeStr == "any") {
        valueType = VT_ANY;
    } else {
        ythrow yexception() << "Invalid value type '" << nodeStr << "'";
    }
}

TString ToString(ESortOrder sortOrder)
{
    switch (sortOrder) {
        case SO_ASCENDING: return "ascending";
        case SO_DESCENDING: return "descending";
        default:
            ythrow yexception() << "Invalid sort order " << static_cast<int>(sortOrder);
    }
}

void Deserialize(ESortOrder& sortOrder, const TNode& node)
{
    const auto& nodeStr = node.AsString();
    if (nodeStr == "ascending") {
        sortOrder = SO_ASCENDING;
    } else if (nodeStr == "descending") {
        sortOrder = SO_DESCENDING;
    } else {
        ythrow yexception() << "Invalid sort order '" << nodeStr << "'";
    }
}

void Serialize(const TColumnSchema& columnSchema, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer).BeginMap()
        .Item("name").Value(columnSchema.Name_)
        .Item("type").Value(ToString(columnSchema.Type_))
        .DoIf(columnSchema.SortOrder_.Defined(), [&] (TFluentMap fluent) {
            fluent.Item("sort_order").Value(ToString(*columnSchema.SortOrder_));
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

void Serialize(const TRichYPath& path, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer).BeginAttributes()
        .DoIf(!path.Ranges_.empty(), [&] (TFluentAttributes fluent) {
            fluent.Item("ranges").List(path.Ranges_);
        })
        .DoIf(!path.Columns_.Parts_.empty(), [&] (TFluentAttributes fluent) {
            fluent.Item("columns").Value(path.Columns_);
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
    DESERIALIZE_ATTR("executable", path.Executable_);
    DESERIALIZE_ATTR("format", path.Format_);
    DESERIALIZE_ATTR("schema", path.Schema_);
    DESERIALIZE_ATTR("timestamp", path.Timestamp_);
    Deserialize(path.Path_, node);
}

void Serialize(const TAttributeFilter& filter, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer).List(filter.Attributes_);
}

#undef DESERIALIZE_ITEM
#undef DESERIALIZE_ATTR

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
