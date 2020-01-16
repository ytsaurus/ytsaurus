#include "common.h"

#include "format.h"
#include "serialize.h"

#include <mapreduce/yt/interface/protos/extension.pb.h>

#include <mapreduce/yt/interface/serialize.h>

#include <library/yson/node/node_builder.h>
#include <library/yson/node/node_io.h>

#include <util/generic/xrange.h>

namespace NYT {

using ::google::protobuf::FieldDescriptor;
using ::google::protobuf::Descriptor;

////////////////////////////////////////////////////////////////////////////////

bool TTableSchema::Empty() const
{
    return Columns_.empty();
}

TTableSchema& TTableSchema::AddColumn(const TString& name, EValueType type) &
{
    Columns_.push_back(TColumnSchema().Name(name).Type(type));
    return *this;
}

TTableSchema TTableSchema::AddColumn(const TString& name, EValueType type) &&
{
    return std::move(AddColumn(name, type));
}

TTableSchema& TTableSchema::AddColumn(const TString& name, EValueType type, ESortOrder sortOrder) &
{
    Columns_.push_back(TColumnSchema().Name(name).Type(type).SortOrder(sortOrder));
    return *this;
}

TTableSchema TTableSchema::AddColumn(const TString& name, EValueType type, ESortOrder sortOrder) &&
{
    return std::move(AddColumn(name, type, sortOrder));
}

TTableSchema& TTableSchema::SortBy(const TVector<TString>& columns) &
{
    THashMap<TString, ui64> columnsIndex;
    TVector<TColumnSchema> newColumns;
    newColumns.reserve(Columns_.size());
    newColumns.resize(columns.size());

    for (auto i: xrange(columns.size())) {
        Y_ENSURE(!columnsIndex.contains(columns[i]), "Key column name '"
            << columns[i] << "' repeats in columns list");
        columnsIndex[columns[i]] = i;
    }

    for (auto& column : Columns_) {
        if (auto it = columnsIndex.find(column.Name_)) {
            column.SortOrder(ESortOrder::SO_ASCENDING);
            newColumns[it->second] = std::move(column); // see newColumns.resize() above
            columnsIndex.erase(it);
        } else {
            column.SortOrder_ = Nothing();
            newColumns.push_back(std::move(column));
        }
    }

    Y_ENSURE(columnsIndex.empty(), "Column name '" << columnsIndex.begin()->first
            << "' not found in table schema");

    newColumns.swap(Columns_);
    return *this;
}

TTableSchema TTableSchema::SortBy(const TVector<TString>& columns) &&
{
    return std::move(SortBy(columns));
}

TNode TTableSchema::ToNode() const
{
    TNode result;
    TNodeBuilder builder(&result);
    Serialize(*this, &builder);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

TTableSchema CreateTableSchema(
    const Descriptor& messageDescriptor,
    const TKeyColumns& keyColumns,
    bool keepFieldsWithoutExtension)
{
    auto result = CreateTableSchema(messageDescriptor, keepFieldsWithoutExtension);
    if (!keyColumns.Parts_.empty()) {
        result.SortBy(keyColumns.Parts_);
    }
    return result;
}

TTableSchema CreateTableSchema(NTi::TTypePtr type)
{
    Y_VERIFY(type);
    TTableSchema schema;
    Deserialize(schema, NodeFromYsonString(NTi::NIo::AsYtSchema(type.Get())));
    return schema;
}

////////////////////////////////////////////////////////////////////////////////

bool IsTrivial(const TReadLimit& readLimit)
{
    return !readLimit.Key_ && !readLimit.RowIndex_ && !readLimit.Offset_;
}

EValueType NodeTypeToValueType(TNode::EType nodeType)
{
    switch (nodeType) {
        case TNode::EType::Int64: return VT_INT64;
        case TNode::EType::Uint64: return VT_UINT64;
        case TNode::EType::String: return VT_STRING;
        case TNode::EType::Double: return VT_DOUBLE;
        case TNode::EType::Bool: return VT_BOOLEAN;
        default:
            ythrow yexception() << "Cannot convert TNode type " << nodeType << " to EValueType";
    }
}

namespace NDetail {

////////////////////////////////////////////////////////////////////////////////

TString ToString(EValueType type)
{
    switch (type) {
        case VT_INT8:    return "int8";
        case VT_INT16:   return "int16";
        case VT_INT32:   return "int32";
        case VT_INT64:   return "int64";
        case VT_UINT8:   return "uint8";
        case VT_UINT16:  return "uint16";
        case VT_UINT32:  return "uint32";
        case VT_UINT64:  return "uint64";
        case VT_DOUBLE:  return "double";
        case VT_BOOLEAN: return "boolean";
        case VT_STRING:  return "string";
        case VT_UTF8:    return "utf8";
        case VT_ANY:     return "any";
        default:
            ythrow yexception() << "Invalid value type " << static_cast<int>(type);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NYT
