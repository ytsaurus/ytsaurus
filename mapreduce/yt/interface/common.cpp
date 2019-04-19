#include "common.h"

#include <mapreduce/yt/interface/protos/extension.pb.h>

#include <util/generic/xrange.h>

namespace NYT {
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

TNode ToNode(const TColumnSchema& columnSchema)
{
    TNode result = TNode::CreateMap();

    result["name"] = columnSchema.Name_;
    result["type"] = ToString(columnSchema.Type_);
    if (columnSchema.SortOrder_) {
        result["sort_order"] = ::ToString(*columnSchema.SortOrder_);
    }
    if (columnSchema.Lock_) {
        result["lock"] = ::ToString(*columnSchema.Lock_);
    }
    if (columnSchema.Expression_) {
        result["expression"] = *columnSchema.Expression_;
    }
    if (columnSchema.Aggregate_) {
        result["aggregate"] = *columnSchema.Aggregate_;
    }
    if (columnSchema.Group_) {
        result["group"] = *columnSchema.Group_;
    }

    result["required"] = columnSchema.Required_;

    return result;
}

TString GetColumnName(const ::google::protobuf::FieldDescriptor& field) {
    const auto& options = field.options();
    const auto columnName = options.GetExtension(column_name);
    if (!columnName.empty()) {
        return columnName;
    }
    const auto keyColumnName = options.GetExtension(key_column_name);
    if (!keyColumnName.empty()) {
        return keyColumnName;
    }
    return field.name();
}

EValueType GetColumnType(const ::google::protobuf::FieldDescriptor& field) {
    using namespace ::google::protobuf;
    Y_ENSURE(!field.is_repeated());
    switch (field.cpp_type()) {
    case FieldDescriptor::CPPTYPE_INT32:
        return EValueType::VT_INT32;
    case FieldDescriptor::CPPTYPE_INT64:
        return EValueType::VT_INT64;
    case FieldDescriptor::CPPTYPE_UINT32:
        return EValueType::VT_UINT32;
    case FieldDescriptor::CPPTYPE_UINT64:
        return EValueType::VT_UINT64;
    case FieldDescriptor::CPPTYPE_FLOAT:
    case FieldDescriptor::CPPTYPE_DOUBLE:
        return EValueType::VT_DOUBLE;
    case FieldDescriptor::CPPTYPE_BOOL:
        return EValueType::VT_BOOLEAN;
    case FieldDescriptor::CPPTYPE_STRING:
    case FieldDescriptor::CPPTYPE_MESSAGE:
    case FieldDescriptor::CPPTYPE_ENUM:
        return EValueType::VT_STRING;
    default:
        ythrow yexception() << "Unexpected field type '" << field.cpp_type_name() << "' for field " << field.name();
    }
}

bool HasExtension(const ::google::protobuf::FieldDescriptor& field) {
    const auto& o = field.options();
    return o.HasExtension(column_name) || o.HasExtension(key_column_name);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail

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
    TNode result = TNode::CreateList();
    result.Attributes()["strict"] = Strict_;
    result.Attributes()["unique_keys"] = UniqueKeys_;
    for (const auto& column : Columns_) {
        result.Add(NDetail::ToNode(column));
    }
    return result;
}

TTableSchema CreateTableSchema(
    const ::google::protobuf::Descriptor& tableProto,
    const TKeyColumns& keyColumns,
    const bool keepFieldsWithoutExtension) {

    TTableSchema result;
    for (int idx = 0, lim = tableProto.field_count(); idx < lim; ++idx) {
        const auto field = tableProto.field(idx);
        if (!keepFieldsWithoutExtension && !NDetail::HasExtension(*field)) {
            continue;
        }

        const auto name = NDetail::GetColumnName(*field);
        TColumnSchema column;
        column.Name(name);
        column.Type(NDetail::GetColumnType(*field));
        column.Required(field->is_required());
        result.AddColumn(column);
    }

    if (!keyColumns.Parts_.empty()) {
        result.SortBy(keyColumns.Parts_);
    }

    return result;
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
