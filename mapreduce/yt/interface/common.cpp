#include "common.h"

#include "format.h"
#include "serialize.h"

#include <mapreduce/yt/interface/protos/extension.pb.h>

#include <mapreduce/yt/interface/serialize.h>

#include <library/yson/node/node_builder.h>

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

static EValueType GetScalarFieldType(const FieldDescriptor& fieldDescriptor)
{
    switch (fieldDescriptor.cpp_type()) {
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
        ythrow yexception() << "Unexpected field type '" << fieldDescriptor.cpp_type_name() << "' for field " << fieldDescriptor.name();
    }
}

static bool HasExtension(const FieldDescriptor& fieldDescriptor)
{
    const auto& options = fieldDescriptor.options();
    return options.HasExtension(column_name) || options.HasExtension(key_column_name);
}

static TNode CreateFieldRawTypeV2(
    const FieldDescriptor& fieldDescriptor,
    ESerializationMode::Enum messageSerializationMode)
{
    auto fieldSerializationMode = messageSerializationMode;
    if (fieldDescriptor.options().HasExtension(serialization_mode)) {
        fieldSerializationMode = fieldDescriptor.options().GetExtension(serialization_mode);
    }

    TNode type;
    if (fieldDescriptor.type() == FieldDescriptor::TYPE_MESSAGE &&
        fieldSerializationMode == ESerializationMode::YT)
    {
        const auto& messageDescriptor = *fieldDescriptor.message_type();
        auto fields = TNode::CreateList();
        for (int fieldIndex = 0; fieldIndex < messageDescriptor.field_count(); ++fieldIndex) {
            const auto& innerFieldDescriptor = *messageDescriptor.field(fieldIndex);
            fields.Add(TNode()
                ("name", NDetail::GetColumnName(innerFieldDescriptor))
                ("type", CreateFieldRawTypeV2(
                    innerFieldDescriptor,
                    messageDescriptor.options().GetExtension(field_serialization_mode))));
        }
        type = TNode()
            ("metatype", "struct")
            ("fields", std::move(fields));
    } else {
        type = NDetail::ToString(GetScalarFieldType(fieldDescriptor));
    }

    switch (fieldDescriptor.label()) {
        case FieldDescriptor::Label::LABEL_REPEATED:
            Y_ENSURE(fieldSerializationMode == ESerializationMode::YT,
                "Repeated fields are supported only for YT serialization mode");
            return TNode()
                ("metatype", "list")
                ("element", std::move(type));
        case FieldDescriptor::Label::LABEL_OPTIONAL:
            return TNode()
                ("metatype", "optional")
                ("element", std::move(type));
        case FieldDescriptor::LABEL_REQUIRED:
            return type;
    }
    Y_FAIL();
}

TTableSchema CreateTableSchema(
    const Descriptor& messageDescriptor,
    const TKeyColumns& keyColumns,
    bool keepFieldsWithoutExtension)
{
    TTableSchema result;
    auto messageSerializationMode = messageDescriptor.options().GetExtension(field_serialization_mode);
    for (int fieldIndex = 0; fieldIndex < messageDescriptor.field_count(); ++fieldIndex) {
        const auto& fieldDescriptor = *messageDescriptor.field(fieldIndex);
        if (!keepFieldsWithoutExtension && !HasExtension(fieldDescriptor)) {
            continue;
        }
        TColumnSchema column;
        column.Name(NDetail::GetColumnName(fieldDescriptor));
        column.RawTypeV2(CreateFieldRawTypeV2(fieldDescriptor, messageSerializationMode));
        result.AddColumn(std::move(column));
    }

    if (!keyColumns.Parts_.empty()) {
        result.SortBy(keyColumns.Parts_);
    }

    return result;
}
////////////////////////////////////////////////////////////////////////////////

TTableSchema CreateYdlTableSchema(NTi::TType::TPtr type)
{
    Y_VERIFY(type);
    TTableSchema schema;
    Deserialize(schema, type->AsYtSchema());
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
