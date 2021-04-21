#include "common.h"

#include "errors.h"
#include "format.h"
#include "serialize.h"

#include <mapreduce/yt/interface/protos/extension.pb.h>

#include <mapreduce/yt/interface/serialize.h>

#include <library/cpp/yson/node/node_builder.h>
#include <library/cpp/yson/node/node_io.h>
#include <library/cpp/type_info/type.h>

#include <util/generic/xrange.h>

namespace NYT {

using ::google::protobuf::FieldDescriptor;
using ::google::protobuf::Descriptor;

////////////////////////////////////////////////////////////////////////////////

static NTi::TTypePtr OldTypeToTypeV3(EValueType type)
{
    switch (type) {
        case VT_INT64:
            return NTi::Int64();
        case VT_UINT64:
            return NTi::Uint64();

        case VT_DOUBLE:
            return NTi::Double();

        case VT_BOOLEAN:
            return NTi::Bool();

        case VT_STRING:
            return NTi::String();

        case VT_ANY:
            return NTi::Yson();

        case VT_INT8:
            return NTi::Int8();
        case VT_INT16:
            return NTi::Int16();
        case VT_INT32:
            return NTi::Int32();

        case VT_UINT8:
            return NTi::Uint8();
        case VT_UINT16:
            return NTi::Uint16();
        case VT_UINT32:
            return NTi::Uint32();

        case VT_UTF8:
            return NTi::Utf8();

        case VT_NULL:
            return NTi::Null();

        case VT_VOID:
            return NTi::Void();

        case VT_DATE:
            return NTi::Date();
        case VT_DATETIME:
            return NTi::Datetime();
        case VT_TIMESTAMP:
            return NTi::Timestamp();
        case VT_INTERVAL:
            return NTi::Interval();

        case VT_FLOAT:
            return NTi::Float();
        case VT_JSON:
            return NTi::Json();
    }
}

static std::pair<EValueType, bool> Simplify(const NTi::TTypePtr& type)
{
    using namespace NTi;
    const auto typeName = type->GetTypeName();
    switch (typeName) {
        case ETypeName::Bool:
            return {VT_BOOLEAN, true};

        case ETypeName::Int8:
            return {VT_INT8, true};
        case ETypeName::Int16:
            return {VT_INT16, true};
        case ETypeName::Int32:
            return {VT_INT32, true};
        case ETypeName::Int64:
            return {VT_INT64, true};

        case ETypeName::Uint8:
            return {VT_UINT8, true};
        case ETypeName::Uint16:
            return {VT_UINT16, true};
        case ETypeName::Uint32:
            return {VT_UINT32, true};
        case ETypeName::Uint64:
            return {VT_UINT64, true};

        case ETypeName::Float:
            return {VT_FLOAT, true};
        case ETypeName::Double:
            return {VT_DOUBLE, true};

        case ETypeName::String:
            return {VT_STRING, true};
        case ETypeName::Utf8:
            return {VT_UTF8, true};

        case ETypeName::Date:
            return {VT_DATE, true};
        case ETypeName::Datetime:
            return {VT_DATETIME, true};
        case ETypeName::Timestamp:
            return {VT_TIMESTAMP, true};
        case ETypeName::Interval:
            return {VT_INTERVAL, true};

        case ETypeName::TzDate:
        case ETypeName::TzDatetime:
        case ETypeName::TzTimestamp:
            break;

        case ETypeName::Json:
            return {VT_JSON, true};
        case ETypeName::Decimal:
            return {VT_STRING, true};
        case ETypeName::Uuid:
            break;
        case ETypeName::Yson:
            return {VT_ANY, true};

        case ETypeName::Void:
            return {VT_VOID, false};
        case ETypeName::Null:
            return {VT_NULL, false};

        case ETypeName::Optional:
            {
                auto itemType = type->AsOptional()->GetItemType();
                if (itemType->IsPrimitive()) {
                    auto simplified = Simplify(itemType->AsPrimitive());
                    if (simplified.second) {
                        simplified.second = false;
                        return simplified;
                    }
                }
                return {VT_ANY, false};
            }
        case ETypeName::List:
            return {VT_ANY, true};
        case ETypeName::Dict:
            return {VT_ANY, true};
        case ETypeName::Struct:
            return {VT_ANY, true};
        case ETypeName::Tuple:
            return {VT_ANY, true};
        case ETypeName::Variant:
            return {VT_ANY, true};
        case ETypeName::Tagged:
            return Simplify(type->AsTagged()->GetItemType());
    }
    ythrow TApiUsageError() << "Unsupported type: " << typeName;
}

NTi::TTypePtr ToTypeV3(EValueType type, bool required)
{
    auto typeV3 = OldTypeToTypeV3(type);
    if (!Simplify(typeV3).second) {
        if (required) {
            ythrow TApiUsageError() << "type: " << type << " cannot be required";
        } else {
            return typeV3;
        }
    }
    if (required) {
        return typeV3;
    } else {
        return NTi::Optional(typeV3);
    }
}

TColumnSchema::TColumnSchema()
    : TypeV3_(NTi::Optional(NTi::Int64()))
{ }

EValueType TColumnSchema::Type() const
{
    return Simplify(TypeV3_).first;
}

TColumnSchema& TColumnSchema::Type(EValueType type) &
{
    return Type(ToTypeV3(type, false));
}

TColumnSchema TColumnSchema::Type(EValueType type) &&
{
    return Type(ToTypeV3(type, false));
}

TColumnSchema& TColumnSchema::Type(const NTi::TTypePtr& type) &
{
    Y_VERIFY(type.Get(), "Cannot create column schema with nullptr type");
    TypeV3_ = type;
    return *this;
}

TColumnSchema TColumnSchema::Type(const NTi::TTypePtr& type) &&
{
    Y_VERIFY(type.Get(), "Cannot create column schema with nullptr type");
    TypeV3_ = type;
    return *this;
}

TColumnSchema& TColumnSchema::TypeV3(const NTi::TTypePtr& type) &
{
    return Type(type);
}

TColumnSchema TColumnSchema::TypeV3(const NTi::TTypePtr& type) &&
{
    return Type(type);
}

NTi::TTypePtr TColumnSchema::TypeV3() const
{
    return TypeV3_;
}

bool TColumnSchema::Required() const
{
    return Simplify(TypeV3_).second;
}

TColumnSchema& TColumnSchema::Type(EValueType type, bool required) &
{
    return Type(ToTypeV3(type, required));
}

TColumnSchema TColumnSchema::Type(EValueType type, bool required) &&
{
    return Type(ToTypeV3(type, required));
}

bool operator==(const TColumnSchema& lhs, const TColumnSchema& rhs)
{
    return
        lhs.Name() == rhs.Name() &&
        NTi::NEq::TStrictlyEqual()(lhs.TypeV3(), rhs.TypeV3()) &&
        lhs.SortOrder() == rhs.SortOrder() &&
        lhs.Lock() == rhs.Lock() &&
        lhs.Expression() == rhs.Expression() &&
        lhs.Aggregate() == rhs.Aggregate() &&
        lhs.Group() == rhs.Group();
}

bool operator!=(const TColumnSchema& lhs, const TColumnSchema& rhs)
{
    return !(lhs == rhs);
}

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

TTableSchema& TTableSchema::AddColumn(const TString& name, const NTi::TTypePtr& type) &
{
    Columns_.push_back(TColumnSchema().Name(name).Type(type));
    return *this;
}

TTableSchema TTableSchema::AddColumn(const TString& name, const NTi::TTypePtr& type) &&
{
    return std::move(AddColumn(name, type));
}

TTableSchema& TTableSchema::AddColumn(const TString& name, const NTi::TTypePtr& type, ESortOrder sortOrder) &
{
    Columns_.push_back(TColumnSchema().Name(name).Type(type).SortOrder(sortOrder));
    return *this;
}

TTableSchema TTableSchema::AddColumn(const TString& name, const NTi::TTypePtr& type, ESortOrder sortOrder) &&
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
        if (auto it = columnsIndex.find(column.Name())) {
            column.SortOrder(ESortOrder::SO_ASCENDING);
            newColumns[it->second] = std::move(column); // see newColumns.resize() above
            columnsIndex.erase(it);
        } else {
            column.ResetSortOrder();
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

TVector<TColumnSchema>& TTableSchema::MutableColumns()
{
    return Columns_;
}

TNode TTableSchema::ToNode() const
{
    TNode result;
    TNodeBuilder builder(&result);
    Serialize(*this, &builder);
    return result;
}

bool operator==(const TTableSchema& lhs, const TTableSchema& rhs)
{
    return
        lhs.Columns() == rhs.Columns() &&
        lhs.Strict() == rhs.Strict() &&
        lhs.UniqueKeys() == rhs.UniqueKeys();
}

bool operator!=(const TTableSchema& lhs, const TTableSchema& rhs)
{
    return !(lhs == rhs);
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
    return !readLimit.Key_ && !readLimit.RowIndex_ && !readLimit.Offset_ && !readLimit.TabletIndex_;
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
        case VT_INT8:
            return "int8";
        case VT_INT16:
            return "int16";
        case VT_INT32:
            return "int32";
        case VT_INT64:
            return "int64";

        case VT_UINT8:
            return "uint8";
        case VT_UINT16:
            return "uint16";
        case VT_UINT32:
            return "uint32";
        case VT_UINT64:
            return "uint64";

        case VT_DOUBLE:
            return "double";

        case VT_BOOLEAN:
            return "boolean";

        case VT_STRING:
            return "string";
        case VT_UTF8:
            return "utf8";

        case VT_ANY:
            return "any";

        case VT_NULL:
            return "null";
        case VT_VOID:
            return "void";

        case VT_DATE:
            return "date";
        case VT_DATETIME:
            return "datetime";
        case VT_TIMESTAMP:
            return "timestamp";
        case VT_INTERVAL:
            return "interval";

        case VT_FLOAT:
            return "float";

        case VT_JSON:
            return "json";
    }
    ythrow yexception() << "Invalid value type " << static_cast<int>(type);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NDetail
} // namespace NYT
