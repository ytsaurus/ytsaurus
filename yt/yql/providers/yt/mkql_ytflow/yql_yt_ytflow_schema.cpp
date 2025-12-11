#include "yql_yt_ytflow_schema.h"

#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_node_visitor.h>
#include <yql/essentials/minikql/mkql_node_builder.h>

#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/schema.h>

#include <vector>


namespace NYql {

using namespace NKikimr::NMiniKQL;

namespace {

struct TConvertTypeNodeVisitor
    : public TThrowingNodeVisitor
{
public:
    using TThrowingNodeVisitor::Visit;

    void Visit(const TType& node) {
        const_cast<TType&>(node).Accept(*this);
    }

    void Visit(TVoidType& /*node*/) override {
        LogicalType = NYT::NTableClient::SimpleLogicalType(
            NYT::NTableClient::ESimpleLogicalValueType::Void);
    }

    void Visit(TNullType& /*node*/) override {
        LogicalType = NYT::NTableClient::NullLogicalType();
    }

    void Visit(TDataType& node) override {
#define YQL_TO_YT_TYPE_LIST(xx) \
    xx(String, String) \
    xx(JsonDocument, String) \
    xx(DyNumber, String) \
    xx(Uuid, Uuid) \
    xx(Json, Json) \
    xx(Utf8, Utf8) \
    xx(Int64, Int64) \
    xx(Int32, Int32) \
    xx(Int16, Int16) \
    xx(Int8, Int8) \
    xx(Uint64, Uint64) \
    xx(Uint32, Uint32) \
    xx(Uint16, Uint16) \
    xx(Uint8, Uint8) \
    xx(Double, Double) \
    xx(Float, Float) \
    xx(Bool, Boolean) \
    xx(Yson, Any) \
    xx(TzDate, String) \
    xx(TzDatetime, String) \
    xx(TzTimestamp, String) \
    xx(TzDate32, String) \
    xx(TzDatetime64, String) \
    xx(TzTimestamp64, String) \
    xx(Date, Date) \
    xx(Datetime, Datetime) \
    xx(Timestamp, Timestamp) \
    xx(Interval, Interval) \
    xx(Date32, Date32) \
    xx(Datetime64, Datetime64) \
    xx(Timestamp64, Timestamp64) \
    xx(Interval64, Interval64)

#define CASE(yql_type, yt_type) \
    case NUdf::EDataSlot::yql_type: \
        LogicalType = NYT::NTableClient::SimpleLogicalType( \
            NYT::NTableClient::ESimpleLogicalValueType::yt_type); \
        break;

        auto dataSlot = node.GetDataSlot();
        MKQL_ENSURE(dataSlot.Defined(), "Undefined data slot");

        switch (dataSlot.GetRef()) {
        YQL_TO_YT_TYPE_LIST(CASE)
        case NUdf::EDataSlot::Decimal: {
            auto& decimalType = static_cast<TDataDecimalType&>(node);
            auto [precision, scale] = decimalType.GetParams();
            LogicalType = NYT::NTableClient::DecimalLogicalType(precision, scale);
            break;
        }
        }

#undef CASE
#undef YQL_TO_YT_TYPE_LIST
    }

    void Visit(TStructType& node) override {
        std::vector<NYT::NTableClient::TStructField> fields;
        fields.reserve(node.GetMembersCount());

        for (ui32 memberIndex = 0; memberIndex < node.GetMembersCount(); ++memberIndex) {
            auto* memberType = node.GetMemberType(memberIndex);
            Visit(*memberType);

            auto name = node.GetMemberName(memberIndex);
            fields.push_back(NYT::NTableClient::TStructField{
                .Name = std::string(name),
                .StableName = std::string(name),
                .Type = std::move(LogicalType)
            });
        }

        LogicalType = NYT::NTableClient::StructLogicalType(
            std::move(fields),
            /*removedFieldStableNames*/ {});
    }

    void Visit(TListType& node) override {
        Visit(*node.GetItemType());
        LogicalType = NYT::NTableClient::ListLogicalType(std::move(LogicalType));
    }

    void Visit(TOptionalType& node) override {
        Visit(*node.GetItemType());
        LogicalType = NYT::NTableClient::OptionalLogicalType(std::move(LogicalType));
    }

    void Visit(TDictType& node) override {
        Visit(*node.GetKeyType());
        auto keyType = std::move(LogicalType);

        Visit(*node.GetPayloadType());
        auto valueType = std::move(LogicalType);

        LogicalType = NYT::NTableClient::DictLogicalType(
            std::move(keyType), std::move(valueType));
    }

    void Visit(TTupleType& node) override {
        std::vector<NYT::NTableClient::TLogicalTypePtr> items;
        items.reserve(node.GetElementsCount());

        for (
            ui32 elementIndex = 0;
            elementIndex < node.GetElementsCount();
            ++elementIndex
        ) {
            auto* elementType = node.GetElementType(elementIndex);
            Visit(*elementType);
            items.push_back(std::move(LogicalType));
        }

        LogicalType = NYT::NTableClient::TupleLogicalType(std::move(items));
    }

    void Visit(TVariantType& node) override {
        auto* underlyingType = node.GetUnderlyingType();
        Visit(*underlyingType);

        switch (underlyingType->GetKind()) {
        case TTypeBase::EKind::Tuple: {
            const auto& tupleLogicalType = LogicalType->AsTupleTypeRef();
            LogicalType = NYT::NTableClient::VariantTupleLogicalType(
                tupleLogicalType.GetElements());

            break;
        }

        case TTypeBase::EKind::Struct: {
            const auto& structLogicalType = LogicalType->AsStructTypeRef();
            LogicalType = NYT::NTableClient::VariantStructLogicalType(
                structLogicalType.GetFields());

            break;
        }

        default:
            MKQL_ENSURE(
                false, "Unexpected underlying variant type: "
                << underlyingType->GetKindAsStr());

            break;
        }
    }

    void Visit(TTaggedType& node) override {
        Visit(*node.GetBaseType());
        LogicalType = NYT::NTableClient::TaggedLogicalType(
            TString(node.GetTag()), std::move(LogicalType));
    }

public:
    NYT::NTableClient::TLogicalTypePtr LogicalType;
};

} // anonymous namespace

NYT::NTableClient::TLogicalTypePtr ConvertType(const TType* type) {
    TConvertTypeNodeVisitor visitor;
    visitor.Visit(*type);
    return std::move(visitor.LogicalType);
}

NYT::NTableClient::TLogicalTypePtr FilterType(
    NYT::NTableClient::TLogicalTypePtr structType,
    TVector<TString> filter
) {
    const auto& structTypeRef = structType->AsStructTypeRef();

    THashMap<TStringBuf, const NYT::NTableClient::TStructField*> fieldNameToField;
    for (const auto& field : structTypeRef.GetFields()) {
        NYT::EmplaceOrCrash(fieldNameToField, field.Name, &field);
    };

    auto fields = filter | std::views::transform([&] (const auto& fieldName) {
        return *fieldNameToField.at(fieldName);
    });
    return NYT::NTableClient::StructLogicalType(
        {fields.begin(), fields.end()},
        /*removedFieldStableNames*/ {});
}

const TType* FilterFields(
    const TType* structType,
    TVector<TString> filter,
    const TTypeEnvironment& typeEnv
) {
    MKQL_ENSURE(
        structType->IsStruct(),
        "Unexpected type: " << structType->GetKindAsStr());

    const auto* castedStructType = static_cast<const TStructType*>(structType);

    THashMap<TStringBuf, const TType*> memberTypes;
    for (
        ui32 memberIndex = 0;
        memberIndex < castedStructType->GetMembersCount();
        ++memberIndex
    ) {
        memberTypes.emplace(
            castedStructType->GetMemberName(memberIndex),
            castedStructType->GetMemberType(memberIndex));
    }

    TStructTypeBuilder structTypeBuilder(typeEnv);
    for (const auto& name : filter) {
        structTypeBuilder.Add(name, const_cast<TType*>(memberTypes.at(name)));
    }

    return structTypeBuilder.Build();
}

NYT::NTableClient::TLogicalTypePtr PartiallyReorderFields(
    NYT::NTableClient::TLogicalTypePtr structType,
    TVector<TString> partialOrder
) {
    const auto& structTypeRef = structType->AsStructTypeRef();

    THashMap<TStringBuf, const  NYT::NTableClient::TStructField*> fieldNameToField;
    for (const auto& field : structTypeRef.GetFields()) {
        fieldNameToField.emplace(field.Name, &field);
    };

    THashSet<TStringBuf> visitedFieldNames;

    std::vector<NYT::NTableClient::TStructField> fields;
    fields.reserve(fieldNameToField.size());

    for (const auto& name : partialOrder) {
        fields.push_back(*fieldNameToField.at(name));
        visitedFieldNames.emplace(std::move(name));
    }

    for (const auto& field : structTypeRef.GetFields()) {
        if (!visitedFieldNames.contains(field.Name)) {
            fields.push_back(field);
            visitedFieldNames.emplace(field.Name);
        }
    }

    return NYT::NTableClient::StructLogicalType(std::move(fields), /*removedFieldStableNames*/ {});
}

NYT::NTableClient::TLogicalTypePtr RenameFields(
    NYT::NTableClient::TLogicalTypePtr structType,
    THashMap<TString, TString> renames
) {
    const auto& structTypeRef = structType->AsStructTypeRef();

    std::vector<NYT::NTableClient::TStructField> fields;
    fields.reserve(structTypeRef.GetFields().size());

    for (const auto& field : structTypeRef.GetFields()) {
        const auto& newName = renames.at(field.Name);
        fields.push_back({
            .Name = newName,
            .StableName = newName,
            .Type = field.Type,
        });
    }

    return NYT::NTableClient::StructLogicalType(std::move(fields), /*removedFieldStableNames*/ {});
}

NYT::NTableClient::TTableSchemaPtr BuildTableSchema(
    NYT::NTableClient::TLogicalTypePtr structType
) {
    const auto& structTypeRef = structType->AsStructTypeRef();

    std::vector<NYT::NTableClient::TColumnSchema> columns;
    columns.reserve(structTypeRef.GetFields().size());

    for (const auto& field : structTypeRef.GetFields()) {
        columns.push_back(NYT::NTableClient::TColumnSchema()
            .SetName(field.Name)
            .SetStableName(NYT::NTableClient::TColumnStableName(field.StableName))
            .SetLogicalType(field.Type));
    }

    auto tableSchema = NYT::New<NYT::NTableClient::TTableSchema>(std::move(columns));

    return tableSchema;
}

bool EqualTo(
    NYT::NTableClient::TNameTablePtr one,
    NYT::NTableClient::TNameTablePtr another
) {
    if (one->GetSize() != another->GetSize()) {
        return false;
    }

    for (const auto& name : one->GetNames()) {
        if (one->FindId(name) != another->FindId(name)) {
            return false;
        }
    }

    return true;
}

} // namespace NYql
