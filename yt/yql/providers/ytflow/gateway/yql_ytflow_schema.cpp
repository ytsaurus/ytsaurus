#include "yql_ytflow_schema.h"

#include <library/cpp/yt/memory/new.h>

#include <yt/yt/client/table_client/comparator.h>
#include <yt/yt/client/table_client/logical_type.h>

#include <util/string/cast.h>
#include <util/system/yassert.h>


namespace NYql::NYtflow::NPrivate {

struct TConvertTypeVisitor: public TTypeAnnotationVisitor {
public:
#define VISIT_UNSUPPORTED(type) \
    void Visit(const type & /*type*/) override { \
        Y_ABORT("not supported type: " #type); \
    }

    VISIT_UNSUPPORTED(TUnitExprType)
    VISIT_UNSUPPORTED(TUniversalExprType)
    VISIT_UNSUPPORTED(TUniversalStructExprType)
    VISIT_UNSUPPORTED(TMultiExprType)
    VISIT_UNSUPPORTED(TStreamExprType)
    VISIT_UNSUPPORTED(TFlowExprType)
    VISIT_UNSUPPORTED(TPgExprType)
    VISIT_UNSUPPORTED(TWorldExprType)
    VISIT_UNSUPPORTED(TCallableExprType)
    VISIT_UNSUPPORTED(TResourceExprType)
    VISIT_UNSUPPORTED(TTypeExprType)
    VISIT_UNSUPPORTED(TGenericExprType)
    VISIT_UNSUPPORTED(TErrorExprType)
    VISIT_UNSUPPORTED(TEmptyListExprType)
    VISIT_UNSUPPORTED(TEmptyDictExprType)
    VISIT_UNSUPPORTED(TBlockExprType)
    VISIT_UNSUPPORTED(TScalarExprType)
    VISIT_UNSUPPORTED(TLinearExprType)
    VISIT_UNSUPPORTED(TDynamicLinearExprType)
#undef VISIT_UNSUPPORTED

    void Visit(const TTypeAnnotationNode& node) {
        node.Accept(*this);
    }

    void Visit(const TTupleExprType& type) override {
        std::vector<NYT::NTableClient::TLogicalTypePtr> items;
        items.reserve(type.GetItems().size());

        for (const auto* itemType: type.GetItems()) {
            Visit(*itemType);
            items.push_back(std::move(LogicalType));
        }

        LogicalType = NYT::NTableClient::TupleLogicalType(std::move(items));
    }

    void Visit(const TStructExprType& type) override {
        std::vector<NYT::NTableClient::TStructField> fields;
        fields.reserve(type.GetItems().size());

        for (const auto* itemType: type.GetItems()) {
            Visit(*itemType);
            auto name = itemType->GetName();
            fields.push_back(NYT::NTableClient::TStructField{
                .Name = std::string(name),
                .StableName = std::string(name),
                .Type = std::move(LogicalType)
            });
        }

        LogicalType = NYT::NTableClient::StructLogicalType(std::move(fields), /*removedFieldStableNames*/ {});
    }

    void Visit(const TItemExprType& type) override {
        Visit(*type.GetItemType());
    }

    void Visit(const TListExprType& type) override {
        Visit(*type.GetItemType());
        LogicalType = NYT::NTableClient::ListLogicalType(std::move(LogicalType));
    }

    void Visit(const TDataExprType& type) override {
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
    case EDataSlot::yql_type: \
        LogicalType = NYT::NTableClient::SimpleLogicalType( \
            NYT::NTableClient::ESimpleLogicalValueType::yt_type); \
        break;

        switch (type.GetSlot()) {
        YQL_TO_YT_TYPE_LIST(CASE)
        case EDataSlot::Decimal: {
            auto paramsType = static_cast<const TDataExprParamsType&>(type);
            LogicalType = NYT::NTableClient::DecimalLogicalType(
                FromString<ui8>(paramsType.GetParamOne()),
                FromString<ui8>(paramsType.GetParamTwo()));
            break;
        }
        }

#undef CASE
#undef YQL_TO_YT_TYPE_LIST
    }

    void Visit(const TOptionalExprType& type) override {
        Visit(*type.GetItemType());
        LogicalType = NYT::NTableClient::OptionalLogicalType(std::move(LogicalType));
    }

    void Visit(const TDictExprType& type) override {
        Visit(*type.GetKeyType());
        auto keyType = std::move(LogicalType);

        Visit(*type.GetPayloadType());
        auto valueType = std::move(LogicalType);

        LogicalType = NYT::NTableClient::DictLogicalType(
            std::move(keyType), std::move(valueType));
    }

    void Visit(const TVoidExprType& /*type*/) override {
        LogicalType = NYT::NTableClient::SimpleLogicalType(
            NYT::NTableClient::ESimpleLogicalValueType::Void);
    }

    void Visit(const TNullExprType& /*type*/) override {
        LogicalType = NYT::NTableClient::NullLogicalType();
    }

    void Visit(const TTaggedExprType& type) override {
        Visit(*type.GetBaseType());
        LogicalType = NYT::NTableClient::TaggedLogicalType(
            TString(type.GetTag()), std::move(LogicalType));
    }

    void Visit(const TVariantExprType& type) override {
        auto* underlyingType = type.GetUnderlyingType();
        Visit(*underlyingType);

        switch (underlyingType->GetKind()) {
        case ETypeAnnotationKind::Tuple: {
            const auto& tupleLogicalType = LogicalType->AsTupleTypeRef();
            LogicalType = NYT::NTableClient::VariantTupleLogicalType(
                tupleLogicalType.GetElements());

            break;
        }

        case ETypeAnnotationKind::Struct: {
            const auto& structLogicalType = LogicalType->AsStructTypeRef();
            LogicalType = NYT::NTableClient::VariantStructLogicalType(structLogicalType.GetFields());

            break;
        }

        default:
            YQL_ENSURE(
                false, "Unexpected underlying variant type: "
                << underlyingType->GetKind());

            break;
        }
    }

public:
    NYT::NTableClient::TLogicalTypePtr LogicalType;
};

} // namespace NYql::NYtflow::NPrivate

namespace NYql::NYtflow {

NYT::NTableClient::TTableSchemaPtr BuildTableSchema(const TTypeAnnotationNode* type) {
    auto* structType = type->Cast<TStructExprType>();

    std::vector<NYT::NTableClient::TColumnSchema> columns;
    columns.reserve(structType->GetItems().size());

    NPrivate::TConvertTypeVisitor visitor;
    for (auto* itemType: structType->GetItems()) {
        visitor.Visit(*itemType->GetItemType());

        auto name = itemType->GetName();

        columns.push_back(NYT::NTableClient::TColumnSchema()
            .SetName(std::string(name))
            .SetStableName(NYT::NTableClient::TColumnStableName(std::string(name)))
            .SetLogicalType(std::move(visitor.LogicalType)));
    }

    auto tableSchema = NYT::New<NYT::NTableClient::TTableSchema>(std::move(columns));

    return tableSchema;
}

NYT::NTableClient::TTableSchemaPtr ConvertToQueueWriteSchema(
    NYT::NTableClient::TTableSchemaPtr tableSchema)
{
    return tableSchema->ToWrite()->ToCreate();
}

NYT::NTableClient::TTableSchemaPtr ConvertToQueueCreateSchema(
    NYT::NTableClient::TTableSchemaPtr tableSchema)
{
    auto createTableSchema = tableSchema->ToWrite()->ToCreate();

    auto columns = createTableSchema->Columns();

    columns.push_back(NYT::NTableClient::TColumnSchema(
        "$timestamp",
        NYT::NTableClient::ESimpleLogicalValueType::Uint64));

    columns.push_back(NYT::NTableClient::TColumnSchema(
        "$cumulative_data_weight",
        NYT::NTableClient::ESimpleLogicalValueType::Int64));

    return NYT::New<NYT::NTableClient::TTableSchema>(
        std::move(columns),
        createTableSchema->IsStrict(),
        createTableSchema->IsUniqueKeys());
}

NYT::NTableClient::TTableSchemaPtr ConvertToSortedTableCreateSchema(
    NYT::NTableClient::TTableSchemaPtr tableSchema,
    const TVector<TString>& keyColumns)
{
    YQL_ENSURE(!keyColumns.empty(), "Sorted table schema requires a non-empty key");

    auto createTableSchema = tableSchema->ToWrite()->ToCreate();

    std::vector<NYT::NTableClient::TColumnSchema> columns;
    columns.reserve(createTableSchema->GetColumnCount());

    for (const auto& keyColumn : keyColumns) {
        const auto* column = createTableSchema->FindColumn(keyColumn);
        YQL_ENSURE(column, "Unknown sort column: " << keyColumn);

        columns.push_back(NYT::NTableClient::TColumnSchema(*column)
            .SetSortOrder(NYT::NTableClient::ESortOrder::Ascending));
    }

    for (const auto& column : createTableSchema->Columns()) {
        if (!FindPtr(keyColumns, TString(column.Name()))) {
            columns.push_back(column);
        }
    }

    return NYT::New<NYT::NTableClient::TTableSchema>(
        std::move(columns),
        createTableSchema->IsStrict(),
        /*uniqueKeys*/ true);
}

} // namespace NYql::NYtflow
