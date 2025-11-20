#include "schema.h"

#include <yt/yt/client/table_client/columnar_statistics.h>
#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/row_base.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/unversioned_row.h>

#include <contrib/libs/apache/arrow_next/cpp/src/parquet/metadata.h>

namespace NYT::NArrow {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

NTableClient::TLogicalTypePtr GetLogicalTypeFromArrowType(const std::shared_ptr<arrow20::Field>& arrowType);

////////////////////////////////////////////////////////////////////////////////

NTableClient::TLogicalTypePtr GetLogicalTypeFromArrowType(const std::shared_ptr<arrow20::DataType>& arrowType)
{
    using namespace NTableClient;
    switch (arrowType->id()) {
        case arrow20::Type::BOOL:
            return SimpleLogicalType(ESimpleLogicalValueType::Boolean);
        case arrow20::Type::UINT8:
            return SimpleLogicalType(ESimpleLogicalValueType::Uint8);
        case arrow20::Type::UINT16:
            return SimpleLogicalType(ESimpleLogicalValueType::Uint16);
        case arrow20::Type::UINT32:
            return SimpleLogicalType(ESimpleLogicalValueType::Uint32);
        case arrow20::Type::UINT64:
            return SimpleLogicalType(ESimpleLogicalValueType::Uint64);
        case arrow20::Type::INT8:
            return SimpleLogicalType(ESimpleLogicalValueType::Int8);
        case arrow20::Type::INT16:
            return SimpleLogicalType(ESimpleLogicalValueType::Int16);
        case arrow20::Type::DATE32:
        case arrow20::Type::TIME32:
        case arrow20::Type::INT32:
            return SimpleLogicalType(ESimpleLogicalValueType::Int32);
        case arrow20::Type::DATE64:
        case arrow20::Type::TIMESTAMP:
        case arrow20::Type::INT64:
        case arrow20::Type::TIME64:
            return SimpleLogicalType(ESimpleLogicalValueType::Int64);
        case arrow20::Type::HALF_FLOAT:
        case arrow20::Type::FLOAT:
            return SimpleLogicalType(ESimpleLogicalValueType::Float);
        case arrow20::Type::DOUBLE:
            return SimpleLogicalType(ESimpleLogicalValueType::Double);
        case arrow20::Type::STRING:
            return SimpleLogicalType(ESimpleLogicalValueType::Utf8);
        case arrow20::Type::BINARY:
        case arrow20::Type::FIXED_SIZE_BINARY:
            return SimpleLogicalType(ESimpleLogicalValueType::String);
        case arrow20::Type::LIST:
            return ListLogicalType(
                GetLogicalTypeFromArrowType(std::reinterpret_pointer_cast<arrow20::ListType>(arrowType)->value_field()));
        case arrow20::Type::MAP:
            return DictLogicalType(
                GetLogicalTypeFromArrowType(std::reinterpret_pointer_cast<arrow20::MapType>(arrowType)->key_field()),
                GetLogicalTypeFromArrowType(std::reinterpret_pointer_cast<arrow20::MapType>(arrowType)->item_field()));

        case arrow20::Type::STRUCT:
        {
            auto structType = std::reinterpret_pointer_cast<arrow20::StructType>(arrowType);
            std::vector<TStructField> members;
            members.reserve(structType->num_fields());
            for (auto fieldIndex = 0; fieldIndex < structType->num_fields(); ++fieldIndex) {
                auto field = structType->field(fieldIndex);
                members.push_back({TString(field->name()), GetLogicalTypeFromArrowType(field)});
            }
            return StructLogicalType(std::move(members));
        }
        // Currently YT supports only Decimal128 with precision <= 35. Thus, we represent short enough arrow decimal types
        // as the corresponding YT decimals, and longer arrow decimal types as strings in decimal form.
        // The latter is subject to change whenever wider decimal types are introduced in YT.
        case arrow20::Type::DECIMAL128:
        {
            constexpr int MaximumYTDecimalPrecision = 35;
            auto decimalType = std::reinterpret_pointer_cast<arrow20::Decimal128Type>(arrowType);
            if (decimalType->precision() <= MaximumYTDecimalPrecision) {
                return DecimalLogicalType(decimalType->precision(), decimalType->scale());
            } else {
                return SimpleLogicalType(ESimpleLogicalValueType::String);
            }
        }
        case arrow20::Type::DECIMAL256:
            return SimpleLogicalType(ESimpleLogicalValueType::String);
        default:
            THROW_ERROR_EXCEPTION("Unsupported arrow type: %Qv", arrowType->ToString());
    }
}

NTableClient::TLogicalTypePtr GetLogicalTypeFromArrowType(const std::shared_ptr<arrow20::Field>& arrowField)
{
    auto resultType = GetLogicalTypeFromArrowType(arrowField->type());
    return arrowField->nullable() ? OptionalLogicalType(resultType) : resultType;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

NTableClient::TTableSchemaPtr CreateYTTableSchemaFromArrowSchema(
    const std::shared_ptr<arrow20::Schema>& arrowSchema)
{
    std::vector<TColumnSchema> columns;
    for(const auto& field : arrowSchema->fields()) {
        columns.push_back(TColumnSchema(TString(field->name()), GetLogicalTypeFromArrowType(field)));
    }

    return New<TTableSchema>(
        std::move(columns),
        /*strict*/ true,
        /*uniqueKeys*/ false);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NArrow
