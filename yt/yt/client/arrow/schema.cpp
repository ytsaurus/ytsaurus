#include "schema.h"

#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/row_base.h>
#include <yt/yt/client/table_client/schema.h>

namespace NYT::NArrow {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

NTableClient::TLogicalTypePtr GetLogicalTypeFromArrowType(const std::shared_ptr<arrow::DataType>& arrowType)
{
    using namespace NTableClient;
    switch (arrowType->id()) {
        case arrow::Type::BOOL:
            return SimpleLogicalType(ESimpleLogicalValueType::Boolean);
        case arrow::Type::UINT8:
            return SimpleLogicalType(ESimpleLogicalValueType::Uint8);
        case arrow::Type::UINT16:
            return SimpleLogicalType(ESimpleLogicalValueType::Uint16);
        case arrow::Type::UINT32:
            return SimpleLogicalType(ESimpleLogicalValueType::Uint32);
        case arrow::Type::UINT64:
            return SimpleLogicalType(ESimpleLogicalValueType::Uint64);
        case arrow::Type::INT8:
            return SimpleLogicalType(ESimpleLogicalValueType::Int8);
        case arrow::Type::INT16:
            return SimpleLogicalType(ESimpleLogicalValueType::Int16);
        case arrow::Type::INT32:
            return SimpleLogicalType(ESimpleLogicalValueType::Int32);
        case arrow::Type::INT64:
            return SimpleLogicalType(ESimpleLogicalValueType::Int64);
        case arrow::Type::FLOAT:
            return SimpleLogicalType(ESimpleLogicalValueType::Float);
        case arrow::Type::DOUBLE:
            return SimpleLogicalType(ESimpleLogicalValueType::Double);
        case arrow::Type::STRING:
            return SimpleLogicalType(ESimpleLogicalValueType::String);
        case arrow::Type::BINARY:
        case arrow::Type::FIXED_SIZE_BINARY:
            return SimpleLogicalType(ESimpleLogicalValueType::Any);
        // TODO(achulkov2): More types.
        default:
            THROW_ERROR_EXCEPTION("Unsupported arrow type: %Qv", arrowType->ToString());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

NTableClient::TTableSchemaPtr CreateYTTableSchemaFromArrowSchema(
    const std::shared_ptr<arrow::Schema>& arrowSchema)
{
    std::vector<TColumnSchema> columns;
    for(const auto& field : arrowSchema->fields()) {
        columns.push_back(
            TColumnSchema(
                field->name(),
                GetLogicalTypeFromArrowType(field->type()))
            .SetRequired(!field->nullable()));
    }

    return New<TTableSchema>(
        std::move(columns),
        /*strict*/ true,
        /*uniqueKeys*/ false);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NArrow
