#include "schema.h"

#include "column_evaluator_cache.h"
#include "message.h"
#include "payload_converter.h"

#include <yt/yt/client/table_client/comparator.h>
#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/library/heavy_schema_validation/schema_validation.h>

#include <yt/yt/library/query/engine_api/column_evaluator.h>

namespace NYT::NFlow {

////////////////////////////////////////////////////////////////////////////////

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

// TODO(mikari): make flow specific schema validation.

void ValidateStreamSchema(const TTableSchema& schema)
{
    if (schema.IsSorted()) {
        THROW_ERROR_EXCEPTION("Stream schema should not be sorted");
    }
    if (schema.HasComputedColumns()) {
        THROW_ERROR_EXCEPTION("Stream schema should not have computable fields");
    }
    std::vector<TColumnSchema> columns;
    for (const auto& column : schema.Columns()) {
        if (!column.Name().starts_with('$')) {
            columns.push_back(column);
        }
    }
    auto testSchema = New<TTableSchema>(columns, false, false);
    ValidateTableSchemaHeavy(*testSchema, false);
}

void ValidateIsGroupable(const TTableSchema& schema, const TTableSchema& groupBySchema)
{
    for (const auto& column : groupBySchema.Columns()) {
        if (column.Expression()) {
            continue;
        }
        if (auto* mainColumn = schema.FindColumn(column.Name())) {
            if (*mainColumn->LogicalType() != *column.LogicalType() && *mainColumn->LogicalType() != *DenullifyLogicalType(column.LogicalType())) {
                THROW_ERROR_EXCEPTION("Column %Qv has inconsistent types in original and group-by schemas",
                        column.Name())
                    << TErrorAttribute("source_type", ToString(*mainColumn->LogicalType()))
                    << TErrorAttribute("group_by_type", ToString(*column.LogicalType()));
            }
        } else {
            THROW_ERROR_EXCEPTION("Source schema should contains non-expression group-by column %Qv",
                column.Name());
        }
    }
}

void ValidateSchemaExpressions(
    const NTableClient::TTableSchemaPtr& schema,
    const NQueryClient::IColumnEvaluatorCachePtr& evaluatorCache)
{
    if (!schema->HasComputedColumns()) {
        return;
    }

    for (const auto& column : schema->Columns()) {
        if (column.Expression()) {
            if (column.GetWireType() != EValueType::Uint64) {
                THROW_ERROR_EXCEPTION(
                    "Expression column %Qv must have type uint64, got %Qlv",
                    column.Name(),
                    column.GetWireType());
            }
            if (!column.Required()) {
                THROW_ERROR_EXCEPTION(
                    "Expression column %Qv must be required",
                    column.Name());
            }
        }
    }

    auto cache = evaluatorCache ? evaluatorCache : CreateFastColumnEvaluatorCache();

    try {
        // Throws if any expression references a column not in the schema.
        auto evaluator = cache->Find(schema);
        YT_VERIFY(evaluator != nullptr);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Invalid expression in schema")
            << TErrorAttribute("schema", ToString(*schema))
            << ex;
    }
}

void ValidateGroupBySchema(
    const NTableClient::TTableSchemaPtr& schema,
    const NQueryClient::IColumnEvaluatorCachePtr& evaluatorCache)
{
    ValidateSchemaExpressions(schema, evaluatorCache);

    for (const auto& column : schema->Columns()) {
        if (column.IsOfV1Type(ESimpleLogicalValueType::Double)) {
            THROW_ERROR_EXCEPTION("Group-by schema column %Qv has type \"double\" which is not allowed",
                column.Name());
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TPayload ConvertPayloadToNewSchema(
    const TPayload& payload,
    const NTableClient::TTableSchemaPtr& sourceSchema,
    const NTableClient::TTableSchemaPtr& targetSchema,
    const IPayloadConverterCachePtr& converterCache)
{
    YT_VERIFY(sourceSchema);
    YT_VERIFY(targetSchema);

    if (sourceSchema == targetSchema) {
        return payload;
    }

    return converterCache->Convert(payload, sourceSchema, targetSchema);
}

TMessage ConvertMessageToNewSchema(
    const TMessage& message,
    const NTableClient::TTableSchemaPtr& targetSchema,
    const IPayloadConverterCachePtr& converterCache)
{
    return {
        {
            .MessageId = message.MessageId,
            .SystemTimestamp = message.SystemTimestamp,
            .EventTimestamp = message.EventTimestamp,
            .StreamId = message.StreamId,
        },
        ConvertPayloadToNewSchema(message.Payload, message.PayloadSchema, targetSchema, converterCache),
        targetSchema};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
