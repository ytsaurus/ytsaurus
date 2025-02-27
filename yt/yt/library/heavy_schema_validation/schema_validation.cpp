#include "schema_validation.h"

// TODO(sandello,lukyan): Refine these dependencies.
#include <yt/yt/library/query/base/query_preparer.h>
#include <yt/yt/library/query/base/functions.h>

#include <yt/yt/client/table_client/column_sort_schema.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/logical_type.h>

#include <yt/yt/client/complex_types/check_type_compatibility.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NTableClient {

using namespace NYTree;
using namespace NQueryClient;
using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////
//! Validates the column schema update.
/*!
 *  \pre{oldColumn and newColumn should have the same stable name.}
 *
 *  Validates that:
 *  - New column type is compatible with the old one.
 *  - Optional column doesn't become required.
 *  - Column expression remains the same.
 *  - Column materialized remains the same if column expression is present.
 *  - Column aggregate method either was introduced or remains the same.
 *  - Column sort order either changes to std::nullopt or remains the same.
 */
void ValidateColumnSchemaUpdate(const TColumnSchema& oldColumn, const TColumnSchema& newColumn)
{
    YT_VERIFY(oldColumn.StableName() == newColumn.StableName());

    auto compatibility = NComplexTypes::CheckTypeCompatibility(
        oldColumn.LogicalType(),
        newColumn.LogicalType());

    if (compatibility.first != ESchemaCompatibility::FullyCompatible) {
        THROW_ERROR_EXCEPTION(NTableClient::EErrorCode::IncompatibleSchemas, "Type mismatch for column %v",
            oldColumn.GetDiagnosticNameString())
            << compatibility.second;
    }

    if (newColumn.SortOrder().operator bool() && newColumn.SortOrder() != oldColumn.SortOrder()) {
        THROW_ERROR_EXCEPTION(NTableClient::EErrorCode::IncompatibleSchemas, "Sort order mismatch for column %v: old %Qlv, new %Qlv",
            oldColumn.GetDiagnosticNameString(),
            oldColumn.SortOrder(),
            newColumn.SortOrder());
    }

    if (newColumn.Expression() != oldColumn.Expression()) {
        THROW_ERROR_EXCEPTION(NTableClient::EErrorCode::IncompatibleSchemas, "Expression mismatch for column %v: old %Qv, new %Qv",
            oldColumn.GetDiagnosticNameString(),
            oldColumn.Expression(),
            newColumn.Expression());
    }

    if (newColumn.Expression()) {
        if (newColumn.Materialized().value_or(true) != oldColumn.Materialized().value_or(true)) {
            THROW_ERROR_EXCEPTION(NTableClient::EErrorCode::IncompatibleSchemas, "Materialization mode mismatch for column %v: old %Qv, new %Qv",
                oldColumn.GetDiagnosticNameString(),
                oldColumn.Materialized().value_or(true),
                newColumn.Materialized().value_or(true));
        }
    }

    if (oldColumn.Aggregate() && oldColumn.Aggregate() != newColumn.Aggregate()) {
        THROW_ERROR_EXCEPTION(NTableClient::EErrorCode::IncompatibleSchemas, "Aggregate mode mismatch for column %v: old %Qv, new %Qv",
            oldColumn.GetDiagnosticNameString(),
            oldColumn.Aggregate(),
            newColumn.Aggregate());
    }

    if (oldColumn.SortOrder() && oldColumn.Lock() != newColumn.Lock()) {
        THROW_ERROR_EXCEPTION(NTableClient::EErrorCode::IncompatibleSchemas, "Lock mismatch for key column %v: old %Qv, new %Qv",
            oldColumn.GetDiagnosticNameString(),
            oldColumn.Lock(),
            newColumn.Lock());
    }

    if (oldColumn.MaxInlineHunkSize() && !newColumn.MaxInlineHunkSize()) {
        THROW_ERROR_EXCEPTION(NTableClient::EErrorCode::IncompatibleSchemas, "Cannot reset max inline hunk size for column %v",
            oldColumn.GetDiagnosticNameString());
    }
}

////////////////////////////////////////////////////////////////////////////////

//! Validates that all columns from the old schema are present in the new schema,
//! potentially among the deleted ones.
static void ValidateColumnRemoval(
    const TTableSchema& oldSchema,
    const TTableSchema& newSchema,
    TSchemaUpdateEnabledFeatures enabledFeatures,
    bool isTableDynamic)
{
    YT_VERIFY(newSchema.GetStrict());
    for (const auto& oldColumn : oldSchema.Columns()) {
        if (newSchema.FindColumnByStableName(oldColumn.StableName())) {
            continue;
        }

        if (!enabledFeatures.EnableStaticTableDropColumn && !isTableDynamic ||
            !enabledFeatures.EnableDynamicTableDropColumn && isTableDynamic)
        {
            THROW_ERROR_EXCEPTION("Cannot remove column %v from a strict schema",
                oldColumn.GetDiagnosticNameString());
        }

        if (!newSchema.FindDeletedColumn(oldColumn.StableName())) {
            THROW_ERROR_EXCEPTION(
                "Column %v is missing in strict schema; to remove a column, one must annotate it with \"deleted\" flag "
                "while keeping it in schema",
                oldColumn.GetDiagnosticNameString());
        }

        if (oldColumn.SortOrder() && newSchema.FindDeletedColumn(oldColumn.StableName())) {
            THROW_ERROR_EXCEPTION("Key column %v may not be deleted",
                oldColumn.GetDiagnosticNameString());
        }
    }
    if (!newSchema.DeletedColumns().empty() && !enabledFeatures.EnableDynamicTableDropColumn) {
        auto getDeletedColumnNamesAttribute = [&] {
            std::vector<TColumnStableName> deletedColumnNames;
            for (const auto& deletedColumn : newSchema.DeletedColumns()) {
                deletedColumnNames.push_back(deletedColumn.StableName());
            }
            return TErrorAttribute("deleted_columns", deletedColumnNames);
        };
        if (!isTableDynamic && !enabledFeatures.EnableStaticTableDropColumn) {
            THROW_ERROR_EXCEPTION("Deleting columns is not allowed on a static table")
                << getDeletedColumnNamesAttribute();
        }
        if (isTableDynamic && !enabledFeatures.EnableDynamicTableDropColumn) {
            THROW_ERROR_EXCEPTION("Deleting columns is not allowed on a dynamic table")
                << getDeletedColumnNamesAttribute();
        }
    }
    for (const auto& oldDeletedColumn : oldSchema.DeletedColumns()) {
        if (!newSchema.FindDeletedColumn(oldDeletedColumn.StableName())) {
            THROW_ERROR_EXCEPTION("Deleted column %Qv must remain in the deleted column list",
                oldDeletedColumn.StableName());
        }
    }
}

//! Validates that all columns from the new schema are present in the old schema.
void ValidateColumnsNotInserted(const TTableSchema& oldSchema, const TTableSchema& newSchema)
{
    YT_VERIFY(!oldSchema.GetStrict());
    for (const auto& newColumn : newSchema.Columns()) {
        if (!oldSchema.FindColumnByStableName(newColumn.StableName())) {
            THROW_ERROR_EXCEPTION("Cannot insert a new column %v into non-strict schema",
                newColumn.GetDiagnosticNameString());
        }
    }
}

//! Validates that table schema columns match.
/*!
 *  Validates that:
 *  - For each column present in both #oldSchema and #newSchema, its declarations match each other.
 *  - Key columns are not removed (but they may become non-key).
 *  - If any key columns are removed, the unique_keys is set to false.
 */
void ValidateColumnsMatch(const TTableSchema& oldSchema, const TTableSchema& newSchema)
{
    int commonKeyColumnPrefix = 0;
    for (int oldColumnIndex = 0; oldColumnIndex < oldSchema.GetColumnCount(); ++oldColumnIndex) {
        const auto& oldColumn = oldSchema.Columns()[oldColumnIndex];
        const auto* newColumnPtr = newSchema.FindColumnByStableName(oldColumn.StableName());
        if (!newColumnPtr) {
            // We consider only columns present both in oldSchema and newSchema.
            continue;
        }
        const auto& newColumn = *newColumnPtr;
        ValidateColumnSchemaUpdate(oldColumn, newColumn);

        if (oldColumn.SortOrder() && newColumn.SortOrder()) {
            int newColumnIndex = newSchema.GetColumnIndex(newColumn);
            if (oldColumnIndex != newColumnIndex) {
                THROW_ERROR_EXCEPTION("Cannot change position of a key column %v: old %v, new %v",
                    oldColumn.GetDiagnosticNameString(),
                    oldColumnIndex,
                    newColumnIndex);
            }
            if (commonKeyColumnPrefix <= oldColumnIndex) {
                commonKeyColumnPrefix = oldColumnIndex + 1;
            }
        }
    }

    // Check that all columns from the commonKeyColumnPrefix in oldSchema are actually present in newSchema.
    for (int oldColumnIndex = 0; oldColumnIndex < commonKeyColumnPrefix; ++oldColumnIndex) {
        const auto& oldColumn = oldSchema.Columns()[oldColumnIndex];
        if (!newSchema.FindColumnByStableName(oldColumn.StableName())) {
            THROW_ERROR_EXCEPTION("Key column with %v is missing in new schema", oldColumn.GetDiagnosticNameString());
        }
    }

    if (commonKeyColumnPrefix < oldSchema.GetKeyColumnCount() && newSchema.GetUniqueKeys()) {
        THROW_ERROR_EXCEPTION("Table cannot have unique keys since some of its key columns were removed");
    }
}

void ValidateNoRequiredColumnsAdded(const TTableSchema& oldSchema, const TTableSchema& newSchema)
{
    for (const auto& newColumn : newSchema.Columns()) {
        if (newColumn.Required()) {
            const auto* oldColumn = oldSchema.FindColumnByStableName(newColumn.StableName());
            if (!oldColumn) {
                THROW_ERROR_EXCEPTION("Cannot insert a new required column %v into a non-empty table",
                    newColumn.GetDiagnosticNameString());
            }
        }
    }
}

void ValidateKeyColumnWasNotAlteredToAny(const TTableSchema& oldSchema, const TTableSchema& newSchema)
{
    for (int oldColumnIndex = 0; oldColumnIndex < oldSchema.GetKeyColumnCount(); ++oldColumnIndex) {
        const auto& oldColumn = oldSchema.Columns()[oldColumnIndex];

        const auto* newColumn = newSchema.FindColumnByStableName(oldColumn.StableName());

        if (!newColumn) {
            // Removing key columns from the end of key columns prefix when Strict = false is ok.
            // This case will be handled by other validation routines, just skip here.
            continue;
        }

        auto oldType = oldColumn.GetWireType();
        auto newType = newColumn->GetWireType();

        if ((oldType != EValueType::Any) && (newType == EValueType::Any)) {
            THROW_ERROR_EXCEPTION("Altering a key column type to \"any\" is forbidden")
                << TErrorAttribute("column_name", oldColumn.StableName());
        }
    }
}

static bool IsPhysicalType(ESimpleLogicalValueType logicalType)
{
    return static_cast<ui32>(logicalType) == static_cast<ui32>(GetPhysicalType(logicalType));
}

//! Validates aggregated columns.
/*!
 *  Validates that:
 *  - Aggregated columns are non-key.
 *  - Aggregate function appears in a list of pre-defined aggregate functions.
 *  - Type of an aggregated column matches the type of an aggregate function.
 */
void ValidateAggregatedColumns(const TTableSchema& schema)
{
    for (int index = 0; index < schema.GetColumnCount(); ++index) {
        const auto& columnSchema = schema.Columns()[index];
        if (columnSchema.Aggregate()) {
            if (index < schema.GetKeyColumnCount()) {
                THROW_ERROR_EXCEPTION("Key column %v cannot be aggregated", columnSchema.GetDiagnosticNameString());
            }

            auto aggregateName = *columnSchema.Aggregate();
            auto elementType = columnSchema.GetWireType();

            if (auto nested = TryParseNestedAggregate(aggregateName)) {
                if (nested->Aggregate.empty()) {
                    continue;
                }

                aggregateName = nested->Aggregate;
                elementType = GetNestedColumnElementType(columnSchema.LogicalType().Get());
            } else if (!columnSchema.IsOfV1Type() || !IsPhysicalType(columnSchema.CastToV1Type())) {
                THROW_ERROR_EXCEPTION("Aggregated column %v is forbidden to have logical type %Qlv",
                    columnSchema.GetDiagnosticNameString(),
                    *columnSchema.LogicalType());
            }

            auto typeInferrer = GetBuiltinTypeInferrers()->GetFunction(aggregateName);
            if (auto descriptor = typeInferrer->As<TAggregateFunctionTypeInferrer>()) {
                std::vector<TTypeSet> typeConstraints;
                std::vector<int> argumentIndexes;

                auto [_, resultIndex] = descriptor->GetNormalizedConstraints(
                    &typeConstraints,
                    &argumentIndexes);
                auto& resultConstraint = typeConstraints[resultIndex];

                if (!resultConstraint.Get(elementType)) {
                    THROW_ERROR_EXCEPTION("Aggregate function %Qv result type set %Qlv differs from column %v type %Qlv",
                        *columnSchema.Aggregate(),
                        resultConstraint,
                        columnSchema.GetDiagnosticNameString(),
                        elementType);
                }
            } else {
                THROW_ERROR_EXCEPTION("Unknown aggregate function %Qv at column %v",
                    *columnSchema.Aggregate(),
                    columnSchema.GetDiagnosticNameString());
            }
        }
    }
}

void ValidateComputedColumns(const TTableSchema& schema, bool isTableDynamic)
{
    for (int index = 0; index < schema.GetColumnCount(); ++index) {
        const auto& columnSchema = schema.Columns()[index];
        if (!columnSchema.Expression()) {
            if (columnSchema.Materialized().has_value()) {
                THROW_ERROR_EXCEPTION("Column %v has \"materialized\" parameter without \"expression\" parameter", columnSchema.GetDiagnosticNameString());
            }
            continue;
        }

        auto materialized = columnSchema.Materialized().value_or(true);
        if (materialized && index >= schema.GetKeyColumnCount() && isTableDynamic) {
            THROW_ERROR_EXCEPTION("Non-key column %v cannot be computed in materializable way", columnSchema.GetDiagnosticNameString());
        } else if (!materialized && index < schema.GetKeyColumnCount()) {
            THROW_ERROR_EXCEPTION("Key column %v cannot be computed in non-materializable way", columnSchema.GetDiagnosticNameString());
        }

        THashSet<std::string> references;
        // TODO(babenko): migrate to std::string
        auto expr = PrepareExpression(*columnSchema.Expression(), schema, GetBuiltinTypeInferrers(), &references);
        if (*columnSchema.LogicalType() != *expr->LogicalType) {
            THROW_ERROR_EXCEPTION(
                "Computed column %v type mismatch: declared type is %Qlv but expression type is %Qlv",
                columnSchema.GetDiagnosticNameString(),
                *columnSchema.LogicalType(),
                *expr->LogicalType);
        }

        for (const auto& ref : references) {
            const auto* refColumn = schema.FindColumn(ref);
            if (!refColumn) {
                THROW_ERROR_EXCEPTION("Computed column %v depends on unknown column %Qv",
                    columnSchema.GetDiagnosticNameString(),
                    ref);
            }
            if (!refColumn->SortOrder() && isTableDynamic) {
                THROW_ERROR_EXCEPTION("Computed column %v depends on a non-key column %v",
                    columnSchema.GetDiagnosticNameString(),
                    refColumn->GetDiagnosticNameString());
            }
            if (refColumn->Expression()) {
                THROW_ERROR_EXCEPTION("Computed column %v depends on a computed column %v",
                    columnSchema.GetDiagnosticNameString(),
                    refColumn->GetDiagnosticNameString());
            }
        }
    }
}

//! TODO(max42): document this functions somewhere (see also https://st.yandex-team.ru/YT-1433).
void ValidateTableSchemaUpdateInternal(
    const TTableSchema& oldSchema,
    const TTableSchema& newSchema,
    TSchemaUpdateEnabledFeatures enabledFeatures,
    bool isTableDynamic,
    bool isTableEmpty,
    bool allowAlterKeyColumnToAny)
{
    try {
        ValidateTableSchemaHeavy(newSchema, isTableDynamic);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION(NTableClient::EErrorCode::InvalidSchemaValue, "New table schema is not valid")
            << TErrorAttribute("old_schema", oldSchema)
            << TErrorAttribute("new_schema", newSchema)
            << ex;
    }

    try {
        if (!allowAlterKeyColumnToAny) {
            // This is forbidden even for empty static tables.
            ValidateKeyColumnWasNotAlteredToAny(oldSchema, newSchema);
        }

        if (isTableEmpty) {
            // Any valid schema is allowed to be set for an empty table.
            return;
        }

        if (isTableDynamic && oldSchema.IsSorted() != newSchema.IsSorted()) {
            THROW_ERROR_EXCEPTION("Cannot change dynamic table type from sorted to ordered or vice versa");
        }

        if (oldSchema.GetKeyColumnCount() == 0 && newSchema.GetKeyColumnCount() > 0) {
            THROW_ERROR_EXCEPTION("Cannot change schema from unsorted to sorted");
        }
        if (!oldSchema.GetStrict() && newSchema.GetStrict()) {
            THROW_ERROR_EXCEPTION("Changing \"strict\" from \"false\" to \"true\" is not allowed");
        }
        if (!oldSchema.GetUniqueKeys() && newSchema.GetUniqueKeys()) {
            THROW_ERROR_EXCEPTION("Changing \"unique_keys\" from \"false\" to \"true\" is not allowed");
        }

        if (oldSchema.GetStrict() && !newSchema.GetStrict()) {
            if (oldSchema.Columns() != newSchema.Columns()) {
                THROW_ERROR_EXCEPTION("Changing columns is not allowed while changing \"strict\" from \"true\" to \"false\"");
            }
            return;
        }

        if (oldSchema.GetStrict()) {
            ValidateColumnRemoval(oldSchema, newSchema, enabledFeatures, isTableDynamic);
        } else {
            ValidateColumnsNotInserted(oldSchema, newSchema);
        }
        ValidateColumnsMatch(oldSchema, newSchema);

        // We allow adding computed columns only on creation of the table.
        if (!oldSchema.Columns().empty() || !isTableEmpty) {
            for (const auto& newColumn : newSchema.Columns()) {
                if (newColumn.Expression() && !oldSchema.FindColumnByStableName(newColumn.StableName())) {
                    THROW_ERROR_EXCEPTION("Cannot introduce a new computed column %v after creation",
                        newColumn.GetDiagnosticNameString());
                }
            }
        }

        ValidateNoRequiredColumnsAdded(oldSchema, newSchema);
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION(NTableClient::EErrorCode::IncompatibleSchemas, "Table schemas are incompatible")
            << TErrorAttribute("old_schema", oldSchema)
            << TErrorAttribute("new_schema", newSchema)
            << ex;
    }
}

////////////////////////////////////////////////////////////////////////////////

void ValidateTableSchemaUpdate(
    const TTableSchema& oldSchema,
    const TTableSchema& newSchema,
    bool isTableDynamic,
    bool isTableEmpty)
{
    ValidateTableSchemaUpdateInternal(
        oldSchema,
        newSchema,
        TSchemaUpdateEnabledFeatures{
            false,  /* EnableStaticTableDropColumn */
            false  /* EnableDynamicTableDropColumn */
        },
        isTableDynamic,
        isTableEmpty);
}

////////////////////////////////////////////////////////////////////////////////


void ValidateTableSchemaHeavy(
    const TTableSchema& schema,
    bool isTableDynamic)
{
    ValidateTableSchema(schema, isTableDynamic);
    ValidateComputedColumns(schema, isTableDynamic);
    ValidateAggregatedColumns(schema);
}

////////////////////////////////////////////////////////////////////////////////

TError ValidateComputedColumnsCompatibility(
    const TTableSchema& inputSchema,
    const TTableSchema& outputSchema)
{
    try {
        for (const auto& outputColumn : outputSchema.Columns()) {
            if (!outputColumn.Expression()) {
                continue;
            }
            const auto* inputColumn = inputSchema.FindColumn(outputColumn.Name());
            if (!inputColumn) {
                continue;
            }
            if (outputColumn.Expression() != inputColumn->Expression()) {
                THROW_ERROR_EXCEPTION("Computed column %v has different expressions in input "
                    "and output schemas",
                    outputColumn.GetDiagnosticNameString())
                    << TErrorAttribute("input_schema_expression", inputColumn->Expression())
                    << TErrorAttribute("output_schema_expression", outputColumn.Expression());
            }
            if (*outputColumn.LogicalType() != *inputColumn->LogicalType()) {
                THROW_ERROR_EXCEPTION("Computed column %v type in the input table %Qlv "
                    "differs from the type in the output table %Qlv",
                    outputColumn.GetDiagnosticNameString(),
                    *inputColumn->LogicalType(),
                    *outputColumn.LogicalType());
            }
        }
    } catch (const TErrorException& exception) {
        return exception.Error()
            << TErrorAttribute("input_table_schema", inputSchema)
            << TErrorAttribute("output_table_schema", outputSchema);
    }

    return TError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
