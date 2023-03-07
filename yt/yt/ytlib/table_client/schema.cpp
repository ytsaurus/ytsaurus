#include "schema.h"

// TODO(sandello,lukyan): Refine these dependencies.
#include <yt/ytlib/query_client/query_preparer.h>
#include <yt/ytlib/query_client/functions.h>

#include <yt/core/ytree/convert.h>

namespace NYT::NTableClient {

using namespace NYTree;
using namespace NQueryClient;
using namespace NChunkClient;
using namespace NTabletClient;

////////////////////////////////////////////////////////////////////////////////
//! Validates the column schema update.
/*!
 *  \pre{oldColumn and newColumn should have the same name.}
 *
 *  Validates that:
 *  - Column type remains the same.
 *  - Optional column doesn't become required.
 *  - Column expression remains the same.
 *  - Column aggregate method either was introduced or remains the same.
 *  - Column sort order either changes to std::nullopt or remains the same.
 */
void ValidateColumnSchemaUpdate(const TColumnSchema& oldColumn, const TColumnSchema& newColumn)
{
    YT_VERIFY(oldColumn.Name() == newColumn.Name());
    try {
        ValidateAlterType(oldColumn.LogicalType(), newColumn.LogicalType());
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION("Type mismatch for column %Qv",
            oldColumn.Name())
            << ex;
    }

    if (newColumn.SortOrder().operator bool() && newColumn.SortOrder() != oldColumn.SortOrder()) {
        THROW_ERROR_EXCEPTION("Sort order mismatch for column %Qv: old %Qlv, new %Qlv",
            oldColumn.Name(),
            oldColumn.SortOrder(),
            newColumn.SortOrder());
    }

    if (newColumn.Expression() != oldColumn.Expression()) {
        THROW_ERROR_EXCEPTION("Expression mismatch for column %Qv: old %Qv, new %Qv",
            oldColumn.Name(),
            oldColumn.Expression(),
            newColumn.Expression());
    }

    if (oldColumn.Aggregate() && oldColumn.Aggregate() != newColumn.Aggregate()) {
        THROW_ERROR_EXCEPTION("Aggregate mode mismatch for column %Qv: old %Qv, new %Qv",
            oldColumn.Name(),
            oldColumn.Aggregate(),
            newColumn.Aggregate());
    }

    if (oldColumn.SortOrder() && oldColumn.Lock() != newColumn.Lock()) {
        THROW_ERROR_EXCEPTION("Lock mismatch for key column %Qv: old %Qv, new %Qv",
            oldColumn.Name(),
            oldColumn.Lock(),
            newColumn.Lock());
    }
}

////////////////////////////////////////////////////////////////////////////////

//! Validates that all columns from the old schema are present in the new schema.
void ValidateColumnsNotRemoved(const TTableSchema& oldSchema, const TTableSchema& newSchema)
{
    YT_VERIFY(newSchema.GetStrict());
    for (int oldColumnIndex = 0; oldColumnIndex < oldSchema.Columns().size(); ++oldColumnIndex) {
        const auto& oldColumn = oldSchema.Columns()[oldColumnIndex];
        if (!newSchema.FindColumn(oldColumn.Name())) {
            THROW_ERROR_EXCEPTION("Cannot remove column %Qv from a strict schema",
                oldColumn.Name());
        }
    }
}

//! Validates that all columns from the new schema are present in the old schema.
void ValidateColumnsNotInserted(const TTableSchema& oldSchema, const TTableSchema& newSchema)
{
    YT_VERIFY(!oldSchema.GetStrict());
    for (int newColumnIndex = 0; newColumnIndex < newSchema.Columns().size(); ++newColumnIndex) {
        const auto& newColumn = newSchema.Columns()[newColumnIndex];
        if (!oldSchema.FindColumn(newColumn.Name())) {
            THROW_ERROR_EXCEPTION("Cannot insert a new column %Qv into non-strict schema",
                newColumn.Name());
        }
    }
}

//! Validates that for each column present in both #oldSchema and #newSchema, its declarations match each other.
//! Also validates that key columns positions are not changed.
void ValidateColumnsMatch(const TTableSchema& oldSchema, const TTableSchema& newSchema)
{
    int commonKeyColumnPrefix = 0;
    for (int oldColumnIndex = 0; oldColumnIndex < oldSchema.Columns().size(); ++oldColumnIndex) {
        const auto& oldColumn = oldSchema.Columns()[oldColumnIndex];
        const auto* newColumnPtr = newSchema.FindColumn(oldColumn.Name());
        if (!newColumnPtr) {
            // We consider only columns present both in oldSchema and newSchema.
            continue;
        }
        const auto& newColumn = *newColumnPtr;
        ValidateColumnSchemaUpdate(oldColumn, newColumn);
        int newColumnIndex = newSchema.GetColumnIndex(newColumn);

        if (oldColumn.SortOrder() && newColumn.SortOrder()) {
            if (oldColumnIndex != newColumnIndex) {
                THROW_ERROR_EXCEPTION("Cannot change position of a key column %Qv: old %v, new %v",
                    oldColumn.Name(),
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
        if (!newSchema.FindColumn(oldColumn.Name())) {
            THROW_ERROR_EXCEPTION("Key column %Qv is missing in new schema", oldColumn.Name());
        }
    }

    if (commonKeyColumnPrefix < oldSchema.GetKeyColumnCount() && newSchema.GetUniqueKeys()) {
        THROW_ERROR_EXCEPTION("Cannot have unique_keys = true after removing some of the key columns");
    }
}

void ValidateNoRequiredColumnsAdded(const TTableSchema& oldSchema, const TTableSchema& newSchema)
{
    for (int newColumnIndex = 0; newColumnIndex < newSchema.Columns().size(); ++newColumnIndex) {
        const auto& newColumn = newSchema.Columns()[newColumnIndex];
        if (newColumn.Required()) {
            const auto* oldColumn = oldSchema.FindColumn(newColumn.Name());
            if (!oldColumn) {
                THROW_ERROR_EXCEPTION("Cannot insert a new required column %Qv into a non-empty table",
                    newColumn.Name());
            }
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
    for (int index = 0; index < schema.Columns().size(); ++index) {
        const auto& columnSchema = schema.Columns()[index];
        if (columnSchema.Aggregate()) {
            if (index < schema.GetKeyColumnCount()) {
                THROW_ERROR_EXCEPTION("Key column %Qv cannot be aggregated", columnSchema.Name());
            }
            if (!columnSchema.SimplifiedLogicalType() || !IsPhysicalType(*columnSchema.SimplifiedLogicalType())) {
                THROW_ERROR_EXCEPTION("Aggregated column %Qv is forbiden to have logical type %Qlv",
                    columnSchema.Name(),
                    *columnSchema.LogicalType());
            }

            const auto& name = *columnSchema.Aggregate();
            if (auto descriptor = BuiltinTypeInferrersMap->GetFunction(name)->As<TAggregateTypeInferrer>()) {
                TTypeSet constraint;
                std::optional<EValueType> stateType;
                std::optional<EValueType> resultType;

                descriptor->GetNormalizedConstraints(&constraint, &stateType, &resultType, name);
                if (!constraint.Get(columnSchema.GetPhysicalType())) {
                    THROW_ERROR_EXCEPTION("Argument type mismatch in aggregate function %Qv from column %Qv: expected %Qlv, got %Qlv",
                        *columnSchema.Aggregate(),
                        columnSchema.Name(),
                        constraint,
                        columnSchema.GetPhysicalType());
                }

                if (stateType && *stateType != columnSchema.GetPhysicalType()) {
                    THROW_ERROR_EXCEPTION("Aggregate function %Qv state type %Qlv differs from column %Qv type %Qlv",
                        *columnSchema.Aggregate(),
                        stateType,
                        columnSchema.Name(),
                        columnSchema.GetPhysicalType());
                }

                if (resultType && *resultType != columnSchema.GetPhysicalType()) {
                    THROW_ERROR_EXCEPTION("Aggregate function %Qv result type %Qlv differs from column %Qv type %Qlv",
                        *columnSchema.Aggregate(),
                        resultType,
                        columnSchema.Name(),
                        columnSchema.GetPhysicalType());
                }
            } else {
                THROW_ERROR_EXCEPTION("Unknown aggregate function %Qv at column %Qv",
                    *columnSchema.Aggregate(),
                    columnSchema.Name());
            }
        }
    }
}

//! Validates computed columns.
/*!
 *  Validates that:
 *  - Computed column has to be key column.
 *  - Type of a computed column matches the type of its expression.
 *  - All referenced columns appear in schema, are key columns and are not computed.
 */
void ValidateComputedColumns(const TTableSchema& schema, bool isTableDynamic)
{
    // TODO(max42): Passing *this before the object is finally constructed
    // doesn't look like a good idea (although it works :) ). Get rid of this.

    for (int index = 0; index < schema.Columns().size(); ++index) {
        const auto& columnSchema = schema.Columns()[index];
        if (columnSchema.Expression()) {
            if (index >= schema.GetKeyColumnCount() && isTableDynamic) {
                THROW_ERROR_EXCEPTION("Non-key column %Qv cannot be computed", columnSchema.Name());
            }
            THashSet<TString> references;
            auto expr = PrepareExpression(*columnSchema.Expression(), schema, BuiltinTypeInferrersMap, &references);
            if (GetLogicalType(expr->Type) != columnSchema.SimplifiedLogicalType()) {
                THROW_ERROR_EXCEPTION(
                    "Computed column %Qv type mismatch: declared type is %Qlv but expression type is %Qlv",
                    columnSchema.Name(),
                    *columnSchema.LogicalType(),
                    expr->Type);
            }

            for (const auto& ref : references) {
                const auto& refColumn = schema.GetColumnOrThrow(ref);
                if (!refColumn.SortOrder() && isTableDynamic) {
                    THROW_ERROR_EXCEPTION("Computed column %Qv depends on a non-key column %Qv",
                        columnSchema.Name(),
                        ref);
                }
                if (refColumn.Expression()) {
                    THROW_ERROR_EXCEPTION("Computed column %Qv depends on a computed column %Qv",
                        columnSchema.Name(),
                        ref);
                }
            }
        }
    }
}

//! TODO(max42): document this functions somewhere (see also https://st.yandex-team.ru/YT-1433).
void ValidateTableSchemaUpdate(
    const TTableSchema& oldSchema,
    const TTableSchema& newSchema,
    bool isTableDynamic,
    bool isTableEmpty)
{
    ValidateTableSchemaHeavy(newSchema, isTableDynamic);

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
        ValidateColumnsNotRemoved(oldSchema, newSchema);
    } else {
        ValidateColumnsNotInserted(oldSchema, newSchema);
    }
    ValidateColumnsMatch(oldSchema, newSchema);

    // We allow adding computed columns only on creation of the table.
    if (!oldSchema.Columns().empty() || !isTableEmpty) {
        for (const auto& newColumn : newSchema.Columns()) {
            if (!oldSchema.FindColumn(newColumn.Name())) {
                if (newColumn.Expression()) {
                    THROW_ERROR_EXCEPTION("Cannot introduce a new computed column %Qv after creation",
                        newColumn.Name());
                }
            }
        }
    }

    ValidateNoRequiredColumnsAdded(oldSchema, newSchema);
}

////////////////////////////////////////////////////////////////////////////////

void ValidatePivotKey(const TOwningKey& pivotKey, const TTableSchema& schema)
{
    if (pivotKey.GetCount() > schema.GetKeyColumnCount()) {
        THROW_ERROR_EXCEPTION("Pivot key must form a prefix of key");
    }

    for (int index = 0; index < pivotKey.GetCount(); ++index) {
        if (pivotKey[index].Type != EValueType::Null && pivotKey[index].Type != schema.Columns()[index].GetPhysicalType()) {
            THROW_ERROR_EXCEPTION(
                "Mismatched type of column %Qv in pivot key: expected %Qlv, found %Qlv",
                schema.Columns()[index].Name(),
                schema.Columns()[index].GetPhysicalType(),
                pivotKey[index].Type);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TTableSchema InferInputSchema(const std::vector<TTableSchema>& schemas, bool discardKeyColumns)
{
    YT_VERIFY(!schemas.empty());

    // NB: If one schema is not strict then the resulting schema should be an intersection, not union.
    for (const auto& schema : schemas) {
        if (!schema.GetStrict()) {
            THROW_ERROR_EXCEPTION("Input table schema is not strict");
        }
    }

    int commonKeyColumnPrefix = 0;
    if (!discardKeyColumns) {
        while (true) {
            if (commonKeyColumnPrefix >= schemas.front().GetKeyColumnCount()) {
                break;
            }
            const auto& keyColumnName = schemas.front().Columns()[commonKeyColumnPrefix].Name();
            bool mismatch = false;
            for (const auto& schema : schemas) {
                if (commonKeyColumnPrefix >= schema.GetKeyColumnCount() ||
                    schema.Columns()[commonKeyColumnPrefix].Name() != keyColumnName)
                {
                    mismatch = true;
                    break;
                }
            }
            if (mismatch) {
                break;
            }
            ++commonKeyColumnPrefix;
        }
    }

    THashMap<TString, TColumnSchema> nameToColumnSchema;
    std::vector<TString> columnNames;

    for (const auto& schema : schemas) {
        for (int columnIndex = 0; columnIndex < schema.Columns().size(); ++columnIndex) {
            auto column = schema.Columns()[columnIndex];
            if (columnIndex >= commonKeyColumnPrefix) {
                column = column.SetSortOrder(std::nullopt);
            }
            column = column
                .SetExpression(std::nullopt)
                .SetAggregate(std::nullopt)
                .SetLock(std::nullopt);

            auto it = nameToColumnSchema.find(column.Name());
            if (it == nameToColumnSchema.end()) {
                nameToColumnSchema[column.Name()] = column;
                columnNames.push_back(column.Name());
            } else {
                if (it->second != column) {
                    THROW_ERROR_EXCEPTION(
                        "Conflict while merging schemas, column %Qs has two conflicting declarations",
                        column.Name())
                        << TErrorAttribute("first_column_schema", it->second)
                        << TErrorAttribute("second_column_schema", column);
                }
            }
        }
    }

    std::vector<TColumnSchema> columns;
    for (auto columnName : columnNames) {
        columns.push_back(nameToColumnSchema[columnName]);
    }

    return TTableSchema(std::move(columns), true);
}

TError ValidateTableSchemaCompatibility(
    const TTableSchema& inputSchema,
    const TTableSchema& outputSchema,
    bool ignoreSortOrder,
    bool allowSimpleTypeDeoptionalize)
{
    auto addAttributes = [&] (TError error) {
        return error
            << TErrorAttribute("input_table_schema", inputSchema)
            << TErrorAttribute("output_table_schema", outputSchema);
    };

    auto createSchemaIndex = [] (const TTableSchema& schema) {
        THashMap<TString, const TColumnSchema*> result;
        for (const auto& column : schema.Columns()) {
            result[column.Name()] = &column;
        }
        return result;
    };
    auto inputSchemaIndex = createSchemaIndex(inputSchema);
    auto outputSchemaIndex = createSchemaIndex(outputSchema);

    // If output schema is strict, check that input columns are subset of output columns.
    if (outputSchema.GetStrict()) {
        if (!inputSchema.GetStrict()) {
            return addAttributes(TError("Incompatible strictness: input schema is not strict while output schema is"));
        }

        for (const auto& inputColumn : inputSchema.Columns()) {
            if (!outputSchemaIndex.contains(inputColumn.Name())) {
                return addAttributes(TError("Column %Qv is found in input schema but is missing in output schema",
                    inputColumn.Name()));
            }
        }
    }

    // Check that columns are the same.
    for (const auto& outputColumn : outputSchema.Columns()) {
        auto it = inputSchemaIndex.find(outputColumn.Name());
        if (it != inputSchemaIndex.end()) {
            auto inputColumn = it->second;
            bool typeIsOk = IsSubtypeOf(inputColumn->LogicalType(), outputColumn.LogicalType());
            if (allowSimpleTypeDeoptionalize &&
                !typeIsOk &&
                inputColumn->SimplifiedLogicalType() &&
                inputColumn->LogicalType()->GetMetatype() == ELogicalMetatype::Optional)
            {
                // For historical reasons some users of this function consider type optional<T> to be compatible with T.
                // They perform runtime check of values.
                typeIsOk = IsSubtypeOf(inputColumn->LogicalType()->AsOptionalTypeRef().GetElement(), outputColumn.LogicalType());
            }
            if (!typeIsOk) {
                return addAttributes(TError("Column %Qv input type %Qlv is incompatible with the output type %Qlv",
                    inputColumn->Name(),
                    *inputColumn->LogicalType(),
                    *outputColumn.LogicalType()));
            }
            if (outputColumn.Expression() && inputColumn->Expression() != outputColumn.Expression()) {
                return addAttributes(TError("Column %Qv expression mismatch",
                    inputColumn->Name()));
            }
            if (outputColumn.Aggregate() && inputColumn->Aggregate() != outputColumn.Aggregate()) {
                return addAttributes(TError("Column %Qv aggregate mismatch",
                    inputColumn->Name()));
            }
        } else if (outputColumn.Expression()) {
            return addAttributes(TError("Unexpected computed column %Qv in output schema",
                outputColumn.Name()));
        } else if (!inputSchema.GetStrict()) {
            return addAttributes(TError("Column %Qv is present in output schema and is missing in nonstrict input schema",
                    outputColumn.Name()));
        } else if (outputColumn.Required()) {
            return addAttributes(TError("Required column %Qv is present in output schema and is missing in input schema",
                    outputColumn.Name()));
        }
    }

    // Check that we don't lose complex types.
    // We never want to teleport complex types to schemaless part of the chunk because we want to change their type from
    // EValueType::Composite to EValueType::Any.
    if (!outputSchema.GetStrict()) {
        for (const auto& inputColumn : inputSchema.Columns()) {
            if (inputColumn.SimplifiedLogicalType()) {
                continue;
            }
            if (!outputSchemaIndex.contains(inputColumn.Name())) {
                return TError("Column %Qv of input schema with complex type %Qv is missing in strict part of output schema",
                    inputColumn.Name(),
                    *inputColumn.LogicalType());
            }
        }
    }

    if (ignoreSortOrder) {
        return TError();
    }

    // Check that output key columns form a proper prefix of input key columns.
    int cmp = outputSchema.GetKeyColumnCount() - inputSchema.GetKeyColumnCount();
    if (cmp > 0) {
        return addAttributes(TError("Output key columns are wider than input key columns"));
    }

    if (outputSchema.GetUniqueKeys()) {
        if (!inputSchema.GetUniqueKeys()) {
            return addAttributes(TError("Input schema \"unique_keys\" attribute is false"));
        }
        if (cmp != 0) {
            return addAttributes(TError("Input key columns are wider than output key columns"));
        }
    }

    auto inputKeyColumns = inputSchema.GetKeyColumns();
    auto outputKeyColumns = outputSchema.GetKeyColumns();

    for (int index = 0; index < outputKeyColumns.size(); ++index) {
        if (inputKeyColumns[index] != outputKeyColumns[index]) {
            return addAttributes(TError("Input sorting order is incompatible with the output"));
        }
    }

    return TError();
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

} // namespace NYT::NTableClient
