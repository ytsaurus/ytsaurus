#include "schema.h"

// TODO(sandello,lukyan): Refine these dependencies.
#include <yt/yt/ytlib/query_client/query_preparer.h>
#include <yt/yt/ytlib/query_client/functions.h>

#include <yt/yt/client/complex_types/check_type_compatibility.h>
#include <yt/yt/client/table_client/logical_type.h>

#include <yt/yt/core/ytree/convert.h>

namespace NYT::NTableClient {

using namespace NYTree;
using namespace NQueryClient;
using namespace NChunkClient;
using namespace NTabletClient;

////////////////////////////////////////////////////////////////////////////////
//! Validates the column schema update.
/*!
 *  \pre{oldColumn and newColumn should have the same stable name.}
 *
 *  Validates that:
 *  - New column type is compatible with the old one.
 *  - Optional column doesn't become required.
 *  - Column expression remains the same.
 *  - Column aggregate method either was introduced or remains the same.
 *  - Column sort order either changes to std::nullopt or remains the same.
 */
void ValidateColumnSchemaUpdate(const TColumnSchema& oldColumn, const TColumnSchema& newColumn)
{
    YT_VERIFY(oldColumn.StableName() == newColumn.StableName());

    auto compatibility = NComplexTypes::CheckTypeCompatibility(
        oldColumn.LogicalType(),
        newColumn.LogicalType());
    try {
        if (oldColumn.GetWireType() != newColumn.GetWireType()) {
            THROW_ERROR_EXCEPTION("Cannot change physical type from %Qlv to %Qlv",
                oldColumn.GetWireType(),
                newColumn.GetWireType());
        }
        if (compatibility.first != ESchemaCompatibility::FullyCompatible) {
            THROW_ERROR compatibility.second;
        }
    } catch (const std::exception& ex) {
        THROW_ERROR_EXCEPTION(NTableClient::EErrorCode::IncompatibleSchemas, "Type mismatch for column %v",
            oldColumn.GetDiagnosticNameString())
            << ex;
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

//! Validates that all columns from the old schema are present in the new schema.
void ValidateColumnsNotRemoved(const TTableSchema& oldSchema, const TTableSchema& newSchema)
{
    YT_VERIFY(newSchema.GetStrict());
    for (const auto& oldColumn : oldSchema.Columns()) {
        if (!newSchema.FindColumnByStableName(oldColumn.StableName())) {
            THROW_ERROR_EXCEPTION("Cannot remove column %v from a strict schema",
                oldColumn.GetDiagnosticNameString());
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
            if (!columnSchema.IsOfV1Type() || !IsPhysicalType(columnSchema.CastToV1Type())) {
                THROW_ERROR_EXCEPTION("Aggregated column %v is forbidden to have logical type %Qlv",
                    columnSchema.GetDiagnosticNameString(),
                    *columnSchema.LogicalType());
            }

            const auto& name = *columnSchema.Aggregate();
            if (auto descriptor = BuiltinTypeInferrersMap->GetFunction(name)->As<TAggregateTypeInferrer>()) {
                TTypeSet constraint;
                std::optional<EValueType> stateType;
                std::optional<EValueType> resultType;

                descriptor->GetNormalizedConstraints(&constraint, &stateType, &resultType, name);
                if (!constraint.Get(columnSchema.GetWireType())) {
                    THROW_ERROR_EXCEPTION("Argument type mismatch in aggregate function %Qv from column %v: expected %Qlv, got %Qlv",
                        *columnSchema.Aggregate(),
                        columnSchema.GetDiagnosticNameString(),
                        constraint,
                        columnSchema.GetWireType());
                }

                if (stateType && *stateType != columnSchema.GetWireType()) {
                    THROW_ERROR_EXCEPTION("Aggregate function %Qv state type %Qlv differs from column %v type %Qlv",
                        *columnSchema.Aggregate(),
                        stateType,
                        columnSchema.GetDiagnosticNameString(),
                        columnSchema.GetWireType());
                }

                if (resultType && *resultType != columnSchema.GetWireType()) {
                    THROW_ERROR_EXCEPTION("Aggregate function %Qv result type %Qlv differs from column %v type %Qlv",
                        *columnSchema.Aggregate(),
                        resultType,
                        columnSchema.GetDiagnosticNameString(),
                        columnSchema.GetWireType());
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
        // TODO(levysotsky): Use early continue.
        if (columnSchema.Expression()) {
            if (index >= schema.GetKeyColumnCount() && isTableDynamic) {
                THROW_ERROR_EXCEPTION("Non-key column %v cannot be computed", columnSchema.GetDiagnosticNameString());
            }
            THashSet<TString> references;
            auto expr = PrepareExpression(*columnSchema.Expression(), schema, BuiltinTypeInferrersMap, &references);
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
}

//! TODO(max42): document this functions somewhere (see also https://st.yandex-team.ru/YT-1433).
void ValidateTableSchemaUpdate(
    const TTableSchema& oldSchema,
    const TTableSchema& newSchema,
    bool isTableDynamic,
    bool isTableEmpty)
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

void ValidatePivotKey(const TUnversionedRow& pivotKey, const TTableSchema& schema, const TStringBuf& keyType, bool validateRequired)
{
    if (static_cast<int>(pivotKey.GetCount()) > schema.GetKeyColumnCount()) {
        auto titleKeyType = TString(keyType);
        titleKeyType.to_title();
        THROW_ERROR_EXCEPTION(NTableClient::EErrorCode::SchemaViolation, "%v key must form a prefix of key", titleKeyType);
    }

    for (int index = 0; index < static_cast<int>(pivotKey.GetCount()); ++index) {
        if (pivotKey[index].Type != EValueType::Null && pivotKey[index].Type != schema.Columns()[index].GetWireType()) {
            THROW_ERROR_EXCEPTION(
                NTableClient::EErrorCode::SchemaViolation,
                "Mismatched type of column %v in %v key: expected %Qlv, found %Qlv",
                schema.Columns()[index].GetDiagnosticNameString(),
                keyType,
                schema.Columns()[index].GetWireType(),
                pivotKey[index].Type);
        }
        if (validateRequired && pivotKey[index].Type == EValueType::Null && !schema.Columns()[index].LogicalType()->IsNullable()) {
            THROW_ERROR_EXCEPTION(
                NTableClient::EErrorCode::SchemaViolation,
                "Unexpected null for required column %v in %v key",
                schema.Columns()[index].GetDiagnosticNameString(),
                keyType);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TTableSchemaPtr InferInputSchema(const std::vector<TTableSchemaPtr>& schemas, bool discardKeyColumns)
{
    YT_VERIFY(!schemas.empty());

    // NB: If one schema is not strict then the resulting schema should be an intersection, not union.
    for (const auto& schema : schemas) {
        if (!schema->GetStrict()) {
            THROW_ERROR_EXCEPTION("Input table schema is not strict");
        }
    }

    int commonKeyColumnPrefix = 0;
    if (!discardKeyColumns) {
        std::vector<TSortColumns> allSortColumns;
        for (const auto& schema : schemas) {
            allSortColumns.push_back(schema->GetSortColumns());
        }
        for (; commonKeyColumnPrefix < ssize(allSortColumns.front()); ++commonKeyColumnPrefix) {
            const auto& firstSortColumn = allSortColumns.front()[commonKeyColumnPrefix];
            auto match = std::all_of(begin(allSortColumns), end(allSortColumns), [&] (const TSortColumns& sortColumns) {
                return commonKeyColumnPrefix < ssize(sortColumns) && firstSortColumn == sortColumns[commonKeyColumnPrefix];
            });
            if (!match) {
                break;
            }
        }
    }

    std::vector<TColumnSchema> columns;
    THashMap<TStableName, int> stableNameToColumnIndex;
    THashMap<TString, int> nameToColumnIndex;
    for (const auto& schema : schemas) {
        for (int columnIndex = 0; columnIndex < schema->GetColumnCount(); ++columnIndex) {
            auto column = schema->Columns()[columnIndex];
            if (columnIndex >= commonKeyColumnPrefix) {
                column.SetSortOrder(std::nullopt);
            }
            column
                .SetExpression(std::nullopt)
                .SetAggregate(std::nullopt)
                .SetLock(std::nullopt);

            auto it = nameToColumnIndex.find(column.Name());
            if (it == nameToColumnIndex.end()) {
                columns.push_back(column);
                auto index = ssize(columns) - 1;
                EmplaceOrCrash(nameToColumnIndex, column.Name(), index);
                if (auto [it, inserted] = stableNameToColumnIndex.emplace(column.StableName(), index); !inserted) {
                    THROW_ERROR_EXCEPTION(
                        "Conflict while merging schemas: duplicate stable name %Qv for columns with differing names",
                        column.StableName().Get())
                        << TErrorAttribute("first_column_schema", columns[it->second])
                        << TErrorAttribute("second_column_schema", columns[index]);
                }
            } else {
                if (columns[it->second] != column) {
                    THROW_ERROR_EXCEPTION(
                        "Conflict while merging schemas: column %v has two conflicting declarations",
                        column.GetDiagnosticNameString())
                        << TErrorAttribute("first_column_schema", columns[it->second])
                        << TErrorAttribute("second_column_schema", column);
                }
            }
        }
    }

    return New<TTableSchema>(std::move(columns));
}

std::pair<ESchemaCompatibility, TError> CheckTableSchemaCompatibilityImpl(
    const TTableSchema& inputSchema,
    const TTableSchema& outputSchema,
    bool ignoreSortOrder)
{
    // If output schema is strict, check that input columns are subset of output columns.
    if (outputSchema.GetStrict()) {
        if (!inputSchema.GetStrict()) {
            return {
                ESchemaCompatibility::Incompatible,
                TError("Incompatible strictness: input schema is not strict while output schema is"),
            };
        }

        for (const auto& inputColumn : inputSchema.Columns()) {
            if (!outputSchema.FindColumn(inputColumn.Name())) {
                return {
                    ESchemaCompatibility::Incompatible,
                    TError("Column %v is found in input schema but is missing in output schema",
                        inputColumn.GetDiagnosticNameString()),
                };
            }
        }
    }

    auto result = std::pair(ESchemaCompatibility::FullyCompatible, TError());

    // Check that columns are the same.
    for (const auto& outputColumn : outputSchema.Columns()) {
        const auto* inputColumn = inputSchema.FindColumn(outputColumn.Name());
        if (inputColumn) {
            if (inputColumn->StableName() != outputColumn.StableName()) {
                return {
                    ESchemaCompatibility::Incompatible,
                    TError("Column %Qv has stable name %Qv in input and %Qv in output schema",
                        inputColumn->Name(),
                        inputColumn->StableName().Get(),
                        outputColumn.StableName().Get())
                };
            }

            auto currentTypeCompatibility = NComplexTypes::CheckTypeCompatibility(
                inputColumn->LogicalType(), outputColumn.LogicalType());

            if (currentTypeCompatibility.first < result.first) {
                result = {
                    currentTypeCompatibility.first,
                    TError("Column %v input type is incompatible with output type",
                        inputColumn->GetDiagnosticNameString())
                        << currentTypeCompatibility.second
                };
            }

            if (result.first == ESchemaCompatibility::Incompatible) {
                break;
            }

            if (outputColumn.Expression() && inputColumn->Expression() != outputColumn.Expression()) {
                return {
                    ESchemaCompatibility::Incompatible,
                    TError("Column %v expression mismatch",
                        inputColumn->GetDiagnosticNameString()),
                };
            }
            if (outputColumn.Aggregate() && inputColumn->Aggregate() != outputColumn.Aggregate()) {
                return {
                    ESchemaCompatibility::Incompatible,
                    TError("Column %v aggregate mismatch",
                        inputColumn->GetDiagnosticNameString()),
                };
            }
        } else if (outputColumn.Expression()) {
            return {
                ESchemaCompatibility::Incompatible,
                TError("Unexpected computed column %v in output schema",
                    outputColumn.GetDiagnosticNameString()),
            };
        } else if (!inputSchema.GetStrict()) {
            return {
                ESchemaCompatibility::Incompatible,
                TError("Column %v is present in output schema and is missing in non-strict input schema",
                    outputColumn.GetDiagnosticNameString()),
            };
        } else if (outputColumn.Required()) {
            return {
                ESchemaCompatibility::Incompatible,
                TError("Required column %v is present in output schema and is missing in input schema",
                    outputColumn.GetDiagnosticNameString()),
            };
        }
    }

    // Check that we don't lose complex types.
    // We never want to teleport complex types to schemaless part of the chunk because we want to change their type from
    // EValueType::Composite to EValueType::Any.
    if (!outputSchema.GetStrict()) {
        for (const auto& inputColumn : inputSchema.Columns()) {
            if (!IsV3Composite(inputColumn.LogicalType())) {
                continue;
            }
            if (!outputSchema.FindColumn(inputColumn.Name())) {
                return {
                    ESchemaCompatibility::Incompatible,
                    TError("Column %v of input schema with complex type %Qv is missing in strict part of output schema",
                        inputColumn.GetDiagnosticNameString(),
                        *inputColumn.LogicalType()),
                };
            }
        }
    }

    if (ignoreSortOrder) {
        return result;
    }

    // Check that output key columns form a prefix of input key columns.
    int cmp = outputSchema.GetKeyColumnCount() - inputSchema.GetKeyColumnCount();
    if (cmp > 0) {
        return {
            ESchemaCompatibility::Incompatible,
            TError("Output key columns are wider than input key columns"),
        };
    }

    if (outputSchema.GetUniqueKeys()) {
        if (!inputSchema.GetUniqueKeys()) {
            return {
                ESchemaCompatibility::Incompatible,
                TError("Input schema \"unique_keys\" attribute is false"),
            };
        }
        if (cmp != 0) {
            return {
                ESchemaCompatibility::Incompatible,
                TError("Input key columns are wider than output key columns"),
            };
        }
    }

    auto inputKeySchema = inputSchema.ToKeys();
    auto outputKeySchema = outputSchema.ToKeys();

    for (int index = 0; index < outputKeySchema->GetColumnCount(); ++index) {
        const auto& inputColumn = inputKeySchema->Columns()[index];
        const auto& outputColumn = outputKeySchema->Columns()[index];
        if (inputColumn.StableName() != outputColumn.StableName()) {
            return {
                ESchemaCompatibility::Incompatible,
                TError("Key columns do not match: input column %v, output column %v",
                    inputColumn.GetDiagnosticNameString(),
                    outputColumn.GetDiagnosticNameString())
            };
        }
        if (inputColumn.SortOrder() != outputColumn.SortOrder()) {
            return {
                ESchemaCompatibility::Incompatible,
                TError("Sort order of column %v does not match: input sort order %Qlv, output sort order %Qlv",
                    inputColumn.GetDiagnosticNameString(),
                    inputColumn.SortOrder(),
                    outputColumn.SortOrder())
            };
        }
    }

    return result;
}

std::pair<ESchemaCompatibility, TError> CheckTableSchemaCompatibility(
    const TTableSchema& inputSchema,
    const TTableSchema& outputSchema,
    bool ignoreSortOrder)
{
    auto result = CheckTableSchemaCompatibilityImpl(inputSchema, outputSchema, ignoreSortOrder);
    if (result.first != ESchemaCompatibility::FullyCompatible) {
        result.second = TError(NTableClient::EErrorCode::IncompatibleSchemas, "Table schemas are incompatible")
            << result.second
            << TErrorAttribute("input_table_schema", inputSchema)
            << TErrorAttribute("output_table_schema", outputSchema);
    }
    return result;
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
                THROW_ERROR_EXCEPTION("Computed column %v is missing in input schema",
                    outputColumn.GetDiagnosticNameString());
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
