#include "schema.h"

// XXX(max42): this is a workaround for some weird linkage error.
#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/library/query/base/public.h>

#include <yt/yt/client/table_client/column_sort_schema.h>
#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/schema.h>

namespace NYT::NTableClient {

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
    THashMap<TColumnStableName, int> stableNameToColumnIndex;
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
                        column.StableName())
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

////////////////////////////////////////////////////////////////////////////////

void ValidateIndexSchema(
    const TTableSchema& tableSchema,
    const TTableSchema& indexTableSchema,
    std::function<void(const TColumnSchema& indexColumn, const TColumnSchema& tableColumn)> typeMismatchCallback)
{
    THROW_ERROR_EXCEPTION_IF(!tableSchema.IsSorted(),
        "Table must be sorted");
    THROW_ERROR_EXCEPTION_IF(!indexTableSchema.IsSorted(),
        "Index table must be sorted");

    THROW_ERROR_EXCEPTION_IF(!tableSchema.IsUniqueKeys(),
        "Table must have unique keys");
    THROW_ERROR_EXCEPTION_IF(!indexTableSchema.IsUniqueKeys(),
        "Index table must have unique keys");

    for (const auto& tableColumn : tableSchema.Columns()) {
        if (!tableColumn.SortOrder()) {
            break;
        }

        if (tableColumn.Expression()) {
            continue;
        }

        auto* indexColumn = indexTableSchema.FindColumn(tableColumn.Name());
        if (!indexColumn) {
            THROW_ERROR_EXCEPTION("Key column %Qv missing in the index",
                tableColumn.Name());
        }

        if (!indexColumn->SortOrder()) {
            THROW_ERROR_EXCEPTION("Table key column %Qv must be a key column in the index",
                tableColumn.Name());
        }
    }

    const TColumnSchema* firstTableValueColumnInIndex = nullptr;

    for (const auto& indexColumn : indexTableSchema.Columns()) {
        THROW_ERROR_EXCEPTION_IF(indexColumn.Aggregate(),
            "Index table cannot have aggregate columns, found aggregate column %Qv with function %Qv",
            indexColumn.Name(),
            indexColumn.Aggregate());

        if (auto* tableColumn = tableSchema.FindColumn(indexColumn.Name())) {
            const auto& tableType = tableColumn->LogicalType();
            const auto& indexType = indexColumn.LogicalType();

            if (*tableType != *indexType) {
                typeMismatchCallback(indexColumn, *tableColumn);
            }

            THROW_ERROR_EXCEPTION_IF(tableColumn->Aggregate(),
                "Cannot create index on an aggregate column %Qv",
                indexColumn.Name());

            if (!tableColumn->SortOrder()) {
                if (!firstTableValueColumnInIndex) {
                    firstTableValueColumnInIndex = tableColumn;
                } else {
                    THROW_ERROR_EXCEPTION_IF(firstTableValueColumnInIndex->Lock() != tableColumn->Lock(),
                        "All indexed table columns must have same lock group, found %Qv and %Qv",
                        firstTableValueColumnInIndex->Lock(),
                        tableColumn->Lock());
                }
            }
        } else {
            if (!indexColumn.SortOrder()) {
                THROW_ERROR_EXCEPTION_IF(indexColumn.Name() != EmptyValueColumnName,
                    "Non-key non-utility column %Qv of the index is missing in the table schema",
                    indexColumn.Name());
            }
        }
    }
}

void ValidateFullSyncIndexSchema(const TTableSchema& tableSchema, const TTableSchema& indexTableSchema)
{
    auto typeMismatchCallback = [] (const TColumnSchema& indexColumn, const TColumnSchema& tableColumn) {
        THROW_ERROR_EXCEPTION("Type mismatch for column %Qv: %v in the table, %v in the index table",
            indexColumn.Name(),
            *tableColumn.LogicalType(),
            *indexColumn.LogicalType());
    };

    ValidateIndexSchema(tableSchema, indexTableSchema, typeMismatchCallback);
}

bool IsValidUnfoldedColumnPair(const TLogicalTypePtr& tableColumnType, const TLogicalTypePtr& indexColumnType)
{
    auto tableColumnElementType = tableColumnType;
    if (tableColumnElementType->GetMetatype() == ELogicalMetatype::Optional) {
        tableColumnElementType = tableColumnElementType->UncheckedAsOptionalTypeRef().GetElement();
    }

    if (tableColumnElementType->GetMetatype() != ELogicalMetatype::List) {
        return false;
    }

    tableColumnElementType = tableColumnType->UncheckedAsListTypeRef().GetElement();

    return *tableColumnElementType == *indexColumnType;
}

const TColumnSchema& FindUnfoldingColumnAndValidate(const TTableSchema& tableSchema, const TTableSchema& indexTableSchema)
{
    const TColumnSchema* unfoldedColumn = nullptr;

    auto typeMismatchCallback = [&] (const TColumnSchema& indexColumn, const TColumnSchema& tableColumn) {
        THROW_ERROR_EXCEPTION_IF(!IsValidUnfoldedColumnPair(tableColumn.LogicalType(), indexColumn.LogicalType()),
            "Type mismatch for column %Qv: %v in the table, %v in the index table",
            indexColumn.Name(),
            *tableColumn.LogicalType(),
            *indexColumn.LogicalType());

        THROW_ERROR_EXCEPTION_IF(unfoldedColumn,
            "Expected a single unfolded column, found at least two: %v, %v",
            unfoldedColumn->Name(),
            indexColumn.Name());

        unfoldedColumn = &indexColumn;
    };

    ValidateIndexSchema(tableSchema, indexTableSchema, typeMismatchCallback);

    THROW_ERROR_EXCEPTION_IF(!unfoldedColumn,
        "No candidate for unfolded column found in the index table schema");

    return *unfoldedColumn;
}

void ValidateColumnsAreInIndexLockGroup(
    const NQueryClient::TColumnSet& columns,
    const TTableSchema& tableSchema,
    const TTableSchema& indexTableSchema)
{
    const TColumnSchema* firstTableValueColumnInIndex = nullptr;

    for (const auto& indexColumn : indexTableSchema.Columns()) {
        const auto* tableColumn = tableSchema.FindColumn(indexColumn.Name());
        if (!tableColumn || tableColumn->SortOrder()) {
            continue;
        }

        firstTableValueColumnInIndex = tableColumn;
        break;
    }

    THROW_ERROR_EXCEPTION_IF(!firstTableValueColumnInIndex,
        "Expected at least one table value column in index");

    for (const auto& column : columns) {
        const auto& tableColumn = tableSchema.GetColumn(column);

        THROW_ERROR_EXCEPTION_IF(tableColumn.Lock() != firstTableValueColumnInIndex->Lock(),
            "Columns %Qv and %Qv belong to different lock groups: %Qv and %Qv",
            tableColumn.Name(),
            firstTableValueColumnInIndex->Name(),
            tableColumn.Lock(),
            firstTableValueColumnInIndex->Lock());
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
