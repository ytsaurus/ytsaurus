#include "schema.h"

#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/schema.h>

namespace NYT::NQueryClient {

using namespace NTableClient;
using namespace NTabletClient;

////////////////////////////////////////////////////////////////////////////////

using TColumnMismatchCallback = void(const TColumnSchema& indexColumn, const TColumnSchema& tableColumn);
using TBadColumnCallback = void(const TColumnSchema& column);
using TExtraIndexColumnCallback = TBadColumnCallback;

void ThrowExpectedKeyColumn(const TColumnSchema& tableColumn)
{
    THROW_ERROR_EXCEPTION("Table key column %Qv must be a key column in index table",
        tableColumn.Name());
}

void ThrowExpectedTypeMatch(const TColumnSchema& indexColumn, const TColumnSchema& tableColumn)
{
    THROW_ERROR_EXCEPTION("Type mismatch for column %Qv: %v in table, %v in index table",
        indexColumn.Name(),
        *tableColumn.LogicalType(),
        *indexColumn.LogicalType());
};

void ThrowIfNonKey(const TColumnSchema& indexColumn)
{
    THROW_ERROR_EXCEPTION_IF(!indexColumn.SortOrder(),
        "Non-key non-utility column %Qv of the index is missing in the table schema",
        indexColumn.Name());
}

void ThrowIfKey(const TColumnSchema& indexColumn)
{
    THROW_ERROR_EXCEPTION_IF(indexColumn.SortOrder(),
        "Key non-utility column %Qv of the index is missing in the table schema",
        indexColumn.Name());
}

void ValidateIndexSchema(
    const TTableSchema& tableSchema,
    const TTableSchema& indexTableSchema,
    std::function<TColumnMismatchCallback> typeMismatchCallback = ThrowExpectedTypeMatch,
    std::function<TBadColumnCallback> tableKeyIsNotIndexKeyCallback = ThrowExpectedKeyColumn,
    std::function<TExtraIndexColumnCallback> extraIndexColumnCallback = ThrowIfNonKey)
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
            THROW_ERROR_EXCEPTION("Key column %Qv missing in the index schema",
                tableColumn.Name());
        }

        if (!indexColumn->SortOrder()) {
            tableKeyIsNotIndexKeyCallback(tableColumn);
        }
    }

    const TColumnSchema* firstTableValueColumnInIndex = nullptr;

    for (const auto& indexColumn : indexTableSchema.Columns()) {
        THROW_ERROR_EXCEPTION_IF(indexColumn.Aggregate(),
            "Index table cannot have aggregate columns, found aggregate column %Qv with function %Qv",
            indexColumn.Name(),
            indexColumn.Aggregate());

        if (auto* tableColumn = tableSchema.FindColumn(indexColumn.Name())) {
            if (indexColumn.Expression()) {
                if (tableColumn->Expression()) {
                    continue;
                }
                THROW_ERROR_EXCEPTION("Column %Qv is evaluated in index and not evaluated in table",
                    indexColumn.Name());
            } else if (tableColumn->Expression()) {
                THROW_ERROR_EXCEPTION("Column %Qv is evaluated in table and not evaluated in index",
                    indexColumn.Name());
            }

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
            if (indexColumn.Name() == EmptyValueColumnName) {
                THROW_ERROR_EXCEPTION_IF(indexColumn.Required(),
                    "Utility column %Qv must have a nullable type, found %v",
                    EmptyValueColumnName,
                    *indexColumn.LogicalType());

                THROW_ERROR_EXCEPTION_IF(indexColumn.SortOrder(),
                    "Utility column %Qv must not be a key",
                    indexColumn.Name());
            } else {
                extraIndexColumnCallback(indexColumn);
            }
        }
    }
}

void ValidateFullSyncIndexSchema(const TTableSchema& tableSchema, const TTableSchema& indexTableSchema)
{
    ValidateIndexSchema(tableSchema, indexTableSchema);
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

    tableColumnElementType = tableColumnElementType->UncheckedAsListTypeRef().GetElement();

    return *tableColumnElementType == *indexColumnType;
}

const TColumnSchema& FindUnfoldedColumnAndValidate(const TTableSchema& tableSchema, const TTableSchema& indexTableSchema)
{
    const TColumnSchema* unfoldedColumn = nullptr;

    auto typeMismatchCallback = [&] (const TColumnSchema& indexColumn, const TColumnSchema& tableColumn) {
        if (!IsValidUnfoldedColumnPair(tableColumn.LogicalType(), indexColumn.LogicalType())) {
            ThrowExpectedTypeMatch(indexColumn, tableColumn);
        }

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

void ValidateUnfoldingIndexSchema(
    const TTableSchema& tableSchema,
    const TTableSchema& indexTableSchema,
    const TString& unfoldedColumnName)
{
    auto typeMismatchCallback = [&] (const TColumnSchema& indexColumn, const TColumnSchema& tableColumn) {
        if (indexColumn.Name() == unfoldedColumnName) {
            THROW_ERROR_EXCEPTION_IF(!IsValidUnfoldedColumnPair(tableColumn.LogicalType(), indexColumn.LogicalType()),
                "Type mismatch for the unfolded column %Qv: %v is not a list of %v",
                unfoldedColumnName,
                *tableColumn.LogicalType(),
                *indexColumn.LogicalType());
        } else {
            ThrowExpectedTypeMatch(indexColumn, tableColumn);
        }
    };

    ValidateIndexSchema(tableSchema, indexTableSchema, typeMismatchCallback);
}

void ValidateColumnsAreInIndexLockGroup(
    const TColumnSet& columns,
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

    for (const auto& column : columns) {
        const auto& tableColumn = tableSchema.GetColumn(column);
        if (firstTableValueColumnInIndex) {
            THROW_ERROR_EXCEPTION_IF(tableColumn.Lock() != firstTableValueColumnInIndex->Lock(),
                "Columns %Qv and %Qv belong to different lock groups: %Qv and %Qv",
                tableColumn.Name(),
                firstTableValueColumnInIndex->Name(),
                tableColumn.Lock(),
                firstTableValueColumnInIndex->Lock());
        } else {
            firstTableValueColumnInIndex = &tableColumn;
        }
    }
}

void ValidateUniqueIndexSchema(const TTableSchema& tableSchema, const TTableSchema& indexTableSchema)
{
    auto allowSortednessMismatch = [] (const TColumnSchema&) { };
    ValidateIndexSchema(
        tableSchema,
        indexTableSchema,
        ThrowExpectedTypeMatch,
        allowSortednessMismatch,
        ThrowIfKey);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
