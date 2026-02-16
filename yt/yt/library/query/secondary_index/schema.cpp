#include "schema.h"

#include <yt/yt/library/heavy_schema_validation/schema_validation.h>

#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/schema.h>

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

using namespace NTableClient;
using namespace NTabletClient;

////////////////////////////////////////////////////////////////////////////////

using TColumnMismatchCallback = std::function<void(const TColumnSchema& indexColumn, const TColumnSchema& tableColumn)>;
using TBadColumnCallback = std::function<void(const TColumnSchema& column)>;
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

bool IsValidUnfoldedColumnPair(const TLogicalTypePtr& tableColumnType, const TLogicalTypePtr& indexColumnType)
{
    auto tableColumnElementType = tableColumnType;

    if (tableColumnElementType->GetMetatype() == ELogicalMetatype::Optional) {
        tableColumnElementType = tableColumnElementType->UncheckedAsOptionalTypeRef().GetElement();
    }

    if (*tableColumnElementType == *SimpleLogicalType(ESimpleLogicalValueType::Any)) {
        return true;
    }

    return *ListLogicalType(indexColumnType) == *tableColumnElementType;
}

////////////////////////////////////////////////////////////////////////////////

class TLockGroupValidator
{
public:
    void ValidateColumn(const TColumnSchema& column)
    {
        if (column.SortOrder()) {
            return;
        }

        if (LockGroup_) {
            THROW_ERROR_EXCEPTION_IF(*LockGroup_ != column.Lock(),
                "All indexed table columns, columns referenced in predicate and "
                "columns referenced in evaluated columns must have same lock group, "
                "found %Qv for column %Qv and %Qv for column %Qv",
                *LockGroup_,
                *ColumnName_,
                column.Lock(),
                column.Name());
        } else {
            LockGroup_ = column.Lock();
            ColumnName_ = column.Name();
        }
    }

private:
    std::optional<std::optional<std::string>> LockGroup_;
    std::optional<std::string> ColumnName_;
};

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TIndexSchemaValidator)

class TIndexSchemaValidator
{
public:
    TIndexSchemaValidator(
        ESecondaryIndexKind kind,
        const TTableSchema& tableSchema,
        const TTableSchema& indexTableSchema,
        const std::optional<std::string>& predicate,
        const TTableSchemaPtr& evaluatedColumnsSchema,
        const std::optional<TUnfoldedColumns>& unfoldedColumns)
        : Kind_(kind)
        , TableSchema_(tableSchema)
        , IndexTableSchema_(indexTableSchema)
        , Predicate_(predicate)
        , EvaluatedColumnsSchema_(evaluatedColumnsSchema)
        , UnfoldedColumns_(unfoldedColumns)
    {
        THROW_ERROR_EXCEPTION_IF(!TableSchema_.IsSorted(),
            "Table must be sorted");
        THROW_ERROR_EXCEPTION_IF(!TableSchema_.IsUniqueKeys(),
            "Table must have unique keys");
    }

    void Validate()
    {
        switch (Kind_) {
            case ESecondaryIndexKind::FullSync:
                ValidateFullSyncIndexSchema();
                return;

            case ESecondaryIndexKind::Unfolding:
                ValidateUnfoldingIndexSchema();
                return;

            case ESecondaryIndexKind::Unique:
                ValidateUniqueIndexSchema();
                return;

            default:
                THROW_ERROR_EXCEPTION("Unsupported index kind %Qlv", Kind_);
        }
    }

private:
    const ESecondaryIndexKind Kind_;
    const TTableSchema& TableSchema_;
    const TTableSchema& IndexTableSchema_;
    const std::optional<std::string>& Predicate_;
    const TTableSchemaPtr& EvaluatedColumnsSchema_;
    const std::optional<TUnfoldedColumns>& UnfoldedColumns_;

    TLockGroupValidator LockValidator_;

    void ValidateFullSyncIndexSchema()
    {
        ValidateIndexSchema(
            ThrowExpectedTypeMatch,
            ThrowExpectedKeyColumn,
            ThrowIfNonKey);
    }

    void ValidateUnfoldingIndexSchema()
    {
        YT_VERIFY(UnfoldedColumns_);

        auto typeMismatchCallback = [&] (const TColumnSchema& indexColumn, const TColumnSchema& tableColumn) {
            if (tableColumn.Name() != UnfoldedColumns_->TableColumn ||
                indexColumn.Name() != UnfoldedColumns_->IndexColumn)
            {
                ThrowExpectedTypeMatch(indexColumn, tableColumn);
            }
        };

        auto extraIndexColumnCallback = [&] (const TColumnSchema& indexColumn) {
            if (indexColumn.Name() != UnfoldedColumns_->IndexColumn) {
                ThrowIfNonKey(indexColumn);
            }
        };

        ValidateIndexSchema(
            typeMismatchCallback,
            ThrowExpectedKeyColumn,
            extraIndexColumnCallback);

        const auto& indexUnfoldingColumn = IndexTableSchema_.GetColumnOrThrow(UnfoldedColumns_->IndexColumn);
        const TColumnSchema* unfoldedColumn = nullptr;
        if (const auto* tableUnfoldedColumn = TableSchema_.FindColumn(UnfoldedColumns_->TableColumn)) {
            unfoldedColumn = tableUnfoldedColumn;
        } else if (const auto* evaluatedUnfoldedColumn = EvaluatedColumnsSchema_
            ? EvaluatedColumnsSchema_->FindColumn(UnfoldedColumns_->TableColumn)
            : nullptr)
        {
            unfoldedColumn = evaluatedUnfoldedColumn;
        } else {
            THROW_ERROR_EXCEPTION("Neither indexed table schema not index evaluated columns contain unfolded column %Qv",
                UnfoldedColumns_->TableColumn);
        }

        THROW_ERROR_EXCEPTION_IF(!IsValidUnfoldedColumnPair(unfoldedColumn->LogicalType(), indexUnfoldingColumn.LogicalType()),
            "Type mismatch for the unfolded column %Qv: %v is not a list of %v",
            indexUnfoldingColumn.Name(),
            *unfoldedColumn->LogicalType(),
            *indexUnfoldingColumn.LogicalType());
    }

    void ValidateUniqueIndexSchema()
    {
        auto allowSortednessMismatch = [] (const TColumnSchema&) { };
        ValidateIndexSchema(
            ThrowExpectedTypeMatch,
            allowSortednessMismatch,
            ThrowIfKey);
    }

    void ValidateIndexSchema(
        const TColumnMismatchCallback& typeMismatchCallback,
        const TBadColumnCallback& tableKeyIsNotIndexKeyCallback,
        const TExtraIndexColumnCallback& extraIndexColumnCallback)
    {
        THROW_ERROR_EXCEPTION_IF(!IndexTableSchema_.IsSorted(),
            "Index table must be sorted");

        THROW_ERROR_EXCEPTION_IF(!IndexTableSchema_.IsUniqueKeys(),
            "Index table must have unique keys");

        ValidatePredicate(Predicate_);
        ValidateIndexEvaluatedColumns(typeMismatchCallback);

        for (const auto& tableColumn : TableSchema_.Columns()) {
            if (!tableColumn.SortOrder()) {
                break;
            }

            if (tableColumn.Expression()) {
                continue;
            }

            const auto* indexColumn = IndexTableSchema_.FindColumn(tableColumn.Name());
            if (!indexColumn) {
                THROW_ERROR_EXCEPTION("Key column %Qv missing in the index schema",
                    tableColumn.Name());
            }

            if (!indexColumn->SortOrder()) {
                tableKeyIsNotIndexKeyCallback(tableColumn);
            }
        }

        for (const auto& indexColumn : IndexTableSchema_.Columns()) {
            if (const auto* tableColumn = TableSchema_.FindColumn(indexColumn.Name())) {
                if (indexColumn.Expression()) {
                    THROW_ERROR_EXCEPTION_UNLESS(tableColumn->Expression(),
                        "Column %Qv is evaluated in index and not evaluated in table",
                        indexColumn.Name());
                    continue;
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
                    LockValidator_.ValidateColumn(*tableColumn);
                }

                if (indexColumn.Aggregate()) {
                    THROW_ERROR_EXCEPTION_UNLESS(Kind_ == ESecondaryIndexKind::Unfolding,
                        "Index table of kind %Qlv cannot have aggregate columns, got %Qv with function %Qv",
                        Kind_,
                        indexColumn.Name(),
                        *indexColumn.Aggregate());
                }
            } else if (EvaluatedColumnsSchema_ && EvaluatedColumnsSchema_->FindColumn(indexColumn.Name())) {
                continue;
            } else if (indexColumn.Name() == EmptyValueColumnName) {
                THROW_ERROR_EXCEPTION_IF(indexColumn.Required(),
                    "Column %Qv must have a nullable type, found %v",
                    EmptyValueColumnName,
                    *indexColumn.LogicalType());

                THROW_ERROR_EXCEPTION_IF(indexColumn.SortOrder(),
                    "Column %Qv must not be a key",
                    EmptyValueColumnName);
            } else {
                extraIndexColumnCallback(indexColumn);
            }
        }
    }

    void ValidatePredicate(const std::optional<std::string>& predicate)
    {
        if (!predicate) {
            return;
        }

        auto references = ValidateComputedColumnExpression(
            TColumnSchema("predicate", EValueType::Boolean)
                .SetExpression(predicate),
            TableSchema_,
            /*isTableDynamic*/ true,
            /*allowDependenceOnNonKeyColumns*/ true);

        for (const auto& reference : references) {
            LockValidator_.ValidateColumn(TableSchema_.GetColumn(reference));
        }
    }

    void ValidateIndexEvaluatedColumns(const TColumnMismatchCallback& typeMismatchCallback)
    {
        if (!EvaluatedColumnsSchema_) {
            return;
        }

        ValidateColumnsCollision(TableSchema_, *EvaluatedColumnsSchema_);

        for (const auto& column : EvaluatedColumnsSchema_->Columns()) {
            THROW_ERROR_EXCEPTION_IF(!column.Expression(), "Expected an expression for the evaluated column %Qv",
                column.Name());

            THROW_ERROR_EXCEPTION_IF(column.Aggregate(), "Evaluated column %Qv cannot be aggregate",
                column.Name());

            if (const auto* indexColumn = IndexTableSchema_.FindColumn(column.Name())) {
                if (*indexColumn->LogicalType() != *column.LogicalType()) {
                    typeMismatchCallback(*indexColumn, column);
                }
            }

            auto references = ValidateComputedColumnExpression(
                column,
                TableSchema_,
                /*isTableDynamic*/ true,
                /*allowDependenceOnNonKeyColumns*/ true);

            for (const auto& reference : references) {
                LockValidator_.ValidateColumn(TableSchema_.GetColumn(reference));
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

void ValidateIndexSchema(
    ESecondaryIndexKind kind,
    const TTableSchema& tableSchema,
    const TTableSchema& indexTableSchema,
    const std::optional<std::string>& predicate,
    const TTableSchemaPtr& evaluatedColumnsSchema,
    const std::optional<TUnfoldedColumns>& unfoldedColumns)
{
    TIndexSchemaValidator(
        kind,
        tableSchema,
        indexTableSchema,
        predicate,
        evaluatedColumnsSchema,
        unfoldedColumns)
        .Validate();
}

////////////////////////////////////////////////////////////////////////////////

void ValidateColumnsCollision(const TTableSchema& lhs, const TTableSchema& rhs)
{
    for (const auto& lhsColumn : rhs.Columns()) {
        if (const auto* rhsColumn = lhs.FindColumn(lhsColumn.Name())) {
            THROW_ERROR_EXCEPTION_IF(lhsColumn != *rhsColumn,
                "Columns collision on %Qv between %v and %v",
                lhsColumn.Name(),
                lhsColumn,
                *rhsColumn);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
