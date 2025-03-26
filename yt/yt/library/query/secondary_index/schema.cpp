#include "schema.h"

#include <yt/yt/library/heavy_schema_validation/schema_validation.h>

#include <yt/yt/client/table_client/logical_type.h>
#include <yt/yt/client/table_client/schema.h>

namespace NYT::NQueryClient {

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

    if (tableColumnElementType->GetMetatype() != ELogicalMetatype::List) {
        return false;
    }

    tableColumnElementType = tableColumnElementType->UncheckedAsListTypeRef().GetElement();

    return *tableColumnElementType == *indexColumnType;
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
        const std::optional<TString>& predicate,
        const TTableSchemaPtr& evaluatedColumnsSchema,
        const std::optional<TString>& unfoldedColumnName)
        : Kind_(kind)
        , TableSchema_(tableSchema)
        , IndexTableSchema_(indexTableSchema)
        , Predicate_(predicate)
        , EvaluatedColumnsSchema_(evaluatedColumnsSchema)
        , UnfoldedColumnName_(unfoldedColumnName)
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

    const TColumnSchema& FindUnfoldingColumnAndValidate()
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

        ValidateIndexSchema(
            typeMismatchCallback,
            ThrowExpectedKeyColumn,
            ThrowIfNonKey);

        THROW_ERROR_EXCEPTION_IF(!unfoldedColumn,
            "No candidate for unfolded column found in the index table schema");

        return *unfoldedColumn;
    }

private:
    const ESecondaryIndexKind Kind_;
    const TTableSchema& TableSchema_;
    const TTableSchema& IndexTableSchema_;
    const std::optional<TString>& Predicate_;
    const TTableSchemaPtr& EvaluatedColumnsSchema_;
    const std::optional<TString>& UnfoldedColumnName_;

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
        YT_VERIFY(UnfoldedColumnName_);

        auto typeMismatchCallback = [&] (const TColumnSchema& indexColumn, const TColumnSchema& tableColumn) {
            if (indexColumn.Name() == UnfoldedColumnName_) {
                THROW_ERROR_EXCEPTION_IF(!IsValidUnfoldedColumnPair(tableColumn.LogicalType(), indexColumn.LogicalType()),
                    "Type mismatch for the unfolded column %Qv: %v is not a list of %v",
                    UnfoldedColumnName_,
                    *tableColumn.LogicalType(),
                    *indexColumn.LogicalType());
            } else {
                ThrowExpectedTypeMatch(indexColumn, tableColumn);
            }
        };

        ValidateIndexSchema(
            typeMismatchCallback,
            ThrowExpectedKeyColumn,
            ThrowIfNonKey);

        const auto& unfoldingColumn = IndexTableSchema_.GetColumnOrThrow(*UnfoldedColumnName_);
        const TColumnSchema* unfoldedColumn = nullptr;
        if (const auto* tableUnfoldedColumn = TableSchema_.FindColumn(*UnfoldedColumnName_)) {
            unfoldedColumn = tableUnfoldedColumn;
        } else if (EvaluatedColumnsSchema_) {
            unfoldedColumn = EvaluatedColumnsSchema_->FindColumn(*UnfoldedColumnName_);
        }

        if (!unfoldedColumn) {
            THROW_ERROR_EXCEPTION("Neither indexed table schema not index evaluated columns contain unfolded column %Qv",
                *UnfoldedColumnName_);
        }

        THROW_ERROR_EXCEPTION_IF(!IsValidUnfoldedColumnPair(unfoldedColumn->LogicalType(), unfoldingColumn.LogicalType()),
            "Type mismatch for the unfolded column %Qv: %v is not a list of %v",
            unfoldingColumn.Name(),
            *unfoldedColumn->LogicalType(),
            *unfoldingColumn.LogicalType());
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
        TColumnMismatchCallback typeMismatchCallback,
        TBadColumnCallback tableKeyIsNotIndexKeyCallback,
        TExtraIndexColumnCallback extraIndexColumnCallback)
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

            auto* indexColumn = IndexTableSchema_.FindColumn(tableColumn.Name());
            if (!indexColumn) {
                THROW_ERROR_EXCEPTION("Key column %Qv missing in the index schema",
                    tableColumn.Name());
            }

            if (!indexColumn->SortOrder()) {
                tableKeyIsNotIndexKeyCallback(tableColumn);
            }
        }

        for (const auto& indexColumn : IndexTableSchema_.Columns()) {
            THROW_ERROR_EXCEPTION_IF(indexColumn.Aggregate(),
                "Index table cannot have aggregate columns, found aggregate column %Qv with function %Qv",
                indexColumn.Name(),
                indexColumn.Aggregate());

            if (auto* tableColumn = TableSchema_.FindColumn(indexColumn.Name())) {
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

    void ValidatePredicate(const std::optional<TString>& predicate)
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

    void ValidateIndexEvaluatedColumns(TColumnMismatchCallback typeMismatchCallback)
    {
        if (!EvaluatedColumnsSchema_) {
            return;
        }

        ValidateNoNameCollisions(TableSchema_, *EvaluatedColumnsSchema_);

        for (const auto& column : EvaluatedColumnsSchema_->Columns()) {
            THROW_ERROR_EXCEPTION_IF(!column.Expression(), "Expected an expression for the evaluated column %Qv",
                column.Name());

            THROW_ERROR_EXCEPTION_IF(column.Aggregate(), "Evaluated column %Qv cannot be aggregate",
                column.Name());

            auto* indexColumn = IndexTableSchema_.FindColumn(column.Name());

            THROW_ERROR_EXCEPTION_IF(!indexColumn, "Evaluated column %Qv not found in index schema",
                column);

            if (*indexColumn->LogicalType() != *column.LogicalType()) {
                typeMismatchCallback(*indexColumn, column);
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
    const TTableSchema& IndexTableSchema_,
    const std::optional<TString>& predicate,
    const TTableSchemaPtr& evaluatedColumnsSchema,
    const std::optional<TString> UnfoldedColumnName_)
{
    TIndexSchemaValidator(
        kind,
        tableSchema,
        IndexTableSchema_,
        predicate,
        evaluatedColumnsSchema,
        UnfoldedColumnName_)
        .Validate();
}

const TColumnSchema& FindUnfoldingColumnAndValidate(
    const TTableSchema& tableSchema,
    const TTableSchema& indexTableSchema,
    const std::optional<TString>& predicate,
    const TTableSchemaPtr& evaluatedColumnsSchema)
{
    return TIndexSchemaValidator(
        ESecondaryIndexKind::Unfolding,
        tableSchema,
        indexTableSchema,
        predicate,
        evaluatedColumnsSchema,
        /*unfoldedColumnName*/ std::nullopt)
        .FindUnfoldingColumnAndValidate();
}

////////////////////////////////////////////////////////////////////////////////

void ValidateNoNameCollisions(const TTableSchema& lhs, const TTableSchema& rhs)
{
    for (auto& lhsColumn : rhs.Columns()) {
        if (auto* rhsColumn = lhs.FindColumn(lhsColumn.Name())) {
            THROW_ERROR_EXCEPTION("Name collision on %Qv between columns %v and %v",
                lhsColumn.Name(),
                lhsColumn,
                *rhsColumn);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
