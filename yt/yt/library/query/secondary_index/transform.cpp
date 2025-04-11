#include "public.h"
#include "schema.h"

#include <yt/yt/library/query/base/private.h>
#include <yt/yt/library/query/base/query.h>
#include <yt/yt/library/query/base/query_preparer.h>

#include <yt/yt/library/query/base/ast.h>
#include <yt/yt/library/query/base/ast_visitors.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>

namespace NYT::NQueryClient {

using namespace NAst;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NTabletClient;

////////////////////////////////////////////////////////////////////////////////

struct TListContainsTransformer
    : public NAst::TRewriter<TListContainsTransformer>
{
    using TBase = TRewriter<TListContainsTransformer>;

    const TReference& RepeatedIndexedColumn;
    const TReference& UnfoldedIndexerColumn;

    TListContainsTransformer(
        TObjectsHolder* head,
        const TReference& repeatedIndexedColumn,
        const TReference& unfoldedIndexerColumn)
        : TBase(head)
        , RepeatedIndexedColumn(repeatedIndexedColumn)
        , UnfoldedIndexerColumn(unfoldedIndexerColumn)
    { }

    NAst::TExpressionPtr OnFunction(NAst::TFunctionExpressionPtr function)
    {
        if (function->Arguments.size() != 2 || function->FunctionName != "list_contains") {
            return TBase::OnFunction(function);
        }

        auto* reference = function->Arguments[0]->As<NAst::TReferenceExpression>();
        if (!reference || reference->Reference != RepeatedIndexedColumn) {
            return TBase::OnFunction(function);
        }

        auto* newReference = Head->New<NAst::TReferenceExpression>(
            NullSourceLocation,
            UnfoldedIndexerColumn);

        return Head->New<NAst::TBinaryOpExpression>(
            NullSourceLocation,
            EBinaryOp::Equal,
            TExpressionList{newReference},
            TExpressionList{function->Arguments[1]});
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TInTransformer
    : public NAst::TRewriter<TInTransformer>
{
    using TBase = TRewriter<TInTransformer>;

    const TReference& RepeatedIndexedColumn;
    const TReference& UnfoldedIndexerColumn;

    TInTransformer(
        TObjectsHolder* head,
        const TReference& repeatedIndexedColumn,
        const TReference& unfoldedIndexerColumn)
        : TBase(head)
        , RepeatedIndexedColumn(repeatedIndexedColumn)
        , UnfoldedIndexerColumn(unfoldedIndexerColumn)
    { }

    NAst::TExpressionPtr OnIn(NAst::TInExpressionPtr inExpr)
    {
        if (inExpr->Expr.size() != 1) {
            return TBase::OnIn(inExpr);
        }

        auto* reference = inExpr->Expr[0]->As<NAst::TReferenceExpression>();
        if (reference->Reference != RepeatedIndexedColumn) {
            return TBase::OnIn(inExpr);
        }

        auto* newReference = Head->New<NAst::TReferenceExpression>(
            NullSourceLocation,
            UnfoldedIndexerColumn);

        return Head->New<NAst::TInExpression>(
            NullSourceLocation,
            TExpressionList{newReference},
            inExpr->Values);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTableReferenceReplacer
    : public NAst::TRewriter<TTableReferenceReplacer>
{
    using TBase = TRewriter<TTableReferenceReplacer>;

    const THashSet<TStringBuf>& ReplacedColumns;
    const std::optional<TString>& OldAlias;
    const std::optional<TString>& NewAlias;

    TTableReferenceReplacer(
        TObjectsHolder* head,
        const THashSet<TStringBuf>& replacedColumns,
        const std::optional<TString>& oldAlias,
        const std::optional<TString>& newAlias)
        : TBase(head)
        , ReplacedColumns(replacedColumns)
        , OldAlias(oldAlias)
        , NewAlias(newAlias)
    { }

    NAst::TExpressionPtr OnReference(TReferenceExpressionPtr reference)
    {
        const auto& columnName = reference->Reference.ColumnName;
        if (OldAlias != reference->Reference.TableName || !ReplacedColumns.contains(columnName)) {
            return reference;
        }

        return Head->New<NAst::TReferenceExpression>(NullSourceLocation, columnName, NewAlias);
    }
};

////////////////////////////////////////////////////////////////////////////////

void TransformWithIndexStatement(
    NAst::TQuery* query,
    const ITableMountCachePtr& cache,
    TObjectsHolder* holder)
{
    if (auto* fromSubquery = std::get_if<NAst::TQueryAstHeadPtr>(&query->FromClause)) {
        THROW_ERROR_EXCEPTION_IF(query->WithIndex,
            "WITH INDEX clause is not supported with subqueries at the moment");

        TransformWithIndexStatement(&fromSubquery->Get()->Ast, cache, holder);
        return;
    }

    if (!query->WithIndex) {
        return;
    }

    auto& index = *(query->WithIndex);
    auto& table = std::get<TTableDescriptor>(query->FromClause);

    auto indexTableInfo = WaitForFast(cache->GetTableInfo(index.Path))
        .ValueOrThrow();
    auto tableInfo = WaitForFast(cache->GetTableInfo(table.Path))
        .ValueOrThrow();

    indexTableInfo->ValidateDynamic();
    indexTableInfo->ValidateSorted();

    const auto& indexTableSchema = *indexTableInfo->Schemas[ETableSchemaKind::Write];
    const auto& tableSchema = tableInfo->Schemas[ETableSchemaKind::Write];
    const auto& indices = tableInfo->Indices;

    const TColumnSchema* unfoldedColumn{};
    auto indexIt = std::find_if(indices.begin(), indices.end(), [&] (const TIndexInfo& index) {
        return index.TableId == indexTableInfo->TableId;
    });

    auto correspondence = ETableToIndexCorrespondence::Unknown;

    if (indexIt == indices.end()) {
        ValidateIndexSchema(
            ESecondaryIndexKind::FullSync,
            *tableSchema,
            indexTableSchema,
            /*predicate*/ std::nullopt,
            /*evaluatedColumnsSchema*/ nullptr);
    } else {
        correspondence = indexIt->Correspondence;
        if (correspondence == ETableToIndexCorrespondence::Invalid) {
            THROW_ERROR_EXCEPTION("Cannot use index %v with %Qlv correspondence",
                indexIt->TableId,
                correspondence)
                << TErrorAttribute("index_table_path", indexTableInfo->Path);
        }

        // COMPAT(sabdenovch)
        if (indexIt->Kind == ESecondaryIndexKind::Unfolding && !indexIt->UnfoldedColumn) {
            unfoldedColumn = &FindUnfoldingColumnAndValidate(
                *tableSchema,
                indexTableSchema,
                indexIt->Predicate,
                indexIt->EvaluatedColumnsSchema);
        } else {
            if (indexIt->UnfoldedColumn) {
                unfoldedColumn = &indexTableSchema.GetColumn(*indexIt->UnfoldedColumn);
            }

            ValidateIndexSchema(
                indexIt->Kind,
                *tableSchema,
                indexTableSchema,
                indexIt->Predicate,
                indexIt->EvaluatedColumnsSchema,
                indexIt->UnfoldedColumn);
        }
    }

    if (!index.Alias) {
        index.Alias = SecondaryIndexAlias;
    }


    if (unfoldedColumn) {
        NAst::TReference repeatedIndexedColumn(unfoldedColumn->Name(), table.Alias);
        NAst::TReference unfoldedIndexerColumn(unfoldedColumn->Name(), index.Alias);

        query->WherePredicate = TListContainsTransformer(
            holder,
            repeatedIndexedColumn,
            unfoldedIndexerColumn)
            .Visit(query->WherePredicate);

        query->WherePredicate = TInTransformer(
            holder,
            repeatedIndexedColumn,
            unfoldedIndexerColumn)
            .Visit(query->WherePredicate);
    }

    NAst::TExpressionList indexJoinColumns;
    NAst::TExpressionList tableJoinColumns;

    THashSet<TStringBuf> replacedColumns;

    for (const auto& tableColumn : tableSchema->Columns()) {
        const auto* indexColumn = indexTableSchema.FindColumn(tableColumn.Name());

        if (!indexColumn || *indexColumn->LogicalType() != *tableColumn.LogicalType()) {
            continue;
        }

        replacedColumns.insert(indexColumn->Name());

        if (correspondence == ETableToIndexCorrespondence::Bijective && !tableColumn.SortOrder()) {
            continue;
        }

        auto* indexReference = holder->New<NAst::TReferenceExpression>(
            NullSourceLocation,
            indexColumn->Name(),
            index.Alias);
        auto* tableReference = holder->New<NAst::TReferenceExpression>(
            NullSourceLocation,
            tableColumn.Name(),
            table.Alias);

        indexJoinColumns.push_back(indexReference);
        tableJoinColumns.push_back(tableReference);
    }

    THROW_ERROR_EXCEPTION_IF(tableJoinColumns.empty(),
        "Misuse of operator WITH INDEX: tables %v and %v have no shared columns",
        table.Path,
        index.Path);

    query->WherePredicate = TTableReferenceReplacer(
        holder,
        std::move(replacedColumns),
        table.Alias,
        index.Alias)
        .Visit(query->WherePredicate);

    std::swap(table, index);
    query->Joins.insert(
        query->Joins.begin(),
        NAst::TJoin(
            /*isLeft*/ false,
            std::move(index),
            std::move(indexJoinColumns),
            std::move(tableJoinColumns),
            /*predicate*/ std::nullopt));

    query->WithIndex.reset();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
