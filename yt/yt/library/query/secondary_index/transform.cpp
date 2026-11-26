#include "public.h"
#include "schema.h"

#include <yt/yt/library/query/base/private.h>
#include <yt/yt/library/query/base/query.h>
#include <yt/yt/library/query/base/query_preparer.h>

#include <yt/yt/library/query/base/ast.h>
#include <yt/yt/library/query/base/ast_visitors.h>

#include <yt/yt/client/chaos_client/replication_card_cache.h>

#include <yt/yt/client/tablet_client/table_mount_cache.h>

namespace NYT::NQueryClient {

using namespace NAst;
using namespace NChaosClient;
using namespace NConcurrency;
using namespace NObjectClient;
using namespace NTabletClient;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = QueryClientLogger;

////////////////////////////////////////////////////////////////////////////////

struct TListContainsTransformer
    : public NAst::TRewriter<TListContainsTransformer>
{
    using TBase = TRewriter<TListContainsTransformer>;

    const TReference& TableUnfoldedColumn;
    const TReference& IndexUnfoldedColumn;

    TListContainsTransformer(
        TObjectsHolder* head,
        const TReference& tableUnfoledColumn,
        const TReference& indexUnfoldedColumn)
        : TBase(head)
        , TableUnfoldedColumn(tableUnfoledColumn)
        , IndexUnfoldedColumn(indexUnfoldedColumn)
    { }

    NAst::TExpressionPtr OnFunction(NAst::TFunctionExpressionPtr function)
    {
        if (function->Arguments.size() != 2 || function->FunctionName != "list_contains") {
            return TBase::OnFunction(function);
        }

        auto* reference = function->Arguments[0]->As<NAst::TReferenceExpression>();
        if (!reference || reference->Reference != TableUnfoldedColumn) {
            return TBase::OnFunction(function);
        }

        auto* newReference = Head->New<NAst::TReferenceExpression>(
            NullSourceLocation,
            IndexUnfoldedColumn);

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
    const std::optional<std::string>& OldAlias;
    const std::optional<std::string>& NewAlias;

    TTableReferenceReplacer(
        TObjectsHolder* head,
        const THashSet<TStringBuf>& replacedColumns,
        const std::optional<std::string>& oldAlias,
        const std::optional<std::string>& newAlias)
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
    const ITableMountCachePtr& mountInfoCache,
    const IReplicationCardCachePtr& replicationCardCache,
    TObjectsHolder* holder)
{
    if (auto* fromSubquery = std::get_if<NAst::TQueryAstHeadPtr>(&query->FromClause)) {
        THROW_ERROR_EXCEPTION_IF(query->WithIndex,
            "WITH INDEX clause is not supported with subqueries at the moment");

        TransformWithIndexStatement(&(*fromSubquery)->Ast, mountInfoCache, replicationCardCache, holder);
        return;
    }

    if (auto* unionAll = std::get_if<NAst::TUnionAllClause>(&query->FromClause)) {
        THROW_ERROR_EXCEPTION_IF(query->WithIndex,
            "WITH INDEX clause is not supported with UNION ALL at the moment");

        for (auto* subquery : unionAll->Subqueries) {
            TransformWithIndexStatement(&subquery->Ast, mountInfoCache, replicationCardCache, holder);
        }
        return;
    }

    if (!query->WithIndex) {
        return;
    }

    auto& index = *(query->WithIndex);
    auto& table = std::get<TTableDescriptor>(query->FromClause);

    auto indexTableInfo = WaitForFast(mountInfoCache->GetTableInfo(index.Path))
        .ValueOrThrow();
    auto tableInfo = WaitForFast(mountInfoCache->GetTableInfo(table.Path))
        .ValueOrThrow();

    indexTableInfo->ValidateDynamic();
    indexTableInfo->ValidateSorted();

    const auto& indexTableSchema = *indexTableInfo->Schemas[ETableSchemaKind::Write];
    const auto& tableSchema = tableInfo->Schemas[ETableSchemaKind::Write];
    auto indices = tableInfo->Indices;
    if (tableInfo->ReplicationCardId) {
        THROW_ERROR_EXCEPTION_UNLESS(replicationCardCache, "Replication card cache is not configured");

        auto replicationCard = WaitForFast(replicationCardCache->GetReplicationCard({
            .CardId = tableInfo->ReplicationCardId,
            .FetchOptions = MinimalFetchOptions,
        }))
            .ValueOrThrow();

        indices.reserve(replicationCard->SecondaryIndices.size());

        for (const auto& [_, indexInfo] : replicationCard->SecondaryIndices) {
            indices.push_back(indexInfo);
        }
    }

    YT_TLOG_DEBUG("Got indices for table")
        .With("Table", tableInfo->Path)
        .With("Indices", MakeFormattableView(indices, [] (TStringBuilderBase* builder, const TIndexInfo& index) {
            builder->AppendFormat("IndexObjectId: %v, Kind: %Qv, Correspondence: %Qv",
                index.IndexObjectId,
                index.Kind,
                index.Correspondence);
        }));

    auto indexIt = std::find_if(indices.begin(), indices.end(), [&] (const TIndexInfo& index) {
        return index.IndexObjectId == indexTableInfo->TableId || index.IndexObjectId == indexTableInfo->ReplicationCardId;
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
            THROW_ERROR_EXCEPTION("Cannot use index %v with %Qv correspondence",
                indexIt->IndexObjectId,
                correspondence)
                .With("index_table_path", indexTableInfo->Path);
        }

        ValidateIndexSchema(
            indexIt->Kind,
            *tableSchema,
            indexTableSchema,
            indexIt->Predicate,
            indexIt->EvaluatedColumnsSchema,
            indexIt->UnfoldedColumns);
    }

    if (!index.Alias) {
        THROW_ERROR_EXCEPTION("Misuse of operator WITH INDEX: index table %v has no alias",
            index.Path);
    }

    if (indexIt != indices.end() && indexIt->Kind == ESecondaryIndexKind::Unfolding) {
        NAst::TReference tableUnfoldedColumn(indexIt->UnfoldedColumns->TableColumn, table.Alias);
        NAst::TReference indexUnfoldedColumn(indexIt->UnfoldedColumns->IndexColumn, index.Alias);

        query->WherePredicate = TListContainsTransformer(
            holder,
            tableUnfoldedColumn,
            indexUnfoldedColumn)
            .Visit(query->WherePredicate);

        query->WherePredicate = TInTransformer(
            holder,
            tableUnfoldedColumn,
            indexUnfoldedColumn)
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
        replacedColumns,
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
