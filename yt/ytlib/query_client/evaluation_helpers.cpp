#include "stdafx.h"
#include "evaluation_helpers.h"

#include "plan_helpers.h"
#include "helpers.h"
#include "query_statistics.h"

#include <core/concurrency/scheduler.h>

#include <core/profiling/scoped_timer.h>

namespace NYT {
namespace NQueryClient {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TJoinEvaluator GetJoinEvaluator(
    const TJoinClause& joinClause,
    const TConstExpressionPtr& predicate,
    TExecuteQuery executeCallback)
{
    const auto& joinColumns = joinClause.JoinColumns;
    auto& foreignTableSchema = joinClause.ForeignTableSchema;
    auto& foreignKeyColumns = joinClause.ForeignKeyColumns;
    auto foreignPredicate = ExtractPredicateForColumnSubset(predicate, foreignTableSchema);
    const auto& selfTableSchema = joinClause.SelfTableSchema;

    // Create subquery TQuery{ForeignDataSplit, foreign predicate and (join columns) in (keys)}.
    TQueryPtr subquery = New<TQuery>(std::numeric_limits<i64>::max(), std::numeric_limits<i64>::max());

    subquery->TableSchema = foreignTableSchema;
    subquery->KeyColumns = foreignKeyColumns;

    TProjectClause projectClause;
    for (const auto& column : foreignTableSchema.Columns()) {
        if (std::find(joinColumns.begin(), joinColumns.end(), column.Name) != joinColumns.end()) {
            projectClause.Projections.emplace_back(New<TReferenceExpression>(
                NullSourceLocation,
                column.Type,
                column.Name),
                column.Name);
        }
    }

    for (const auto& column : foreignTableSchema.Columns()) {
        if (std::find(joinColumns.begin(), joinColumns.end(), column.Name) == joinColumns.end()) {
            projectClause.Projections.emplace_back(New<TReferenceExpression>(
                NullSourceLocation,
                column.Type,
                column.Name),
                column.Name);
        }
    }

    subquery->ProjectClause = projectClause;

    std::vector<TConstExpressionPtr> joinKeyExprs;
    for (const auto& column : joinColumns) {
        joinKeyExprs.push_back(New<TReferenceExpression>(
            NullSourceLocation,
            foreignTableSchema.GetColumnOrThrow(column).Type,
            column));
    }

    return [=, executeCallback = std::move(executeCallback)] (
            TExecutionContext* executionContext,
            ui64 (*groupHasher)(TRow),
            char (*groupComparer)(TRow, TRow),
            const std::vector<TRow>& keys,
            const std::vector<TRow>& allRows,
            std::vector<TRow>* joinedRows)
        {
            std::vector<TOwningRow> capturedKeys;
            for (const auto& key : keys) {
                capturedKeys.emplace_back(key);
            }
            std::sort(capturedKeys.begin(), capturedKeys.end());

            subquery->Predicate = New<TInOpExpression>(
                NullSourceLocation,
                joinKeyExprs,
                capturedKeys);

            if (foreignPredicate) {
                subquery->Predicate = MakeAndExpression(
                    subquery->Predicate,
                    foreignPredicate);
            }

            // Execute subquery.
            NApi::IRowsetPtr rowset;

            {
                ISchemafulWriterPtr writer;
                TFuture<NApi::IRowsetPtr> rowsetFuture;
                std::tie(writer, rowsetFuture) = NApi::CreateSchemafulRowsetWriter();
                NProfiling::TAggregatingTimingGuard timingGuard(&executionContext->Statistics->AsyncTime);

                auto statistics = executeCallback(subquery, writer);
                *executionContext->Statistics += statistics;

                rowset = WaitFor(rowsetFuture).ValueOrThrow();
            }

            const auto& foreignRows = rowset->GetRows();  

            TJoinLookupRows foreignLookup(
                InitialGroupOpHashtableCapacity,
                groupHasher,
                groupComparer);

            for (auto row : foreignRows) {
                foreignLookup.insert(row);
            }

            // Join rowsets.
            TRowBuilder rowBuilder;
            for (auto row : allRows) {
                rowBuilder.Reset();
                for (const auto& column : joinColumns) {
                    rowBuilder.AddValue(row[selfTableSchema.GetColumnIndexOrThrow(column)]);
                }

                auto equalRange = foreignLookup.equal_range(rowBuilder.GetRow());
                for (auto it = equalRange.first; it != equalRange.second; ++it) {
                    rowBuilder.Reset();

                    for (int valueIndex = 0; valueIndex < row.GetCount(); ++valueIndex) {
                        rowBuilder.AddValue(row[valueIndex]);
                    }

                    auto foreignRow = *it;
                    for (int valueIndex = joinColumns.size(); valueIndex < foreignRow.GetCount(); ++valueIndex) {
                        rowBuilder.AddValue(foreignRow[valueIndex]);
                    }

                    joinedRows->push_back(executionContext->PermanentBuffer->Capture(rowBuilder.GetRow()));
                }
            }
        };
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
