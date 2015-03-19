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
using NVersionedTableClient::GetUnversionedRowDataSize;

////////////////////////////////////////////////////////////////////////////////

const size_t TTopN::PoolChunkSize;
const size_t TTopN::BufferLimit;

TTopN::TTopN(size_t limit, TComparerFunc comparer)
    : Comparer({comparer})
{
    Rows.reserve(limit);
}

std::pair<TRow, size_t> TTopN::Capture(TRow row)
{
    if (EmptyBufferIds.empty()) {
        if (GarbageMemorySize > TotalMemorySize / 2) {
            // Collect garbage.

            std::vector<std::vector<size_t>> buffersToRows(Buffers.size());
            for (size_t rowId = 0; rowId < Rows.size(); ++rowId) {
                buffersToRows[Rows[rowId].second].push_back(rowId);
            }

            std::unique_ptr<TRowBuffer> temp = std::make_unique<TRowBuffer>(PoolChunkSize, PoolChunkSize);

            TotalMemorySize = 0;
            AllocatedMemorySize = 0;
            GarbageMemorySize = 0;

            for (size_t bufferId = 0; bufferId < buffersToRows.size(); ++bufferId) {
                auto& buffer = *temp;

                for (auto rowId : buffersToRows[bufferId]) {
                    auto& row = Rows[rowId].first;
                    
                    auto savedSize = buffer.GetSize();
                    row = buffer.Capture(row);
                    AllocatedMemorySize += buffer.GetSize() - savedSize;
                }

                TotalMemorySize += buffer.GetCapacity();

                temp.swap(Buffers[bufferId]);
                temp->Clear();
                EmptyBufferIds.push_back(bufferId);
            }
        } else {
            // Allocate buffer and add to emptyBufferIds.
            EmptyBufferIds.push_back(Buffers.size());
            Buffers.push_back(std::make_unique<TRowBuffer>(PoolChunkSize, PoolChunkSize));
        }
    }

    YCHECK(!EmptyBufferIds.empty());

    auto bufferId = EmptyBufferIds.back();
    auto& buffer = *Buffers[bufferId];

    auto savedSize = buffer.GetSize();
    auto savedCapacity = buffer.GetCapacity();

    auto captured = buffer.Capture(row);

    AllocatedMemorySize += buffer.GetSize() - savedSize;
    TotalMemorySize += buffer.GetCapacity() - savedCapacity;

    if (buffer.GetSize() > BufferLimit) {
        EmptyBufferIds.pop_back();
    }

    return std::make_pair(captured, bufferId);
}

void TTopN::AccountGarbage(TRow row)
{
    GarbageMemorySize += GetUnversionedRowDataSize(row.GetCount());
    for (int index = 0; index < row.GetCount(); ++index) {
        auto value = row[index];

        if (IsStringLikeType(EValueType(value.Type))) {
            GarbageMemorySize += value.Length;
        }
    }
}

void TTopN::AddRowImpl(TRow row)
{
    if (Rows.size() < Rows.capacity()) {
        auto capturedRow = Capture(row);
        Rows.emplace_back(capturedRow);
        std::push_heap(Rows.begin(), Rows.end(), Comparer);
    } else if (!Comparer(Rows.front().first, row)) {
        auto capturedRow = Capture(row);
        std::pop_heap(Rows.begin(), Rows.end(), Comparer);
        AccountGarbage(Rows.back().first);
        Rows.back() = capturedRow;
        std::push_heap(Rows.begin(), Rows.end(), Comparer);
    }
}

void TTopN::AddRow(TTopN* topN, TRow row)
{
    topN->AddRowImpl(row);
}

////////////////////////////////////////////////////////////////////////////////

bool CountRow(i64* limit)
{
    if (*limit > 0) {
        --*limit;
        return false;
    } else {
        return true;
    }
}

TJoinEvaluator GetJoinEvaluator(
    const TJoinClause& joinClause,
    const TConstExpressionPtr& predicate,
    const TTableSchema& selfTableSchema,
    TExecuteQuery executeCallback)
{
    const auto& joinColumns = joinClause.JoinColumns;
    auto& foreignTableSchema = joinClause.ForeignTableSchema;
    auto& foreignKeyColumns = joinClause.ForeignKeyColumns;
    auto foreignPredicate = ExtractPredicateForColumnSubset(predicate, foreignTableSchema);

    // Create subquery TQuery{ForeignDataSplit, foreign predicate and (join columns) in (keys)}.
    TQueryPtr subquery = New<TQuery>(std::numeric_limits<i64>::max(), std::numeric_limits<i64>::max());

    subquery->TableSchema = foreignTableSchema;
    subquery->KeyColumns = foreignKeyColumns;

    auto projectClause = New<TProjectClause>();
    for (const auto& column : foreignTableSchema.Columns()) {
        if (std::find(joinColumns.begin(), joinColumns.end(), column.Name) != joinColumns.end()) {
            projectClause->AddProjection(New<TReferenceExpression>(
                NullSourceLocation,
                column.Type,
                column.Name),
                column.Name);
        }
    }

    for (const auto& column : foreignTableSchema.Columns()) {
        if (std::find(joinColumns.begin(), joinColumns.end(), column.Name) == joinColumns.end()) {
            projectClause->AddProjection(New<TReferenceExpression>(
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

            subquery->WhereClause = New<TInOpExpression>(
                NullSourceLocation,
                joinKeyExprs,
                capturedKeys);

            if (foreignPredicate) {
                subquery->WhereClause = MakeAndExpression(
                    subquery->WhereClause,
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

                    if (executionContext->StopFlag = CountRow(&executionContext->JoinRowLimit)) {
                        executionContext->Statistics->IncompleteOutput = true;
                        return;
                    }

                    joinedRows->push_back(executionContext->PermanentBuffer->Capture(rowBuilder.GetRow()));
                }
            }
        };
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
