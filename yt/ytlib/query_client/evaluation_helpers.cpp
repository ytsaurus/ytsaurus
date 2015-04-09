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

const i64 PoolChunkSize = 32 * 1024;
const i64 BufferLimit = 32 * PoolChunkSize;

TTopCollector::TTopCollector(i64 limit, NDetail::TComparerFunc comparer)
    : Comparer_(comparer)
{
    Rows_.reserve(limit);
}

std::pair<TRow, int> TTopCollector::Capture(TRow row)
{
    if (EmptyBufferIds_.empty()) {
        if (GarbageMemorySize_ > TotalMemorySize_ / 2) {
            // Collect garbage.

            std::vector<std::vector<size_t>> buffersToRows(Buffers_.size());
            for (size_t rowId = 0; rowId < Rows_.size(); ++rowId) {
                buffersToRows[Rows_[rowId].second].push_back(rowId);
            }

            auto temp = std::make_unique<TRowBuffer>(PoolChunkSize, PoolChunkSize);

            TotalMemorySize_ = 0;
            AllocatedMemorySize_ = 0;
            GarbageMemorySize_ = 0;

            for (size_t bufferId = 0; bufferId < buffersToRows.size(); ++bufferId) {
                auto& buffer = *temp;

                for (auto rowId : buffersToRows[bufferId]) {
                    auto& row = Rows_[rowId].first;
                    
                    auto savedSize = buffer.GetSize();
                    row = buffer.Capture(row);
                    AllocatedMemorySize_ += buffer.GetSize() - savedSize;
                }

                TotalMemorySize_ += buffer.GetCapacity();

                temp.swap(Buffers_[bufferId]);
                temp->Clear();

                if (buffer.GetSize() < BufferLimit) {
                    EmptyBufferIds_.push_back(bufferId);
                }
            }
        } else {
            // Allocate buffer and add to emptyBufferIds.
            EmptyBufferIds_.push_back(Buffers_.size());
            Buffers_.push_back(std::make_unique<TRowBuffer>(PoolChunkSize, PoolChunkSize));
        }
    }

    YCHECK(!EmptyBufferIds_.empty());

    auto bufferId = EmptyBufferIds_.back();
    auto& buffer = *Buffers_[bufferId];

    auto savedSize = buffer.GetSize();
    auto savedCapacity = buffer.GetCapacity();

    auto captured = buffer.Capture(row);

    AllocatedMemorySize_ += buffer.GetSize() - savedSize;
    TotalMemorySize_ += buffer.GetCapacity() - savedCapacity;

    if (buffer.GetSize() >= BufferLimit) {
        EmptyBufferIds_.pop_back();
    }

    return std::make_pair(captured, bufferId);
}

void TTopCollector::AccountGarbage(TRow row)
{
    GarbageMemorySize_ += GetUnversionedRowDataSize(row.GetCount());
    for (int index = 0; index < row.GetCount(); ++index) {
        const auto& value = row[index];

        if (IsStringLikeType(EValueType(value.Type))) {
            GarbageMemorySize_ += value.Length;
        }
    }
}

void TTopCollector::AddRow(TRow row)
{
    if (Rows_.size() < Rows_.capacity()) {
        auto capturedRow = Capture(row);
        Rows_.emplace_back(capturedRow);
        std::push_heap(Rows_.begin(), Rows_.end(), Comparer_);
    } else if (!Comparer_(Rows_.front().first, row)) {
        auto capturedRow = Capture(row);
        std::pop_heap(Rows_.begin(), Rows_.end(), Comparer_);
        AccountGarbage(Rows_.back().first);
        Rows_.back() = capturedRow;
        std::push_heap(Rows_.begin(), Rows_.end(), Comparer_);
    }
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
            subquery->WhereClause = New<TInOpExpression>(
                NullSourceLocation,
                joinKeyExprs,
                keys);

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
