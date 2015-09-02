#include "stdafx.h"
#include "evaluation_helpers.h"

#include "private.h"
#include "plan_helpers.h"
#include "plan_fragment.h"
#include "helpers.h"
#include "query_statistics.h"

#include <core/concurrency/scheduler.h>

#include <core/profiling/scoped_timer.h>

namespace NYT {
namespace NQueryClient {

using namespace NConcurrency;
using NTableClient::GetUnversionedRowDataSize;

static const auto& Logger = QueryClientLogger;

////////////////////////////////////////////////////////////////////////////////

const i64 PoolChunkSize = 32 * 1024;
const i64 BufferLimit = 32 * PoolChunkSize;

TTopCollector::TTopCollector(i64 limit, TComparerFunction* comparer)
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

            auto buffer = New<TRowBuffer>(PoolChunkSize, PoolChunkSize);

            TotalMemorySize_ = 0;
            AllocatedMemorySize_ = 0;
            GarbageMemorySize_ = 0;

            for (size_t bufferId = 0; bufferId < buffersToRows.size(); ++bufferId) {
                for (auto rowId : buffersToRows[bufferId]) {
                    auto& row = Rows_[rowId].first;
                    
                    auto savedSize = buffer->GetSize();
                    row = buffer->Capture(row);
                    AllocatedMemorySize_ += buffer->GetSize() - savedSize;
                }

                TotalMemorySize_ += buffer->GetCapacity();

                std::swap(buffer, Buffers_[bufferId]);
                buffer->Clear();

                if (buffer->GetSize() < BufferLimit) {
                    EmptyBufferIds_.push_back(bufferId);
                }
            }
        } else {
            // Allocate buffer and add to emptyBufferIds.
            EmptyBufferIds_.push_back(Buffers_.size());
            Buffers_.push_back(New<TRowBuffer>(PoolChunkSize, PoolChunkSize));
        }
    }

    YCHECK(!EmptyBufferIds_.empty());

    auto bufferId = EmptyBufferIds_.back();
    auto buffer = Buffers_[bufferId];

    auto savedSize = buffer->GetSize();
    auto savedCapacity = buffer->GetCapacity();

    auto capturedRow = buffer->Capture(row);

    AllocatedMemorySize_ += buffer->GetSize() - savedSize;
    TotalMemorySize_ += buffer->GetCapacity() - savedCapacity;

    if (buffer->GetSize() >= BufferLimit) {
        EmptyBufferIds_.pop_back();
    }

    return std::make_pair(capturedRow, bufferId);
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

bool UpdateAndCheckRowLimit(i64* limit, char* flag)
{
    if (*limit > 0) {
        --(*limit);
        return true;
    } else {
        *flag = true;
        return false;
    }
}

TJoinEvaluator GetJoinEvaluator(
    const TJoinClause& joinClause,
    TConstExpressionPtr predicate,
    const TTableSchema& selfTableSchema)
{
    const auto& equations = joinClause.Equations;
    auto& foreignTableSchema = joinClause.ForeignTableSchema;
    auto& foreignKeyColumnsCount = joinClause.ForeignKeyColumnsCount;
    auto& renamedTableSchema = joinClause.RenamedTableSchema;
    auto& joinedTableSchema = joinClause.JoinedTableSchema;
    auto& foreignDataId = joinClause.ForeignDataId;
    auto foreignPredicate = ExtractPredicateForColumnSubset(predicate, renamedTableSchema);

    // Create subquery TQuery{ForeignDataSplit, foreign predicate and (join columns) in (keys)}.
    auto subquery = New<TQuery>(std::numeric_limits<i64>::max(), std::numeric_limits<i64>::max());

    subquery->TableSchema = foreignTableSchema;
    subquery->KeyColumnsCount = foreignKeyColumnsCount;
    subquery->RenamedTableSchema = renamedTableSchema;

    // (join key... , other columns...)
    auto projectClause = New<TProjectClause>();
    std::vector<TConstExpressionPtr> joinKeyExprs;
    for (const auto& column : equations) {
        projectClause->AddProjection(column.second, InferName(column.second));
        joinKeyExprs.push_back(column.second);
    }

    for (const auto& column : renamedTableSchema.Columns()) {
        projectClause->AddProjection(New<TReferenceExpression>(
            column.Type,
            column.Name),
            column.Name);
    }

    subquery->ProjectClause = projectClause;

    auto subqueryTableSchema = subquery->GetTableSchema();

    auto joinKeySize = equations.size();

    std::vector<std::pair<bool, int>> columnMapping;
    for (const auto& column : joinedTableSchema.Columns()) {
        if (auto self = selfTableSchema.FindColumn(column.Name)) {
            columnMapping.emplace_back(true, selfTableSchema.GetColumnIndex(*self));
        } else if (auto foreign = subqueryTableSchema.FindColumn(column.Name)) {
            columnMapping.emplace_back(false, subqueryTableSchema.GetColumnIndex(*foreign));
        } else {
            YUNREACHABLE();
        }        
    }

    return [=] (
        TExecutionContext* context,
        THasherFunction* groupHasher,
        TComparerFunction* groupComparer,
        const std::vector<TRow>& keys,
        const std::vector<TRow>& allRows,
        std::vector<TRow>* joinedRows)
    {
        // TODO: keys should be joined with allRows: [(key, sourceRow)]

        subquery->WhereClause = New<TInOpExpression>(
            joinKeyExprs,
            // TODO(babenko): fixme
            TSharedRange<TRow>(MakeRange(keys), nullptr));

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
            NProfiling::TAggregatingTimingGuard timingGuard(&context->Statistics->AsyncTime);

            auto statistics = context->ExecuteCallback(subquery, foreignDataId,  writer);
            LOG_DEBUG("Remote subquery statistics %v", statistics);
            *context->Statistics += statistics;

            rowset = WaitFor(rowsetFuture).ValueOrThrow();
        }

        const auto& foreignRows = rowset->GetRows();

        LOG_DEBUG("Got %v foreign rows", foreignRows.size());

        TJoinLookupRows foreignLookup(
            InitialGroupOpHashtableCapacity,
            groupHasher,
            groupComparer);

        for (auto row : foreignRows) {
            foreignLookup.insert(row);
        }

        // Join rowsets.
        TRowBuilder rowBuilder;
        // allRows have format (join key... , other columns...)
        for (auto row : allRows) {
            auto equalRange = foreignLookup.equal_range(row);
            for (auto it = equalRange.first; it != equalRange.second; ++it) {
                rowBuilder.Reset();
                auto foreignRow = *it;
                for (auto columnIndex : columnMapping) {
                    rowBuilder.AddValue(columnIndex.first ? row[joinKeySize + columnIndex.second] : foreignRow[columnIndex.second]);
                }

                if (!UpdateAndCheckRowLimit(&context->JoinRowLimit, &context->StopFlag)) {
                    context->Statistics->IncompleteOutput = true;
                    return;
                }

                joinedRows->push_back(context->PermanentBuffer->Capture(rowBuilder.GetRow()));
            }
        }
    };
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
