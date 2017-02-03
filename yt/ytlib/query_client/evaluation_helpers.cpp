#include "evaluation_helpers.h"
#include "column_evaluator.h"
#include "private.h"
#include "helpers.h"
#include "query.h"
#include "query_helpers.h"
#include "query_statistics.h"

#include <yt/ytlib/table_client/pipe.h>
#include <yt/ytlib/table_client/schemaful_reader.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/profiling/scoped_timer.h>

namespace NYT {
namespace NQueryClient {

using namespace NConcurrency;
using namespace NTableClient;

static const auto& Logger = QueryClientLogger;

////////////////////////////////////////////////////////////////////////////////

static const i64 PoolChunkSize = 32 * 1024;
static const i64 BufferLimit = 32 * PoolChunkSize;

struct TTopCollectorBufferTag
{ };

////////////////////////////////////////////////////////////////////////////////

TTopCollector::TTopCollector(i64 limit, TComparerFunction* comparer)
    : Comparer_(comparer)
{
    Rows_.reserve(limit);
}

std::pair<TMutableRow, int> TTopCollector::Capture(TRow row)
{
    if (EmptyBufferIds_.empty()) {
        if (GarbageMemorySize_ > TotalMemorySize_ / 2) {
            // Collect garbage.

            std::vector<std::vector<size_t>> buffersToRows(Buffers_.size());
            for (size_t rowId = 0; rowId < Rows_.size(); ++rowId) {
                buffersToRows[Rows_[rowId].second].push_back(rowId);
            }

            auto buffer = New<TRowBuffer>(TTopCollectorBufferTag(), PoolChunkSize);

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

                if (buffer->GetSize() < BufferLimit) {
                    EmptyBufferIds_.push_back(bufferId);
                }

                std::swap(buffer, Buffers_[bufferId]);
                buffer->Clear();
            }
        } else {
            // Allocate buffer and add to emptyBufferIds.
            EmptyBufferIds_.push_back(Buffers_.size());
            Buffers_.push_back(New<TRowBuffer>(TTopCollectorBufferTag(), PoolChunkSize));
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
    GarbageMemorySize_ += GetUnversionedRowByteSize(row.GetCount());
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

std::vector<TMutableRow> TTopCollector::GetRows(int rowSize) const
{
    std::vector<TMutableRow> result;
    result.reserve(Rows_.size());
    for (const auto& pair : Rows_) {
        result.push_back(pair.first);
    }
    std::sort(result.begin(), result.end(), Comparer_);
    for (auto& row : result) {
        row.SetCount(rowSize);
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

TJoinClosure::TJoinClosure(
    THasherFunction* lookupHasher,
    TComparerFunction* lookupEqComparer,
    int keySize,
    size_t batchSize)
    : Buffer(New<TRowBuffer>(TPermanentBufferTag()))
    , Lookup(
        InitialGroupOpHashtableCapacity,
        lookupHasher,
        lookupEqComparer)
    , KeySize(keySize)
    , BatchSize(batchSize)
{
    Lookup.set_empty_key(TRow());
}

TGroupByClosure::TGroupByClosure(
    THasherFunction* groupHasher,
    TComparerFunction* groupComparer,
    int keySize,
    bool checkNulls)
    : Buffer(New<TRowBuffer>(TPermanentBufferTag()))
    , Lookup(
        InitialGroupOpHashtableCapacity,
        groupHasher,
        groupComparer)
    , KeySize(keySize)
    , CheckNulls(checkNulls)
{
    Lookup.set_empty_key(TRow());
}

TWriteOpClosure::TWriteOpClosure()
    : OutputBuffer(New<TRowBuffer>(TOutputBufferTag()))
{
    OutputRowsBatch.reserve(WriteRowsetSize);
}

////////////////////////////////////////////////////////////////////////////////

TJoinParameters GetJoinEvaluator(
    const TJoinClause& joinClause,
    TConstExpressionPtr foreignPredicate,
    const TTableSchema& selfTableSchema,
    i64 inputRowLimit,
    i64 outputRowLimit,
    size_t batchSize,
    bool isOrdered)
{
    const auto& foreignEquations = joinClause.ForeignEquations;
    auto isLeft = joinClause.IsLeft;
    auto canUseSourceRanges = joinClause.CanUseSourceRanges;
    auto keyPrefix = joinClause.SelfEquations.size();
    auto& foreignDataId = joinClause.ForeignDataId;

    // Create subquery TQuery{ForeignDataSplit, foreign predicate and (join columns) in (keys)}.
    auto subquery = New<TQuery>(inputRowLimit, outputRowLimit);

    subquery->OriginalSchema = joinClause.OriginalSchema;
    subquery->SchemaMapping = joinClause.SchemaMapping;

    // (join key... , other columns...)
    auto projectClause = New<TProjectClause>();
    std::vector<TConstExpressionPtr> joinKeyExprs;

    for (const auto& column : foreignEquations) {
        projectClause->AddProjection(column, InferName(column));
        joinKeyExprs.push_back(column);
    }

    subquery->ProjectClause = projectClause;

    auto selfColumnNames = joinClause.SelfJoinedColumns;
    std::sort(selfColumnNames.begin(), selfColumnNames.end());

    const auto& selfTableColumns = selfTableSchema.Columns();

    std::vector<size_t> selfColumns;
    for (size_t index = 0; index < selfTableColumns.size(); ++index) {
        if (std::binary_search(
            selfColumnNames.begin(),
            selfColumnNames.end(),
            selfTableColumns[index].Name))
        {
            selfColumns.push_back(index);
        }
    }

    auto joinRenamedTableColumns = joinClause.GetRenamedSchema().Columns();

    auto foreignColumnNames = joinClause.ForeignJoinedColumns;
    std::sort(foreignColumnNames.begin(), foreignColumnNames.end());

    std::vector<size_t> foreignColumns;
    for (size_t index = 0; index < joinRenamedTableColumns.size(); ++index) {
        if (std::binary_search(
            foreignColumnNames.begin(),
            foreignColumnNames.end(),
            joinRenamedTableColumns[index].Name))
        {
            foreignColumns.push_back(projectClause->Projections.size());

            projectClause->AddProjection(
                New<TReferenceExpression>(
                    joinRenamedTableColumns[index].Type,
                    joinRenamedTableColumns[index].Name),
                joinRenamedTableColumns[index].Name);
        }
    };

    auto getForeignQuery = [subquery, canUseSourceRanges, keyPrefix, joinKeyExprs, foreignDataId, foreignPredicate] (
        std::vector<TRow> keys,
        TRowBufferPtr permanentBuffer)
    {
        // TODO: keys should be joined with allRows: [(key, sourceRow)]
        TRowRanges ranges;

        if (canUseSourceRanges) {
            LOG_DEBUG("Using join via source ranges");
            for (auto key : keys) {
                auto lowerBound = key;

                auto upperBound = permanentBuffer->Allocate(keyPrefix + 1);
                for (int column = 0; column < keyPrefix; ++column) {
                    upperBound[column] = lowerBound[column];
                }

                upperBound[keyPrefix] = MakeUnversionedSentinelValue(EValueType::Max);
                ranges.emplace_back(lowerBound, upperBound);
            }
            subquery->WhereClause = foreignPredicate;
        } else {
            LOG_DEBUG("Using join via IN clause");
            ranges.emplace_back(
                permanentBuffer->Capture(NTableClient::MinKey().Get()),
                permanentBuffer->Capture(NTableClient::MaxKey().Get()));

            auto inClause = New<TInOpExpression>(joinKeyExprs, MakeSharedRange(std::move(keys), permanentBuffer));

            subquery->WhereClause = foreignPredicate
                ? MakeAndExpression(inClause, foreignPredicate)
                : inClause;
        }

        LOG_DEBUG("Executing subquery");

        TDataRanges dataSource;
        dataSource.Id = foreignDataId;
        dataSource.Ranges = MakeSharedRange(std::move(ranges), std::move(permanentBuffer));

        return std::make_pair(subquery, dataSource);
    };

    return TJoinParameters{isOrdered, isLeft, selfColumns, foreignColumns, getForeignQuery, batchSize};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
