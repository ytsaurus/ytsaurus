#include "evaluation_helpers.h"
#include "column_evaluator.h"
#include "private.h"
#include "helpers.h"
#include "query.h"
#include "query_helpers.h"

#include <yt/client/query_client/query_statistics.h>

#include <yt/client/table_client/schemaful_reader.h>

#include <yt/ytlib/table_client/pipe.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/profiling/timing.h>

namespace NYT {
namespace NQueryClient {

using namespace NConcurrency;
using namespace NTableClient;

static const auto& Logger = QueryClientLogger;

////////////////////////////////////////////////////////////////////////////////

static const i64 BufferLimit = 16 * PoolChunkSize;
static const i64 MaxTopCollectorLimit = 2 * 1024 * 1024;

struct TTopCollectorBufferTag
{ };

////////////////////////////////////////////////////////////////////////////////

TTopCollector::TTopCollector(i64 limit, TComparerFunction* comparer, size_t rowSize)
    : Comparer_(comparer)
    , RowSize_(rowSize)
{
    if (limit > MaxTopCollectorLimit) {
        THROW_ERROR_EXCEPTION("Maximum ORDER BY limit exceeded")
            << TErrorAttribute("limit", limit)
            << TErrorAttribute("max_limit", MaxTopCollectorLimit);
    }

    Rows_.reserve(limit);
}

std::pair<const TValue*, int> TTopCollector::Capture(const TValue* row)
{
    if (EmptyBufferIds_.empty()) {
        if (GarbageMemorySize_ > TotalMemorySize_ / 2) {
            // Collect garbage.

            std::vector<std::vector<size_t>> buffersToRows(Buffers_.size());
            for (size_t rowId = 0; rowId < Rows_.size(); ++rowId) {
                buffersToRows[Rows_[rowId].second].push_back(rowId);
            }

            auto buffer = New<TRowBuffer>(TTopCollectorBufferTag(), PoolChunkSize, MaxSmallBlockRatio);

            TotalMemorySize_ = 0;
            AllocatedMemorySize_ = 0;
            GarbageMemorySize_ = 0;

            for (size_t bufferId = 0; bufferId < buffersToRows.size(); ++bufferId) {
                for (auto rowId : buffersToRows[bufferId]) {
                    auto& row = Rows_[rowId].first;

                    auto savedSize = buffer->GetSize();
                    row = buffer->Capture(row, RowSize_).Begin();
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
            Buffers_.push_back(New<TRowBuffer>(TTopCollectorBufferTag(), PoolChunkSize, MaxSmallBlockRatio));
        }
    }

    YCHECK(!EmptyBufferIds_.empty());

    auto bufferId = EmptyBufferIds_.back();
    auto buffer = Buffers_[bufferId];

    auto savedSize = buffer->GetSize();
    auto savedCapacity = buffer->GetCapacity();

    auto capturedRow = buffer->Capture(row, RowSize_).Begin();

    AllocatedMemorySize_ += buffer->GetSize() - savedSize;
    TotalMemorySize_ += buffer->GetCapacity() - savedCapacity;

    if (buffer->GetSize() >= BufferLimit) {
        EmptyBufferIds_.pop_back();
    }

    return std::make_pair(capturedRow, bufferId);
}

void TTopCollector::AccountGarbage(const TValue* row)
{
    GarbageMemorySize_ += GetUnversionedRowByteSize(RowSize_);
    for (int index = 0; index < RowSize_; ++index) {
        const auto& value = row[index];

        if (IsStringLikeType(EValueType(value.Type))) {
            GarbageMemorySize_ += value.Length;
        }
    }
}

void TTopCollector::AddRow(const TValue* row)
{
    if (Rows_.size() < Rows_.capacity()) {
        auto capturedRow = Capture(row);
        Rows_.emplace_back(capturedRow);
        std::push_heap(Rows_.begin(), Rows_.end(), Comparer_);
    } else if (!Rows_.empty() && !Comparer_(Rows_.front().first, row)) {
        auto capturedRow = Capture(row);
        std::pop_heap(Rows_.begin(), Rows_.end(), Comparer_);
        AccountGarbage(Rows_.back().first);
        Rows_.back() = capturedRow;
        std::push_heap(Rows_.begin(), Rows_.end(), Comparer_);
    }
}

std::vector<const TValue*> TTopCollector::GetRows() const
{
    std::vector<const TValue*> result;
    result.reserve(Rows_.size());
    for (const auto& pair : Rows_) {
        result.push_back(pair.first);
    }
    std::sort(result.begin(), result.end(), Comparer_);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

TJoinClosure::TJoinClosure(
    THasherFunction* lookupHasher,
    TComparerFunction* lookupEqComparer,
    TComparerFunction* prefixEqComparer,
    int keySize,
    int primaryRowSize,
    size_t batchSize)
    : Buffer(New<TRowBuffer>(TPermanentBufferTag(), PoolChunkSize, MaxSmallBlockRatio))
    , Lookup(
        InitialGroupOpHashtableCapacity,
        lookupHasher,
        lookupEqComparer)
    , PrefixEqComparer(prefixEqComparer)
    , KeySize(keySize)
    , PrimaryRowSize(primaryRowSize)
    , BatchSize(batchSize)
{
    Lookup.set_empty_key(nullptr);
}

TMultiJoinClosure::TItem::TItem(
    IMemoryChunkProviderPtr chunkProvider,
    size_t keySize,
    TComparerFunction* prefixEqComparer,
    THasherFunction* lookupHasher,
    TComparerFunction* lookupEqComparer)
    : Buffer(New<TRowBuffer>(TPermanentBufferTag(), std::move(chunkProvider)))
    , KeySize(keySize)
    , PrefixEqComparer(prefixEqComparer)
    , Lookup(
        InitialGroupOpHashtableCapacity,
        lookupHasher,
        lookupEqComparer)
{
    Lookup.set_empty_key(nullptr);
}

TGroupByClosure::TGroupByClosure(
    IMemoryChunkProviderPtr chunkProvider,
    THasherFunction* groupHasher,
    TComparerFunction* groupComparer,
    int keySize,
    bool checkNulls)
    : Buffer(New<TRowBuffer>(TPermanentBufferTag(), std::move(chunkProvider)))
    , Lookup(
        InitialGroupOpHashtableCapacity,
        groupHasher,
        groupComparer)
    , KeySize(keySize)
    , CheckNulls(checkNulls)
{
    Lookup.set_empty_key(nullptr);
}

TWriteOpClosure::TWriteOpClosure()
    : OutputBuffer(New<TRowBuffer>(TOutputBufferTag()))
{
    OutputRowsBatch.reserve(WriteRowsetSize);
}

////////////////////////////////////////////////////////////////////////////////

std::pair<TQueryPtr, TDataRanges> GetForeignQuery(
    TQueryPtr subquery,
    TConstJoinClausePtr joinClause,
    std::vector<TRow> keys,
    TRowBufferPtr permanentBuffer)
{
    auto foreignKeyPrefix = joinClause->ForeignKeyPrefix;
    const auto& foreignEquations = joinClause->ForeignEquations;

    auto newQuery = New<TQuery>(*subquery);

    TDataRanges dataSource;
    dataSource.Id = joinClause->ForeignDataId;

    if (foreignKeyPrefix > 0) {
        if (foreignKeyPrefix == foreignEquations.size()) {
            LOG_DEBUG("Using join via source ranges");
            dataSource.Keys = MakeSharedRange(std::move(keys), std::move(permanentBuffer));
        } else {
            LOG_DEBUG("Using join via prefix ranges");
            std::vector<TRow> prefixKeys;
            for (auto key : keys) {
                prefixKeys.push_back(permanentBuffer->Capture(key.Begin(), foreignKeyPrefix, false));
            }
            prefixKeys.erase(std::unique(prefixKeys.begin(), prefixKeys.end()), prefixKeys.end());
            dataSource.Keys = MakeSharedRange(std::move(prefixKeys), std::move(permanentBuffer));
        }

        for (size_t index = 0; index < foreignKeyPrefix; ++index) {
            dataSource.Schema.push_back(foreignEquations[index]->Type);
        }

        newQuery->InferRanges = false;
        // COMPAT(lukyan): Use ordered read without modification of protocol
        newQuery->Limit = std::numeric_limits<i64>::max() - 1;
    } else {
        TRowRanges ranges;

        LOG_DEBUG("Using join via IN clause");
        ranges.emplace_back(
            permanentBuffer->Capture(NTableClient::MinKey().Get()),
            permanentBuffer->Capture(NTableClient::MaxKey().Get()));

        auto inClause = New<TInExpression>(foreignEquations, MakeSharedRange(std::move(keys), permanentBuffer));

        dataSource.Ranges = MakeSharedRange(std::move(ranges), std::move(permanentBuffer));

        newQuery->WhereClause = newQuery->WhereClause
            ? MakeAndExpression(inClause, newQuery->WhereClause)
            : inClause;
    }

    return std::make_pair(newQuery, dataSource);
}

////////////////////////////////////////////////////////////////////////////////

void* const* TCGVariables::GetOpaqueData() const
{
    return OpaquePointers_.data();
}

void TCGVariables::Clear()
{
    OpaquePointers_.clear();
    OpaqueValues_.clear();
    OwningLiteralValues_.clear();
    LiteralValues_.reset();
}

int TCGVariables::AddLiteralValue(TOwningValue value)
{
    Y_ASSERT(!LiteralValues_);
    int index = static_cast<int>(OwningLiteralValues_.size());
    OwningLiteralValues_.emplace_back(std::move(value));
    return index;
}

TValue* TCGVariables::GetLiteralValues() const
{
    if (!LiteralValues_) {
        LiteralValues_ = std::make_unique<TValue[]>(OwningLiteralValues_.size());
        size_t index = 0;
        for (const auto& value : OwningLiteralValues_) {
            LiteralValues_[index++] = TValue(value);
        }
        return LiteralValues_.get();
    }
    return LiteralValues_.get();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT
