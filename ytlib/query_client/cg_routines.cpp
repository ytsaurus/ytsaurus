#include "cg_routines.h"
#include "cg_types.h"
#include "evaluation_helpers.h"
#include "helpers.h"

#include <yt/client/security_client/acl.h>
#include <yt/client/security_client/helpers.h>

#include <yt/client/query_client/query_statistics.h>

#include <yt/client/table_client/row_buffer.h>
#include <yt/client/table_client/schemaful_reader.h>
#include <yt/client/table_client/unversioned_writer.h>
#include <yt/client/table_client/unversioned_row.h>
#include <yt/client/table_client/helpers.h>

#include <yt/core/yson/lexer.h>
#include <yt/core/yson/parser.h>
#include <yt/core/yson/pull_parser.h>
#include <yt/core/yson/token.h>
#include <yt/core/yson/writer.h>

#include <yt/core/ytree/ypath_resolver.h>
#include <yt/core/ytree/convert.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/misc/chunked_memory_pool_output.h>
#include <yt/core/misc/farm_hash.h>
#include <yt/core/misc/finally.h>
#include <yt/core/misc/hyperloglog.h>

#include <yt/core/profiling/timing.h>

#include <contrib/libs/re2/re2/re2.h>

#include <util/charset/utf8.h>

#include <mutex>

#include <string.h>

namespace NYT::NCodegen {

////////////////////////////////////////////////////////////////////////////////

template <>
struct TTypeBuilder<re2::RE2*>
    : public TTypeBuilder<void*>
{ };

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCodegen

namespace NYT::NQueryClient {
namespace NRoutines {

////////////////////////////////////////////////////////////////////////////////

using namespace NConcurrency;
using namespace NTableClient;
using namespace NProfiling;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

using THLL = NYT::THyperLogLog<14>;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = QueryClientLogger;
static constexpr auto YieldThreshold = TDuration::MilliSeconds(100);

class TYielder
    : public TWallTimer
    , private TContextSwitchGuard
{
public:
    TYielder()
        : TContextSwitchGuard(
            [this] () noexcept { Stop(); },
            [this] () noexcept { Restart(); })
    { }

    void Checkpoint(size_t processedRows)
    {
        if (GetElapsedTime() > YieldThreshold) {
            YT_LOG_DEBUG("Yielding fiber (ProcessedRows: %v, SyncTime: %v)",
                processedRows,
                GetElapsedTime());
            Yield();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

typedef bool (*TRowsConsumer)(void** closure, TExpressionContext*, const TValue** rows, i64 size);

bool WriteRow(TExecutionContext* context, TWriteOpClosure* closure, TValue* values)
{
    CHECK_STACK();

    auto* statistics = context->Statistics;

    if (statistics->RowsWritten >= context->OutputRowLimit) {
        throw TInterruptedIncompleteException();
    }

    ++statistics->RowsWritten;

    auto& batch = closure->OutputRowsBatch;

    const auto& rowBuffer = closure->OutputBuffer;

    YT_ASSERT(batch.size() < batch.capacity());

    batch.push_back(rowBuffer->Capture(values, closure->RowSize));

    // NB: Aggregate flag is neither set from TCG value nor cleared during row allocation.
    size_t id = 0;
    for (auto* value = batch.back().Begin(); value < batch.back().End(); ++value) {
        auto mutableValue = const_cast<TUnversionedValue*>(value);
        mutableValue->Aggregate = false;
        mutableValue->Id = id++;

        if (!IsStringLikeType(value->Type)) {
            mutableValue->Length = 0;
        }
    }

    if (batch.size() == batch.capacity()) {
        auto& writer = context->Writer;
        bool shouldNotWait;
        {
            TValueIncrementingTimingGuard<TFiberWallTimer> timingGuard(&statistics->WriteTime);
            shouldNotWait = writer->Write(batch);
        }

        if (!shouldNotWait) {
            TValueIncrementingTimingGuard<TWallTimer> timingGuard(&statistics->WaitOnReadyEventTime);
            WaitFor(writer->GetReadyEvent())
                .ThrowOnError();
        }
        batch.clear();
        rowBuffer->Clear();
    }

    return false;
}

void ScanOpHelper(
    TExecutionContext* context,
    void** consumeRowsClosure,
    TRowsConsumer consumeRows)
{
    auto finalLogger = Finally([&] () {
        YT_LOG_DEBUG("Finalizing scan helper");
    });

    auto& reader = context->Reader;

    std::vector<TRow> rows;
    rows.reserve(context->Ordered && context->Limit < RowsetProcessingSize
        ? context->Limit
        : RowsetProcessingSize);

    if (rows.capacity() == 0) {
        return;
    }

    std::vector<const TValue*> values;
    values.reserve(rows.capacity());

    auto* statistics = context->Statistics;

    TYielder yielder;

    auto rowBuffer = New<TRowBuffer>(TIntermediateBufferTag());
    bool hasMoreData;
    bool finished = false;
    do {
        {
            TValueIncrementingTimingGuard<TFiberWallTimer> timingGuard(&statistics->ReadTime);
            hasMoreData = reader->Read(&rows);
        }

        if (rows.empty()) {
            if (!hasMoreData) {
                break;
            }
            TValueIncrementingTimingGuard<TWallTimer> timingGuard(&statistics->WaitOnReadyEventTime);
            WaitFor(reader->GetReadyEvent())
                .ThrowOnError();
            continue;
        }

        // Remove null rows.
        rows.erase(
            std::remove_if(rows.begin(), rows.end(), [] (TRow row) {
                return !row;
            }),
            rows.end());

        if (statistics->RowsRead + rows.size() >= context->InputRowLimit) {
            YT_VERIFY(statistics->RowsRead <= context->InputRowLimit);
            rows.resize(context->InputRowLimit - statistics->RowsRead);
            statistics->IncompleteInput = true;
            hasMoreData = false;
        }

        statistics->RowsRead += rows.size();
        for (auto row : rows) {
            statistics->DataWeightRead += GetDataWeight(row);
            values.push_back(row.Begin());
        }

        finished = consumeRows(consumeRowsClosure, rowBuffer.Get(), values.data(), values.size());

        yielder.Checkpoint(statistics->RowsRead);

        rows.clear();
        values.clear();
        rowBuffer->Clear();

        if (!context->IsMerge && rows.capacity() < RowsetProcessingSize) {
            rows.reserve(std::min(2 * rows.capacity(), RowsetProcessingSize));
        }
    } while (hasMoreData && !finished);
}

void InsertJoinRow(
    TExecutionContext* context,
    TJoinClosure* closure,
    TValue** keyPtr,
    TValue* row)
{
    CHECK_STACK();

    TValue* key = *keyPtr;

    i64 chainIndex = closure->ChainedRows.size();

    if (chainIndex >= context->JoinRowLimit) {
        throw TInterruptedIncompleteException();
    }

    closure->ChainedRows.emplace_back(TChainedRow{
        closure->Buffer->Capture(row, closure->PrimaryRowSize).Begin(),
        key,
        -1});

    if (!closure->LastKey || !closure->PrefixEqComparer(key, closure->LastKey)) {
        if (closure->LastKey) {
            size_t rowSize = closure->CommonKeyPrefixDebug;
            YT_ASSERT(CompareRows(closure->LastKey, closure->LastKey + rowSize, key, key + rowSize) <= 0);
        }

        closure->ProcessSegment();
        closure->LastKey = key;
        closure->Lookup.clear();
        // Key will be realloacted further.
    }

    auto inserted = closure->Lookup.insert(std::make_pair(key, std::make_pair(chainIndex, false)));
    if (inserted.second) {
        for (int index = 0; index < closure->KeySize; ++index) {
            closure->Buffer->Capture(&key[index]);
        }

        *keyPtr = closure->Buffer->AllocateUnversioned(closure->KeySize).Begin();
    } else {
        auto& startIndex = inserted.first->second.first;
        closure->ChainedRows.back().Key = inserted.first->first;
        closure->ChainedRows.back().NextRowIndex = startIndex;
        startIndex = chainIndex;
    }

    if (closure->ChainedRows.size() >= closure->BatchSize) {
        closure->ProcessJoinBatch();
        *keyPtr = closure->Buffer->AllocateUnversioned(closure->KeySize).Begin();
    }
}

char* AllocateAlignedBytes(TExpressionContext* buffer, size_t byteCount)
{
    return buffer
        ->GetPool()
        ->AllocateAligned(byteCount);
}

struct TSlot
{
    size_t Offset;
    size_t Count;
};

TValue* AllocateJoinKeys(
    TExecutionContext* context,
    TMultiJoinClosure* closure,
    TValue** keyPtrs)
{
    for (size_t joinId = 0; joinId < closure->Items.size(); ++joinId) {
        auto& item = closure->Items[joinId];
        char* data = AllocateAlignedBytes(
            item.Buffer.Get(),
            GetUnversionedRowByteSize(item.KeySize) + sizeof(TSlot));
        keyPtrs[joinId] = TMutableRow::Create(data, item.KeySize).Begin();
    }

    size_t primaryRowSize = closure->PrimaryRowSize * sizeof(TValue) + sizeof(TSlot*) * closure->Items.size();

    return reinterpret_cast<TValue*>(AllocateAlignedBytes(closure->Buffer.Get(), primaryRowSize));
}

bool StorePrimaryRow(
    TExecutionContext* context,
    TMultiJoinClosure* closure,
    TValue** primaryValues,
    TValue** keysPtr)
{
    if (closure->PrimaryRows.size() >= context->JoinRowLimit) {
        throw TInterruptedIncompleteException();
    }

    closure->PrimaryRows.emplace_back(*primaryValues);

    for (size_t columnIndex = 0; columnIndex < closure->PrimaryRowSize; ++columnIndex) {
        closure->Buffer->Capture(*primaryValues + columnIndex);
    }

    for (size_t joinId = 0; joinId < closure->Items.size(); ++joinId) {
        auto keyPtr = keysPtr + joinId;
        auto& item = closure->Items[joinId];
        TValue* key = *keyPtr;

        if (!item.LastKey || !item.PrefixEqComparer(key, item.LastKey)) {
            closure->ProcessSegment(joinId);
            item.LastKey = key;
            item.Lookup.clear();
            // Key will be reallocated further.
        }

        *reinterpret_cast<TSlot*>(key + item.KeySize) = TSlot{0, 0};

        auto inserted = item.Lookup.insert(key);
        if (inserted.second) {
            for (size_t columnIndex = 0; columnIndex < item.KeySize; ++columnIndex) {
                closure->Items[joinId].Buffer->Capture(&key[columnIndex]);
            }

            char* data = AllocateAlignedBytes(
                item.Buffer.Get(),
                GetUnversionedRowByteSize(item.KeySize) + sizeof(TSlot));
            *keyPtr = TMutableRow::Create(data, item.KeySize).Begin();
        }

        reinterpret_cast<TSlot**>(*primaryValues + closure->PrimaryRowSize)[joinId] = reinterpret_cast<TSlot*>(
            *inserted.first + item.KeySize);
    }

    if (closure->PrimaryRows.size() >= closure->BatchSize) {
        if (closure->ProcessJoinBatch()) {
            return true;
        }

        // Allocate all keys
        for (size_t joinId = 0; joinId < closure->Items.size(); ++joinId) {
            auto& item = closure->Items[joinId];
            char* data = AllocateAlignedBytes(
                item.Buffer.Get(),
                GetUnversionedRowByteSize(item.KeySize) + sizeof(TSlot));
            keysPtr[joinId] = TMutableRow::Create(data, item.KeySize).Begin();
        }
    }

    size_t primaryRowSize = closure->PrimaryRowSize * sizeof(TValue) + sizeof(TSlot*) * closure->Items.size();

    *primaryValues = reinterpret_cast<TValue*>(AllocateAlignedBytes(closure->Buffer.Get(), primaryRowSize));

    return false;
}

class TJoinBatchState
{
public:
    TJoinBatchState(
        void** consumeRowsClosure,
        TRowsConsumer consumeRows,
        const std::vector<size_t>& selfColumns,
        const std::vector<size_t>& foreignColumns)
        : ConsumeRowsClosure(consumeRowsClosure)
        , ConsumeRows(consumeRows)
        , SelfColumns(selfColumns)
        , ForeignColumns(foreignColumns)
    {
        JoinedRows.reserve(RowsetProcessingSize);
    }

    void ConsumeJoinedRows()
    {
        // Consume joined rows.
        ConsumeRows(ConsumeRowsClosure, IntermediateBuffer.Get(), JoinedRows.data(), JoinedRows.size());
        JoinedRows.clear();
        IntermediateBuffer->Clear();
    }

    void JoinRow(const TValue* row, const TValue* foreignRow)
    {
        auto joinedRow = IntermediateBuffer->AllocateUnversioned(SelfColumns.size() + ForeignColumns.size());

        for (size_t column = 0; column < SelfColumns.size(); ++column) {
            joinedRow[column] = row[SelfColumns[column]];
        }

        for (size_t column = 0; column < ForeignColumns.size(); ++column) {
            joinedRow[column + SelfColumns.size()] = foreignRow[ForeignColumns[column]];
        }

        JoinedRows.push_back(joinedRow.Begin());

        if (JoinedRows.size() >= RowsetProcessingSize) {
            ConsumeJoinedRows();
        }
    }

    void JoinRowNull(const TValue* row)
    {
        auto joinedRow = IntermediateBuffer->AllocateUnversioned(SelfColumns.size() + ForeignColumns.size());

        for (size_t column = 0; column < SelfColumns.size(); ++column) {
            joinedRow[column] = row[SelfColumns[column]];
        }

        for (size_t column = 0; column < ForeignColumns.size(); ++column) {
            joinedRow[column + SelfColumns.size()] = MakeUnversionedSentinelValue(EValueType::Null);
        }

        JoinedRows.push_back(joinedRow.Begin());

        if (JoinedRows.size() >= RowsetProcessingSize) {
            ConsumeJoinedRows();
        }
    }

    void JoinRows(const std::vector<TChainedRow>& chainedRows, int startIndex, const TValue* foreignRow)
    {
        for (
            int chainedRowIndex = startIndex;
            chainedRowIndex >= 0;
            chainedRowIndex = chainedRows[chainedRowIndex].NextRowIndex)
        {
            JoinRow(chainedRows[chainedRowIndex].Row, foreignRow);
        }
    }

    void JoinRowsNull(const std::vector<TChainedRow>& chainedRows, int startIndex)
    {
        for (
            int chainedRowIndex = startIndex;
            chainedRowIndex >= 0;
            chainedRowIndex = chainedRows[chainedRowIndex].NextRowIndex)
        {
            JoinRowNull(chainedRows[chainedRowIndex].Row);
        }
    }

    void SortMergeJoin(
        TExecutionContext* context,
        const std::vector<std::pair<const TValue*, int>>& keysToRows,
        const std::vector<TChainedRow>& chainedRows,
        TTernaryComparerFunction* fullTernaryComparer,
        TComparerFunction* foreignPrefixEqComparer,
        TComparerFunction* foreignSuffixLessComparer,
        bool isPartiallySorted,
        const ISchemafulReaderPtr& reader,
        bool isLeft)
    {
        auto foreignRowBuffer = New<TRowBuffer>(TIntermediateBufferTag());
        std::vector<TRow> sortedForeignSequence;
        size_t unsortedOffset = 0;
        TRow lastForeignKey;


        std::vector<TRow> foreignRows;
        foreignRows.reserve(RowsetProcessingSize);

        // Sort-merge join
        auto currentKey = keysToRows.begin();
        auto lastJoined = keysToRows.end();

        auto processSortedForeignSequence = [&] (auto foreignIt, auto endForeignIt) {
            while (foreignIt != endForeignIt && currentKey != keysToRows.end()) {
                int startIndex = currentKey->second;
                int cmpResult = fullTernaryComparer(currentKey->first, foreignIt->Begin());
                if (cmpResult == 0) {
                    JoinRows(chainedRows, startIndex, foreignIt->Begin());
                    ++foreignIt;
                    lastJoined = currentKey;
                } else if (cmpResult < 0) {
                    if (isLeft && lastJoined != currentKey) {
                        JoinRowsNull(chainedRows, startIndex);
                        lastJoined = currentKey;
                    }
                    ++currentKey;
                } else {
                    ++foreignIt;
                }
            }
            ConsumeJoinedRows();
        };

        auto processForeignSequence = [&] (auto foreignIt, auto endForeignIt) {
            while (foreignIt != endForeignIt) {
                if (!lastForeignKey || !foreignPrefixEqComparer(foreignIt->Begin(), lastForeignKey.Begin())) {
                    std::sort(
                        sortedForeignSequence.begin() + unsortedOffset,
                        sortedForeignSequence.end(),
                        [&] (TRow lhs, TRow rhs) {
                            return foreignSuffixLessComparer(lhs.Begin(), rhs.Begin());
                        });
                    unsortedOffset = sortedForeignSequence.size();

                    if (unsortedOffset >= RowsetProcessingSize) {
                        processSortedForeignSequence(
                            sortedForeignSequence.begin(),
                            sortedForeignSequence.end());
                        sortedForeignSequence.clear();
                        foreignRowBuffer->Clear();
                        unsortedOffset = 0;
                    }

                }

                sortedForeignSequence.push_back(foreignRowBuffer->Capture(*foreignIt));
                lastForeignKey = sortedForeignSequence.back();
                ++foreignIt;
            }
        };

        bool hasMoreData = true;
        while (hasMoreData && currentKey != keysToRows.end()) {
            hasMoreData = reader->Read(&foreignRows);

            if (foreignRows.empty()) {
                if (!hasMoreData) {
                    break;
                }
                TValueIncrementingTimingGuard<TWallTimer> timingGuard(&context->Statistics->WaitOnReadyEventTime);
                WaitFor(reader->GetReadyEvent())
                    .ThrowOnError();
                continue;
            }

            if (isPartiallySorted) {
                processForeignSequence(foreignRows.begin(), foreignRows.end());
            } else {
                processSortedForeignSequence(foreignRows.begin(), foreignRows.end());
            }

            foreignRows.clear();
        }

        if (isPartiallySorted) {
            std::sort(
                sortedForeignSequence.begin() + unsortedOffset,
                sortedForeignSequence.end(),
                [&] (TRow lhs, TRow rhs) {
                    return foreignSuffixLessComparer(lhs.Begin(), rhs.Begin());
                });
            processSortedForeignSequence(
                sortedForeignSequence.begin(),
                sortedForeignSequence.end());
            sortedForeignSequence.clear();
            foreignRowBuffer->Clear();
            unsortedOffset = 0;
        }

        if (isLeft) {
            while (currentKey != keysToRows.end()) {
                int startIndex = currentKey->second;
                if (lastJoined != currentKey) {
                    JoinRowsNull(chainedRows, startIndex);
                }
                ++currentKey;
            }
        }

        ConsumeJoinedRows();
    }

    void HashJoin(
        TExecutionContext* context,
        TJoinLookup* joinLookup,
        const std::vector<TChainedRow>& chainedRows,
        const ISchemafulReaderPtr& reader,
        bool isLeft)
    {
        std::vector<TRow> foreignRows;
        foreignRows.reserve(RowsetProcessingSize);

        bool hasMoreData;
        do {
            hasMoreData = reader->Read(&foreignRows);

            if (foreignRows.empty()) {
                if (!hasMoreData) {
                    break;
                }
                TValueIncrementingTimingGuard<TWallTimer> timingGuard(&context->Statistics->WaitOnReadyEventTime);
                WaitFor(reader->GetReadyEvent())
                    .ThrowOnError();
                continue;
            }

            for (auto foreignRow : foreignRows) {
                auto it = joinLookup->find(foreignRow.Begin());

                if (it == joinLookup->end()) {
                    continue;
                }

                int startIndex = it->second.first;
                bool& isJoined = it->second.second;
                JoinRows(chainedRows, startIndex, foreignRow.Begin());
                isJoined = true;
            }

            ConsumeJoinedRows();

            foreignRows.clear();
        } while (hasMoreData);

        if (isLeft) {
            for (auto lookup : *joinLookup) {
                int startIndex = lookup.second.first;
                bool isJoined = lookup.second.second;

                if (isJoined) {
                    continue;
                }

                JoinRowsNull(chainedRows, startIndex);
            }
        }

        ConsumeJoinedRows();
    }

private:
    void** const ConsumeRowsClosure;
    TRowsConsumer const ConsumeRows;

    std::vector<size_t> SelfColumns;
    std::vector<size_t> ForeignColumns;

    const TRowBufferPtr IntermediateBuffer = New<TRowBuffer>(TIntermediateBufferTag());
    std::vector<const TValue*> JoinedRows;
};

void JoinOpHelper(
    TExecutionContext* context,
    TJoinParameters* parameters,
    THasherFunction* lookupHasher,
    TComparerFunction* lookupEqComparer,
    TComparerFunction* sortLessComparer,
    TComparerFunction* prefixEqComparer,
    TComparerFunction* foreignPrefixEqComparer,
    TComparerFunction* foreignSuffixLessComparer,
    TTernaryComparerFunction* fullTernaryComparer,
    THasherFunction* fullHasher,
    TComparerFunction* fullEqComparer,
    int keySize,
    void** collectRowsClosure,
    void (*collectRows)(
        void** closure,
        TJoinClosure* joinClosure,
        TExpressionContext* buffer),
    void** consumeRowsClosure,
    TRowsConsumer consumeRows)
{
    TJoinClosure closure(
        lookupHasher,
        lookupEqComparer,
        prefixEqComparer,
        keySize,
        parameters->PrimaryRowSize,
        parameters->BatchSize);
    closure.CommonKeyPrefixDebug = parameters->CommonKeyPrefixDebug;

    closure.ProcessSegment = [&] () {
        auto offset = closure.KeysToRows.size();
        for (const auto& item : closure.Lookup) {
            closure.KeysToRows.emplace_back(item.first, item.second.first);
        }

        std::sort(closure.KeysToRows.begin() + offset, closure.KeysToRows.end(), [&] (
            const std::pair<const TValue*, int>& lhs,
            const std::pair<const TValue*, int>& rhs)
            {
                return sortLessComparer(lhs.first, rhs.first);
            });
    };

    closure.ProcessJoinBatch = [&] () {
        closure.ProcessSegment();

        auto keysToRows = std::move(closure.KeysToRows);
        auto chainedRows = std::move(closure.ChainedRows);

        YT_LOG_DEBUG("Collected %v join keys from %v rows",
            keysToRows.size(),
            chainedRows.size());

        std::vector<TRow> keys;
        keys.reserve(keysToRows.size());

        for (const auto& item : keysToRows) {
            keys.push_back(TRow(reinterpret_cast<const TUnversionedRowHeader*>(item.first) - 1));
        }

        TJoinBatchState batchState(
            consumeRowsClosure,
            consumeRows,
            parameters->SelfColumns,
            parameters->ForeignColumns);
        YT_ASSERT(std::is_sorted(keys.begin(), keys.end()));

        // Join rowsets.
        // allRows have format (join key... , other columns...)

        auto& joinLookup = closure.Lookup;
        auto isLeft = parameters->IsLeft;

        if (!parameters->IsOrdered) {
            auto reader = parameters->ExecuteForeign(std::move(keys), closure.Buffer);

            YT_LOG_DEBUG("Joining started");

            if (parameters->IsSortMergeJoin) {
                // Sort-merge join
                batchState.SortMergeJoin(
                    context,
                    keysToRows,
                    chainedRows,
                    fullTernaryComparer,
                    foreignPrefixEqComparer,
                    foreignSuffixLessComparer,
                    parameters->IsPartiallySorted,
                    reader,
                    isLeft);
            } else {
                batchState.HashJoin(context, &joinLookup, chainedRows, reader, isLeft);
            }
        } else {
            auto foreignRowsBuffer = New<TRowBuffer>(TIntermediateBufferTag());

            std::vector<TRow> foreignRows;
            foreignRows.reserve(RowsetProcessingSize);

            TJoinLookupRows foreignLookup(
                InitialGroupOpHashtableCapacity,
                fullHasher,
                fullEqComparer);

            {
                auto reader = parameters->ExecuteForeign(std::move(keys), closure.Buffer);

                std::vector<TRow> rows;
                rows.reserve(RowsetProcessingSize);

                bool hasMoreData;
                do {
                    {
                        TValueIncrementingTimingGuard<TFiberWallTimer> timingGuard(&context->Statistics->ReadTime);
                        hasMoreData = reader->Read(&rows);
                    }

                    if (foreignRows.empty()) {
                        if (!hasMoreData) {
                            break;
                        }
                        TValueIncrementingTimingGuard<TWallTimer> timingGuard(&context->Statistics->WaitOnReadyEventTime);
                        WaitFor(reader->GetReadyEvent())
                            .ThrowOnError();
                        continue;
                    }

                    for (auto row : rows) {
                        foreignLookup.insert(foreignRowsBuffer->Capture(row).Begin());
                    }
                    rows.clear();
                } while (hasMoreData);
            }

            YT_LOG_DEBUG("Got %v foreign rows", foreignLookup.size());
            YT_LOG_DEBUG("Joining started");

            for (const auto& item : chainedRows) {
                auto row = item.Row;
                auto key = item.Key;
                auto equalRange = foreignLookup.equal_range(key);
                for (auto it = equalRange.first; it != equalRange.second; ++it) {
                    batchState.JoinRow(row, *it);
                }

                if (isLeft && equalRange.first == equalRange.second) {
                    batchState.JoinRowNull(row);
                }
            }

            batchState.ConsumeJoinedRows();
        }

        YT_LOG_DEBUG("Joining finished");

        closure.Lookup.clear();
        closure.Buffer->Clear();
        closure.LastKey = nullptr;
    };

    try {
        // Collect join ids.
        collectRows(collectRowsClosure, &closure, closure.Buffer.Get());
    } catch (const TInterruptedIncompleteException&) {
        // Set incomplete and continue
        context->Statistics->IncompleteOutput = true;
    }

    closure.ProcessJoinBatch();
}

void MultiJoinOpHelper(
    TExecutionContext* context,
    TMultiJoinParameters* parameters,
    TJoinComparers* comparers,
    void** collectRowsClosure,
    void (*collectRows)(
        void** closure,
        TMultiJoinClosure* joinClosure,
        TExpressionContext* buffer),
    void** consumeRowsClosure,
    TRowsConsumer consumeRows)
{
    auto finalLogger = Finally([&] () {
        YT_LOG_DEBUG("Finalizing multijoin helper");
    });

    TMultiJoinClosure closure;
    closure.Buffer = New<TRowBuffer>(TPermanentBufferTag(), context->MemoryChunkProvider);
    closure.PrimaryRowSize = parameters->PrimaryRowSize;
    closure.BatchSize = parameters->BatchSize;

    for (size_t joinId = 0; joinId < parameters->Items.size(); ++joinId) {
        TMultiJoinClosure::TItem subclosure(
            context->MemoryChunkProvider,
            parameters->Items[joinId].KeySize,
            comparers[joinId].PrefixEqComparer,
            comparers[joinId].SuffixHasher,
            comparers[joinId].SuffixEqComparer);
        closure.Items.push_back(std::move(subclosure));
    }

    closure.ProcessSegment = [&] (size_t joinId) {
        auto& orderedKeys = closure.Items[joinId].OrderedKeys;
        auto& lookup = closure.Items[joinId].Lookup;

        auto offset = orderedKeys.size();
        orderedKeys.insert(orderedKeys.end(), lookup.begin(), lookup.end());
        std::sort(orderedKeys.begin() + offset, orderedKeys.end(), comparers[joinId].SuffixLessComparer);
    };

    closure.ProcessJoinBatch = [&] () {
        YT_LOG_DEBUG("Joining started");

        std::vector<ISchemafulReaderPtr> readers;
        for (size_t joinId = 0; joinId < closure.Items.size(); ++joinId) {
            closure.ProcessSegment(joinId);

            std::vector<TRow> orderedKeys;
            for (TValue* key : closure.Items[joinId].OrderedKeys) {
                // NB: Aggregate flag is neither set from TCG value nor cleared during row allocation.
                size_t id = 0;
                for (auto* value = key; value < key + closure.Items[joinId].KeySize; ++value) {
                    value->Aggregate = false;
                    value->Id = id++;
                }
                orderedKeys.push_back(TRow(reinterpret_cast<const TUnversionedRowHeader*>(key) - 1));
            }

            auto reader = parameters->Items[joinId].ExecuteForeign(
                orderedKeys,
                closure.Items[joinId].Buffer);
            readers.push_back(reader);
            closure.Items[joinId].Lookup.clear();
            closure.Items[joinId].LastKey = nullptr;
        }

        TYielder yielder;

        std::vector<std::vector<TValue*>> sortedForeignSequences;
        for (size_t joinId = 0; joinId < closure.Items.size(); ++joinId) {
            closure.ProcessSegment(joinId);

            auto orderedKeys = std::move(closure.Items[joinId].OrderedKeys);

            YT_LOG_DEBUG("Collected %v join keys",
                orderedKeys.size());

            auto reader = readers[joinId];

            // Join rowsets.
            // allRows have format (join key... , other columns...)

            bool isPartiallySorted = parameters->Items[joinId].IsPartiallySorted;
            size_t keySize = parameters->Items[joinId].KeySize;

            auto foreignSuffixLessComparer = comparers[joinId].ForeignSuffixLessComparer;
            auto foreignPrefixEqComparer = comparers[joinId].ForeignPrefixEqComparer;
            auto fullTernaryComparer = comparers[joinId].FullTernaryComparer;

            std::vector<TValue*> sortedForeignSequence;
            size_t unsortedOffset = 0;
            TValue* lastForeignKey = nullptr;

            std::vector<TRow> foreignRows;
            foreignRows.reserve(RowsetProcessingSize);
            std::vector<TValue*> foreignValues;

            // Sort-merge join
            auto currentKey = orderedKeys.begin();

            auto processSortedForeignSequence = [&] () {
                size_t index = 0;
                while (index != sortedForeignSequence.size() && currentKey != orderedKeys.end()) {
                    int cmpResult = fullTernaryComparer(*currentKey, sortedForeignSequence[index]);
                    if (cmpResult == 0) {
                        TSlot* slot = reinterpret_cast<TSlot*>(*currentKey + keySize);
                        if (slot->Count == 0) {
                            slot->Offset = index;
                        }
                        ++slot->Count;
                        ++index;
                    } else if (cmpResult < 0) {
                        ++currentKey;
                    } else {
                        ++index;
                    }
                }
            };

            auto processForeignSequence = [&] (auto foreignIt, auto endForeignIt) {
                while (foreignIt != endForeignIt) {
                    if (!lastForeignKey || !foreignPrefixEqComparer(*foreignIt, lastForeignKey)) {
                        std::sort(
                            sortedForeignSequence.begin() + unsortedOffset,
                            sortedForeignSequence.end(),
                            foreignSuffixLessComparer);
                        unsortedOffset = sortedForeignSequence.size();
                        lastForeignKey = *foreignIt;
                    }

                    sortedForeignSequence.push_back(*foreignIt);
                    ++foreignIt;
                }
            };

            TDuration sortingForeignTime;

            bool hasMoreData = true;
            while (hasMoreData && currentKey != orderedKeys.end()) {
                {
                    TValueIncrementingTimingGuard<TFiberWallTimer> timingGuard(&context->Statistics->ReadTime);
                    hasMoreData = reader->Read(&foreignRows);
                }

                if (foreignRows.empty()) {
                    if (!hasMoreData) {
                        break;
                    }
                    TValueIncrementingTimingGuard<TWallTimer> timingGuard(&context->Statistics->WaitOnReadyEventTime);
                    WaitFor(reader->GetReadyEvent())
                        .ThrowOnError();
                    continue;
                }

                for (size_t rowIndex = 0; rowIndex < foreignRows.size(); ++rowIndex) {
                    foreignValues.push_back(closure.Buffer->Capture(foreignRows[rowIndex]).Begin());
                }

                {
                    TValueIncrementingTimingGuard<TFiberWallTimer> timingGuard(&sortingForeignTime);
                    if (isPartiallySorted) {
                        processForeignSequence(foreignValues.begin(), foreignValues.end());
                    } else {
                        sortedForeignSequence.insert(
                            sortedForeignSequence.end(),
                            foreignValues.begin(),
                            foreignValues.end());
                    }
                }

                yielder.Checkpoint(sortedForeignSequence.size());

                foreignRows.clear();
                foreignValues.clear();
            }

            {
                TValueIncrementingTimingGuard<TFiberWallTimer> timingGuard(&sortingForeignTime);

                if (isPartiallySorted) {
                    std::sort(
                        sortedForeignSequence.begin() + unsortedOffset,
                        sortedForeignSequence.end(),
                        foreignSuffixLessComparer);
                }

                processSortedForeignSequence();
            }

            sortedForeignSequences.push_back(std::move(sortedForeignSequence));

            YT_LOG_DEBUG("Finished precessing foreign rowset (SortingTime: %v)", sortingForeignTime);
        }

        auto intermediateBuffer = New<TRowBuffer>(TIntermediateBufferTag());
        std::vector<const TValue*> joinedRows;

        size_t processedRows = 0;

        auto consumeJoinedRows = [&] () -> bool {
            // Consume joined rows.
            processedRows += joinedRows.size();
            bool finished = consumeRows(
                consumeRowsClosure,
                intermediateBuffer.Get(),
                joinedRows.data(),
                joinedRows.size());
            joinedRows.clear();
            intermediateBuffer->Clear();
            yielder.Checkpoint(processedRows);
            return finished;
        };

        // TODO: Join first row in place or join all rows in place and immediately consume them?

        size_t resultRowSize = closure.PrimaryRowSize;

        for (size_t joinId = 0; joinId < closure.Items.size(); ++joinId) {
            resultRowSize += parameters->Items[joinId].ForeignColumns.size();
        }

        YT_LOG_DEBUG("Started producing joined rows");

        std::vector<size_t> indexes(closure.Items.size(), 0);

        auto joinRows = [&] (TValue* rowValues) -> bool {
            size_t incrementIndex = 0;
            while (incrementIndex < closure.Items.size()) {
                TMutableRow joinedRow = intermediateBuffer->AllocateUnversioned(resultRowSize);
                std::copy(rowValues, rowValues + closure.PrimaryRowSize, joinedRow.Begin());

                size_t offset = closure.PrimaryRowSize;
                for (size_t joinId = 0; joinId < closure.Items.size(); ++joinId) {
                    TSlot slot = *(reinterpret_cast<TSlot**>(rowValues + closure.PrimaryRowSize)[joinId]);
                    const auto& foreignIndexes = parameters->Items[joinId].ForeignColumns;

                    if (slot.Count != 0) {
                        YT_VERIFY(indexes[joinId] < slot.Count);
                        TValue* foreignRow = sortedForeignSequences[joinId][slot.Offset + indexes[joinId]];

                        if (incrementIndex == joinId) {
                            ++indexes[joinId];
                            if (indexes[joinId] == slot.Count) {
                                indexes[joinId] = 0;
                                ++incrementIndex;
                            } else {
                                incrementIndex = 0;
                            }
                        }

                        for (size_t columnIndex : foreignIndexes) {
                            joinedRow[offset++] = foreignRow[columnIndex];
                        }
                    } else {
                        if (incrementIndex == joinId) {
                            ++incrementIndex;
                        }

                        bool isLeft = parameters->Items[joinId].IsLeft;
                        if (!isLeft) {
                            indexes.assign(closure.Items.size(), 0);
                            return false;
                        }
                        for (size_t count = foreignIndexes.size(); count > 0; --count) {
                            joinedRow[offset++] = MakeUnversionedSentinelValue(EValueType::Null);
                        }
                    }
                }

                joinedRows.push_back(joinedRow.Begin());

                if (joinedRows.size() >= RowsetProcessingSize) {
                    if (consumeJoinedRows()) {
                        return true;
                    }
                }
            }

            return false;
        };

        auto finally = Finally([&] {
            closure.PrimaryRows.clear();
            closure.Buffer->Clear();
            for (auto& joinItem : closure.Items) {
                joinItem.Buffer->Clear();
            }

            YT_LOG_DEBUG("Joining finished");

        });

        for (TValue* rowValues : closure.PrimaryRows) {
            if (joinRows(rowValues)) {
                return true;
            }
        }

        return consumeJoinedRows();
    };

    try {
        // Collect join ids.
        collectRows(collectRowsClosure, &closure, closure.Buffer.Get());
    } catch (const TInterruptedIncompleteException&) {
        // Set incomplete and continue
        context->Statistics->IncompleteOutput = true;
    }

    closure.ProcessJoinBatch();
}

const TValue* InsertGroupRow(
    TExecutionContext* context,
    TGroupByClosure* closure,
    TValue* row)
{
    CHECK_STACK();

    if (closure->LastKey && !closure->PrefixEqComparer(row, closure->LastKey)) {
        closure->ProcessSegment();
    }

    // Any prefix but ordered scan.
    if (context->Ordered && closure->GroupedRowCount >= context->Offset + context->Limit) {
        if (closure->ValuesCount == 0) {
            return nullptr;
        }

        YT_VERIFY(closure->GroupedRowCount == context->Offset + context->Limit);
        auto found = closure->Lookup.find(row);
        return found != closure->Lookup.end() ? *found : nullptr;
    }

    // FIXME: Incorrect in case of grouping by prefix.
    bool limitReached = closure->GroupedRows.size() == context->GroupRowLimit;

    if (limitReached) {
        auto found = closure->Lookup.find(row);

        if (found == closure->Lookup.end()) {
            throw TInterruptedIncompleteException();
        }

        return *found;
    }

    auto inserted = closure->Lookup.insert(row);

    if (inserted.second) {
        closure->LastKey = *inserted.first;

        YT_ASSERT(*inserted.first == row);

        closure->GroupedRows.push_back(row);
        ++closure->GroupedRowCount;
        YT_VERIFY(closure->GroupedRows.size() <= context->GroupRowLimit);

        for (int index = 0; index < closure->KeySize; ++index) {
            closure->Buffer->Capture(&row[index]);
        }

        if (closure->CheckNulls) {
            for (int index = 0; index < closure->KeySize; ++index) {
                if (row[index].Type == EValueType::Null) {
                    THROW_ERROR_EXCEPTION("Null values are forbidden in group key");
                }
            }
        }
    }

    return *inserted.first;
}

void GroupOpHelper(
    TExecutionContext* context,
    TComparerFunction* prefixEqComparer,
    THasherFunction* groupHasher,
    TComparerFunction* groupComparer,
    int keySize,
    int valuesCount,
    bool checkNulls,
    void** collectRowsClosure,
    void (*collectRows)(
        void** closure,
        TGroupByClosure* groupByClosure,
        TExpressionContext* buffer),
    void** boundaryConsumeRowsClosure,
    TRowsConsumer boundaryConsumeRows,
    void** innerConsumeRowsClosure,
    TRowsConsumer innerConsumeRows)
{
    auto finalLogger = Finally([&] () {
        YT_LOG_DEBUG("Finalizing group helper");
    });

    TGroupByClosure closure(
        context->MemoryChunkProvider,
        prefixEqComparer,
        groupHasher,
        groupComparer,
        keySize,
        valuesCount,
        checkNulls);

    TYielder yielder;
    size_t processedRows = 0;

    auto intermediateBuffer = New<TRowBuffer>(TIntermediateBufferTag());

    auto flushGroupedRows = [&] (bool isBoundary, const TValue** begin, const TValue** end) {
        auto finished = false;

        // FIXME: Do not consider offset in totals
        if (context->Ordered && processedRows < context->Offset) {
            size_t skip = std::min<size_t>(context->Offset - processedRows, end - begin);

            processedRows += skip;
            begin += skip;
        }

        while (!finished && begin < end) {
            i64 size = std::min(begin + RowsetProcessingSize, end) - begin;

            processedRows += size;

            if (isBoundary) {
                finished = boundaryConsumeRows(
                    boundaryConsumeRowsClosure,
                    intermediateBuffer.Get(),
                    begin,
                    size);
            } else {
                finished = innerConsumeRows(
                    innerConsumeRowsClosure,
                    intermediateBuffer.Get(),
                    begin,
                    size);
            }

            intermediateBuffer->Clear();
            yielder.Checkpoint(processedRows);
            begin += size;
        }
    };

    bool isBoundarySegment = true;

    // When group key contains full primary key (used with joins) ProcessSegment will be called on each grouped
    // row.
    closure.ProcessSegment = [&] {
        auto& groupedRows = closure.GroupedRows;
        auto& lookup = closure.Lookup;

        if (Y_UNLIKELY(isBoundarySegment)) {
            size_t innerCount = groupedRows.size() - lookup.size();

            flushGroupedRows(false, groupedRows.data(), groupedRows.data() + innerCount);
            flushGroupedRows(true, groupedRows.data() + innerCount, groupedRows.data() + groupedRows.size());

            closure.GroupedRows.clear();
        } else if(Y_UNLIKELY(groupedRows.size() >= RowsetProcessingSize)) {
            flushGroupedRows(false, groupedRows.data(), groupedRows.data() + groupedRows.size());
            closure.GroupedRows.clear();
        }

        lookup.clear();
        isBoundarySegment = false;
    };

    try {
        collectRows(collectRowsClosure, &closure, closure.Buffer.Get());
    } catch (const TInterruptedIncompleteException&) {
        // Set incomplete and continue
        context->Statistics->IncompleteOutput = true;
    }

    isBoundarySegment = true;

    closure.ProcessSegment();

    YT_VERIFY(closure.GroupedRows.empty());

    YT_LOG_DEBUG("Processed %v group rows", processedRows);
}

void GroupTotalsOpHelper(
    TExecutionContext* context,
    void** collectRowsClosure,
    void (*collectRows)(
        void** closure,
        TExpressionContext* buffer))
{
    auto buffer = New<TRowBuffer>(TIntermediateBufferTag());
    collectRows(collectRowsClosure, buffer.Get());
}

void AllocatePermanentRow(TExecutionContext* context, TExpressionContext* buffer, int valueCount, TValue** row)
{
    CHECK_STACK();

    *row = buffer->AllocateUnversioned(valueCount).Begin();
}

void AddRowToCollector(TTopCollector* topCollector, TValue* row)
{
    topCollector->AddRow(row);
}

void OrderOpHelper(
    TExecutionContext* context,
    TComparerFunction* comparer,
    void** collectRowsClosure,
    void (*collectRows)(void** closure, TTopCollector* topCollector),
    void** consumeRowsClosure,
    TRowsConsumer consumeRows,
    size_t rowSize)
{
    auto finalLogger = Finally([&] () {
        YT_LOG_DEBUG("Finalizing order helper");
    });

    auto limit = context->Offset + context->Limit;

    TTopCollector topCollector(limit, comparer, rowSize, context->MemoryChunkProvider);
    collectRows(collectRowsClosure, &topCollector);
    auto rows = topCollector.GetRows();

    auto rowBuffer = New<TRowBuffer>(TIntermediateBufferTag());

    TYielder yielder;
    size_t processedRows = 0;

    for (size_t index = context->Offset; index < rows.size(); index += RowsetProcessingSize) {
        auto size = std::min(RowsetProcessingSize, rows.size() - index);
        processedRows += size;
        bool finished = consumeRows(consumeRowsClosure, rowBuffer.Get(), rows.data() + index, size);
        YT_VERIFY(!finished);
        rowBuffer->Clear();

        yielder.Checkpoint(processedRows);
    }
}

void WriteOpHelper(
    TExecutionContext* context,
    size_t rowSize,
    void** collectRowsClosure,
    void (*collectRows)(void** closure, TWriteOpClosure* writeOpClosure))
{
    TWriteOpClosure closure;
    closure.RowSize = rowSize;

    try {
        collectRows(collectRowsClosure, &closure);
    } catch (const TInterruptedIncompleteException&) {
        // Set incomplete and continue
        context->Statistics->IncompleteOutput = true;
    }

    auto& batch = closure.OutputRowsBatch;
    auto& writer = context->Writer;

    YT_LOG_DEBUG("Flushing writer");
    if (!batch.empty()) {
        bool shouldNotWait;
        {
            TValueIncrementingTimingGuard<TFiberWallTimer> timingGuard(&context->Statistics->WriteTime);
            shouldNotWait = writer->Write(batch);
        }

        if (!shouldNotWait) {
            TValueIncrementingTimingGuard<TWallTimer> timingGuard(&context->Statistics->WaitOnReadyEventTime);
            WaitFor(writer->GetReadyEvent())
                .ThrowOnError();
        }
    }

    YT_LOG_DEBUG("Closing writer");
    {
        TValueIncrementingTimingGuard<TWallTimer> timingGuard(&context->Statistics->WaitOnReadyEventTime);
        WaitFor(context->Writer->Close())
            .ThrowOnError();
    }
}

char* AllocateBytes(TExpressionContext* context, size_t byteCount)
{
    return context
        ->GetPool()
        ->AllocateUnaligned(byteCount);
}

////////////////////////////////////////////////////////////////////////////////

char IsRowInRowset(
    TComparerFunction* comparer,
    THasherFunction* hasher,
    TComparerFunction* eqComparer,
    TValue* values,
    TSharedRange<TRow>* rows,
    std::unique_ptr<TLookupRows>* lookupRows)
{
    if (rows->Size() < 32) {
        auto found = std::lower_bound(rows->Begin(), rows->End(), values, [&] (TRow row, TValue* values) {
            return comparer(const_cast<TValue*>(row.Begin()), values);
        });

        return found != rows->End() && !comparer(values, const_cast<TValue*>(found->Begin()));
    }

    if (!*lookupRows) {
        *lookupRows = std::make_unique<TLookupRows>(rows->Size(), hasher, eqComparer);
        (*lookupRows)->set_empty_key(nullptr);

        for (TRow row: *rows) {
            (*lookupRows)->insert(row.Begin());
        }
    }

    auto found = (*lookupRows)->find(values);
    return found != (*lookupRows)->end();
}

char IsRowInRanges(
    ui32 valuesCount,
    TValue* values,
    TSharedRange<TRowRange>* ranges)
{
    auto found = std::lower_bound(ranges->Begin(), ranges->End(), values, [&] (TRowRange range, TValue* values) {
        ui32 length = std::min(range.second.GetCount(), valuesCount);
        return CompareRows(range.second.Begin(), range.second.Begin() + length, values, values + length) < 0;
    });

    if (found != ranges->End()) {
        ui32 length = std::min(found->first.GetCount(), valuesCount);

        return CompareRows(
            found->first.Begin(),
            found->first.Begin() + length,
            values,
            values + length) <= 0;
    } else {
        return false;
    }
}

const TValue* TransformTuple(
    TComparerFunction* comparer,
    THasherFunction* hasher,
    TComparerFunction* eqComparer,
    TValue* values,
    TSharedRange<TRow>* rows,
    std::unique_ptr<TLookupRows>* lookupRows)
{
    if (rows->Size() < 32) {
        auto found = std::lower_bound(rows->Begin(), rows->End(), values, [&] (TRow row, TValue* values) {
            return comparer(const_cast<TValue*>(row.Begin()), values);
        });

        if (found != rows->End() && !comparer(values, const_cast<TValue*>(found->Begin()))) {
            return const_cast<TValue*>(found->Begin());
        }

        return nullptr;
    }

    if (!*lookupRows) {
        *lookupRows = std::make_unique<TLookupRows>(rows->Size(), hasher, eqComparer);
        (*lookupRows)->set_empty_key(nullptr);

        for (TRow row: *rows) {
            (*lookupRows)->insert(row.Begin());
        }
    }

    auto found = (*lookupRows)->find(values);
    return found != (*lookupRows)->end() ? *found : nullptr;
}

size_t StringHash(
    const char* data,
    ui32 length)
{
    return FarmFingerprint(data, length);
}

// FarmHash and MurmurHash hybrid to hash TRow.
ui64 SimpleHash(const TUnversionedValue* begin, const TUnversionedValue* end)
{
    const ui64 MurmurHashConstant = 0xc6a4a7935bd1e995ULL;

    // Append fingerprint to hash value. Like Murmurhash.
    const auto hash64 = [&] (ui64 data, ui64 value) {
        value ^= FarmFingerprint(data);
        value *= MurmurHashConstant;
        return value;
    };

    // Hash string. Like Murmurhash.
    const auto hash = [&] (const void* voidData, int length, ui64 seed) {
        ui64 result = seed;
        const ui64* ui64Data = reinterpret_cast<const ui64*>(voidData);
        const ui64* ui64End = ui64Data + (length / 8);

        while (ui64Data < ui64End) {
            auto data = *ui64Data++;
            result = hash64(data, result);
        }

        const char* charData = reinterpret_cast<const char*>(ui64Data);

        if (length & 4) {
            result ^= (*reinterpret_cast<const ui32*>(charData) << (length & 3));
            charData += 4;
        }
        if (length & 2) {
            result ^= (*reinterpret_cast<const ui16*>(charData) << (length & 1));
            charData += 2;
        }
        if (length & 1) {
            result ^= *reinterpret_cast<const ui8*>(charData);
        }

        result *= MurmurHashConstant;
        result ^= (result >> 47);
        result *= MurmurHashConstant;
        result ^= (result >> 47);
        return result;
    };

    ui64 result = end - begin;

    for (auto value = begin; value != end; value++) {
        switch(value->Type) {
            case EValueType::Int64:
                result = hash64(value->Data.Int64, result);
                continue;
            case EValueType::Uint64:
                result = hash64(value->Data.Uint64, result);
                continue;
            case EValueType::Boolean:
                result = hash64(value->Data.Boolean, result);
                continue;
            case EValueType::String:
                result = hash(
                    value->Data.String,
                    value->Length,
                    result);
                continue;
            case EValueType::Null:
                result = hash64(0, result);
                continue;

            case EValueType::Double:
            case EValueType::Any:
            case EValueType::Composite:

            case EValueType::Min:
            case EValueType::Max:
            case EValueType::TheBottom:
                break;
        }
        YT_ABORT();
    }

    return result;
}

ui64 FarmHashUint64(ui64 value)
{
    return FarmFingerprint(value);
}

void ThrowException(const char* error)
{
    THROW_ERROR_EXCEPTION("Error while executing UDF")
        << TError(error);
}

void ThrowQueryException(const char* error)
{
    THROW_ERROR_EXCEPTION("Error while executing query")
        << TError(error);
}

re2::RE2* RegexCreate(TUnversionedValue* regexp)
{
    re2::RE2::Options options;
    options.set_log_errors(false);
    auto re2 = std::make_unique<re2::RE2>(re2::StringPiece(regexp->Data.String, regexp->Length), options);
    if (!re2->ok()) {
        THROW_ERROR_EXCEPTION(
            "Error parsing regular expression %Qv",
            TString(regexp->Data.String, regexp->Length))
            << TError(re2->error().c_str());
    }
    return re2.release();
}

void RegexDestroy(re2::RE2* re2)
{
    delete re2;
}

ui8 RegexFullMatch(re2::RE2* re2, TUnversionedValue* string)
{
    YT_VERIFY(string->Type == EValueType::String);

    return re2::RE2::FullMatch(
        re2::StringPiece(string->Data.String, string->Length),
        *re2);
}

ui8 RegexPartialMatch(re2::RE2* re2, TUnversionedValue* string)
{
    YT_VERIFY(string->Type == EValueType::String);

    return re2::RE2::PartialMatch(
        re2::StringPiece(string->Data.String, string->Length),
        *re2);
}

template <typename StringType>
void CopyString(TExpressionContext* context, TUnversionedValue* result, const StringType& str)
{
    char* data = AllocateBytes(context, str.size());
    memcpy(data, str.data(), str.size());
    result->Type = EValueType::String;
    result->Length = str.size();
    result->Data.String = data;
}

template <typename StringType>
void CopyAny(TExpressionContext* context, TUnversionedValue* result, const StringType& str)
{
    char* data = AllocateBytes(context, str.size());
    memcpy(data, str.c_str(), str.size());
    result->Type = EValueType::Any;
    result->Length = str.size();
    result->Data.String = data;
}

void RegexReplaceFirst(
    TExpressionContext* context,
    re2::RE2* re2,
    TUnversionedValue* string,
    TUnversionedValue* rewrite,
    TUnversionedValue* result)
{
    YT_VERIFY(string->Type == EValueType::String);
    YT_VERIFY(rewrite->Type == EValueType::String);

    re2::string str(string->Data.String, string->Length);
    re2::RE2::Replace(
        &str,
        *re2,
        re2::StringPiece(rewrite->Data.String, rewrite->Length));

    CopyString(context, result, str);
}

void RegexReplaceAll(
    TExpressionContext* context,
    re2::RE2* re2,
    TUnversionedValue* string,
    TUnversionedValue* rewrite,
    TUnversionedValue* result)
{
    YT_VERIFY(string->Type == EValueType::String);
    YT_VERIFY(rewrite->Type == EValueType::String);

    re2::string str(string->Data.String, string->Length);
    re2::RE2::GlobalReplace(
        &str,
        *re2,
        re2::StringPiece(rewrite->Data.String, rewrite->Length));

    CopyString(context, result, str);
}

void RegexExtract(
    TExpressionContext* context,
    re2::RE2* re2,
    TUnversionedValue* string,
    TUnversionedValue* rewrite,
    TUnversionedValue* result)
{
    YT_VERIFY(string->Type == EValueType::String);
    YT_VERIFY(rewrite->Type == EValueType::String);

    re2::string str;
    re2::RE2::Extract(
        re2::StringPiece(string->Data.String, string->Length),
        *re2,
        re2::StringPiece(rewrite->Data.String, rewrite->Length),
        &str);

    CopyString(context, result, str);
}

void RegexEscape(
    TExpressionContext* context,
    TUnversionedValue* string,
    TUnversionedValue* result)
{
    auto str = re2::RE2::QuoteMeta(
        re2::StringPiece(string->Data.String, string->Length));

    CopyString(context, result, str);
}

#define DEFINE_YPATH_GET_IMPL2(PREFIX, TYPE, STATEMENT_OK, STATEMENT_FAIL) \
    void PREFIX ## Get ## TYPE( \
        TExpressionContext* context, \
        TUnversionedValue* result, \
        TUnversionedValue* anyValue, \
        TUnversionedValue* ypath) \
    { \
        auto value = NYTree::TryGet ## TYPE( \
            {anyValue->Data.String, anyValue->Length}, \
            {ypath->Data.String, ypath->Length}); \
        if (value) { \
            STATEMENT_OK \
        } else { \
            STATEMENT_FAIL \
        } \
    }

#define DEFINE_YPATH_GET_IMPL(TYPE, STATEMENT_OK) \
    DEFINE_YPATH_GET_IMPL2(Try, TYPE, STATEMENT_OK, \
        result->Type = EValueType::Null;) \
    DEFINE_YPATH_GET_IMPL2(, TYPE, STATEMENT_OK, \
        THROW_ERROR_EXCEPTION("Value of type %Qv is not found at ypath %Qv", \
            #TYPE, TStringBuf{ypath->Data.String, ypath->Length});)

#define DEFINE_YPATH_GET(TYPE) \
    DEFINE_YPATH_GET_IMPL(TYPE, \
        result->Type = EValueType::TYPE; \
        result->Data.TYPE = *value;)

#define DEFINE_YPATH_GET_STRING \
    DEFINE_YPATH_GET_IMPL(String, \
        CopyString(context, result, *value);)

#define DEFINE_YPATH_GET_ANY \
    DEFINE_YPATH_GET_IMPL(Any, \
        CopyAny(context, result, *value);)

DEFINE_YPATH_GET(Int64)
DEFINE_YPATH_GET(Uint64)
DEFINE_YPATH_GET(Double)
DEFINE_YPATH_GET(Boolean)
DEFINE_YPATH_GET_STRING
DEFINE_YPATH_GET_ANY

////////////////////////////////////////////////////////////////////////////////

#define DEFINE_CONVERT_ANY(TYPE, STATEMENT_OK) \
    void AnyTo ## TYPE(TExpressionContext* context, TUnversionedValue* result, TUnversionedValue* anyValue) \
    { \
        if (anyValue->Type == EValueType::Null) { \
            result->Type = EValueType::Null; \
            return; \
        } \
        NYson::TToken token; \
        NYson::GetToken(TStringBuf(anyValue->Data.String, anyValue->Length), &token); \
        if (token.GetType() == NYson::ETokenType::TYPE) { \
            result->Type = EValueType::TYPE; \
            STATEMENT_OK \
        } else { \
            THROW_ERROR_EXCEPTION("Can not convert value %Qv of type Any to %v", \
                TStringBuf(anyValue->Data.String, anyValue->Length), \
                #TYPE); \
        } \
    }

#define DEFINE_CONVERT_ANY_NUMERIC_IMPL(TYPE) \
    void AnyTo ## TYPE(TExpressionContext* context, TUnversionedValue* result, TUnversionedValue* anyValue) \
    { \
        if (anyValue->Type == EValueType::Null) { \
            result->Type = EValueType::Null; \
            return; \
        } \
        NYson::TToken token; \
        NYson::GetToken(TStringBuf(anyValue->Data.String, anyValue->Length), &token); \
        if (token.GetType() == NYson::ETokenType::Int64) { \
            result->Type = EValueType::TYPE; \
            result->Data.TYPE = token.GetInt64Value(); \
        } else if (token.GetType() == NYson::ETokenType::Uint64) { \
            result->Type = EValueType::TYPE; \
            result->Data.TYPE = token.GetUint64Value(); \
        } else if (token.GetType() == NYson::ETokenType::Double) { \
            result->Type = EValueType::TYPE; \
            result->Data.TYPE = token.GetDoubleValue(); \
        } else { \
            THROW_ERROR_EXCEPTION("Can not convert value %Qv of type Any to %v", \
                TStringBuf(anyValue->Data.String, anyValue->Length), \
                #TYPE); \
        } \
    }

DEFINE_CONVERT_ANY_NUMERIC_IMPL(Int64)
DEFINE_CONVERT_ANY_NUMERIC_IMPL(Uint64)
DEFINE_CONVERT_ANY_NUMERIC_IMPL(Double)
DEFINE_CONVERT_ANY(Boolean, result->Data.Boolean = token.GetBooleanValue();)
DEFINE_CONVERT_ANY(String, CopyString(context, result, token.GetStringValue());)

////////////////////////////////////////////////////////////////////////////////

void ThrowCannotCompareTypes(NYson::ETokenType lhsType, NYson::ETokenType rhsType)
{
    THROW_ERROR_EXCEPTION("Cannot compare values of types %Qlv and %Qlv",
        lhsType,
        rhsType);
}

int CompareAny(char* lhsData, i32 lhsLength, char* rhsData, i32 rhsLength)
{
    TStringBuf lhsInput(lhsData, lhsLength);
    TStringBuf rhsInput(rhsData, rhsLength);

    NYson::TStatelessLexer lexer;

    NYson::TToken lhsToken;
    NYson::TToken rhsToken;
    lexer.GetToken(lhsInput, &lhsToken);
    lexer.GetToken(rhsInput, &rhsToken);

    if (lhsToken.GetType() != rhsToken.GetType()) {
        ThrowCannotCompareTypes(lhsToken.GetType(), rhsToken.GetType());
    }

    auto tokenType = lhsToken.GetType();

    switch (tokenType) {
        case NYson::ETokenType::Boolean: {
            auto lhsValue = lhsToken.GetBooleanValue();
            auto rhsValue = rhsToken.GetBooleanValue();
            if (lhsValue < rhsValue) {
                return -1;
            } else if (lhsValue > rhsValue) {
                return +1;
            } else {
                return 0;
            }
            break;
        }
        case NYson::ETokenType::Int64: {
            auto lhsValue = lhsToken.GetInt64Value();
            auto rhsValue = rhsToken.GetInt64Value();
            if (lhsValue < rhsValue) {
                return -1;
            } else if (lhsValue > rhsValue) {
                return +1;
            } else {
                return 0;
            }
            break;
        }
        case NYson::ETokenType::Uint64: {
            auto lhsValue = lhsToken.GetUint64Value();
            auto rhsValue = rhsToken.GetUint64Value();
            if (lhsValue < rhsValue) {
                return -1;
            } else if (lhsValue > rhsValue) {
                return +1;
            } else {
                return 0;
            }
            break;
        }
        case NYson::ETokenType::Double: {
            auto lhsValue = lhsToken.GetDoubleValue();
            auto rhsValue = rhsToken.GetDoubleValue();
            if (lhsValue < rhsValue) {
                return -1;
            } else if (lhsValue > rhsValue) {
                return +1;
            } else {
                return 0;
            }
            break;
        }
        case NYson::ETokenType::String: {
            auto lhsValue = lhsToken.GetStringValue();
            auto rhsValue = rhsToken.GetStringValue();
            if (lhsValue < rhsValue) {
                return -1;
            } else if (lhsValue > rhsValue) {
                return +1;
            } else {
                return 0;
            }
            break;
        }
        default:
            THROW_ERROR_EXCEPTION("Values of type %Qlv are not comparable",
                tokenType);
    }

    YT_ABORT();
}


#define DEFINE_COMPARE_ANY(TYPE, TOKEN_TYPE) \
int CompareAny##TOKEN_TYPE(char* lhsData, i32 lhsLength, TYPE rhsValue) \
{ \
    TStringBuf lhsInput(lhsData, lhsLength); \
    NYson::TStatelessLexer lexer; \
    NYson::TToken lhsToken; \
    lexer.GetToken(lhsInput, &lhsToken); \
    if (lhsToken.GetType() != NYson::ETokenType::TOKEN_TYPE) { \
        ThrowCannotCompareTypes(lhsToken.GetType(), NYson::ETokenType::TOKEN_TYPE); \
    } \
    auto lhsValue = lhsToken.Get##TOKEN_TYPE##Value(); \
    if (lhsValue < rhsValue) { \
        return -1; \
    } else if (lhsValue > rhsValue) { \
        return +1; \
    } else { \
        return 0; \
    } \
}

DEFINE_COMPARE_ANY(bool, Boolean)
DEFINE_COMPARE_ANY(i64, Int64)
DEFINE_COMPARE_ANY(ui64, Uint64)
DEFINE_COMPARE_ANY(double, Double)

int CompareAnyString(char* lhsData, i32 lhsLength, char* rhsData, i32 rhsLength)
{
    TStringBuf lhsInput(lhsData, lhsLength);
    NYson::TStatelessLexer lexer;
    NYson::TToken lhsToken;
    lexer.GetToken(lhsInput, &lhsToken);
    if (lhsToken.GetType() != NYson::ETokenType::String) {
        ThrowCannotCompareTypes(lhsToken.GetType(), NYson::ETokenType::String);
    }
    auto lhsValue = lhsToken.GetStringValue();
    TStringBuf rhsValue(rhsData, rhsLength);
    if (lhsValue < rhsValue) {
        return -1;
    } else if (lhsValue > rhsValue) {
        return +1;
    } else {
        return 0;
    }
}

void ToAny(TExpressionContext* context, TUnversionedValue* result, TUnversionedValue* value)
{
    NTableClient::ToAny(context, result, value);
}

////////////////////////////////////////////////////////////////////////////////

void ToLowerUTF8(TExpressionContext* context, char** result, int* resultLength, char* source, int sourceLength)
{
    auto lowered = ToLowerUTF8(TStringBuf(source, sourceLength));
    *result = AllocateBytes(context, lowered.size());
    for (int i = 0; i < lowered.size(); i++) {
        (*result)[i] = lowered[i];
    }
    *resultLength = lowered.size();
}

TFingerprint GetFarmFingerprint(const TUnversionedValue* begin, const TUnversionedValue* end)
{
    return NYT::NTableClient::GetFarmFingerprint(begin, end);
}

extern "C" void MakeMap(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* args,
    int argCount)
{
    if (argCount % 2 != 0) {
        THROW_ERROR_EXCEPTION("\"make_map\" takes a even number of arguments");
    }

    TString resultYson;
    TStringOutput output(resultYson);
    NYson::TYsonWriter writer(&output);

    writer.OnBeginMap();
    for (int index = 0; index < argCount / 2; ++index) {
        const auto& nameArg = args[index * 2];
        const auto& valueArg = args[index * 2 + 1];

        if (nameArg.Type != EValueType::String) {
            THROW_ERROR_EXCEPTION("Invalid type of key in key-value pair #%v: expected %Qlv, got %Qlv",
                index,
                EValueType::String,
                nameArg.Type);
        }
        writer.OnKeyedItem(TStringBuf(nameArg.Data.String, nameArg.Length));

        switch (valueArg.Type) {
            case EValueType::Int64:
                writer.OnInt64Scalar(valueArg.Data.Int64);
                continue;
            case EValueType::Uint64:
                writer.OnUint64Scalar(valueArg.Data.Uint64);
                continue;
            case EValueType::Double:
                writer.OnDoubleScalar(valueArg.Data.Double);
                continue;
            case EValueType::Boolean:
                writer.OnBooleanScalar(valueArg.Data.Boolean);
                continue;
            case EValueType::String:
                writer.OnStringScalar(TStringBuf(valueArg.Data.String, valueArg.Length));
                continue;
            case EValueType::Any:
                writer.OnRaw(TStringBuf(valueArg.Data.String, valueArg.Length));
                continue;
            case EValueType::Null:
                writer.OnEntity();
                continue;

            case EValueType::Composite:

            case EValueType::Min:
            case EValueType::Max:
            case EValueType::TheBottom:
                break;
        }
        THROW_ERROR_EXCEPTION("Unexpected type %Qlv of value in key-value pair #%v",
            valueArg.Type,
            index);
    }
    writer.OnEndMap();

    *result = context->Capture(MakeUnversionedAnyValue(resultYson));
}

////////////////////////////////////////////////////////////////////////////////

template <typename TElement, typename TValue>
bool ListContainsImpl(const NYTree::INodePtr& node, const TValue& value)
{
    const auto valueType = NYTree::NDetail::TScalarTypeTraits<TValue>::NodeType;
    for (const auto& element : node->AsList()->GetChildren()) {
        if (element->GetType() == valueType && NYTree::ConvertTo<TElement>(element) == value) {
            return true;
        }
    }
    return false;
}

void ListContains(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* ysonList,
    TUnversionedValue* what)
{
    const auto node = NYTree::ConvertToNode(NYson::TYsonString(ysonList->Data.String, ysonList->Length));

    bool found;
    switch (what->Type) {
        case EValueType::String:
            found = ListContainsImpl<TString>(node, TString(what->Data.String, what->Length));
            break;
        case EValueType::Int64:
            found = ListContainsImpl<i64>(node, what->Data.Int64);
            break;
        case EValueType::Uint64:
            found = ListContainsImpl<ui64>(node, what->Data.Uint64);
            break;
        case EValueType::Boolean:
            found = ListContainsImpl<bool>(node, what->Data.Boolean);
            break;
        case EValueType::Double:
            found = ListContainsImpl<double>(node, what->Data.Double);
            break;
        default:
            THROW_ERROR_EXCEPTION("ListContains() is not implemented for type %v",
                ToString(what->Type));
    }

    *result = MakeUnversionedBooleanValue(found);
}

////////////////////////////////////////////////////////////////////////////////

void AnyToYsonString(
    TExpressionContext* context,
    char** result,
    int* resultLength,
    char* any,
    int anyLength)
{
    YT_VERIFY(anyLength >= 0);
    auto textYsonLengthEstimate = static_cast<size_t>(anyLength) * 3;
    TChunkedMemoryPoolOutput output(context->GetPool(), textYsonLengthEstimate);
    {
        TYsonWriter writer(&output, EYsonFormat::Text);
        TMemoryInput input(any, anyLength);
        TYsonPullParser parser(&input, EYsonType::Node);
        TYsonPullParserCursor cursor(&parser);
        cursor.TransferComplexValue(&writer);
    }
    auto refs = output.FinishAndGetRefs();
    if (refs.size() == 1) {
        *result = refs.front().Begin();
        *resultLength = refs.front().Size();
    } else {
        *resultLength = GetByteSize(refs);
        *result = AllocateBytes(context, *resultLength);
        size_t offset = 0;
        for (const auto& ref : refs) {
            ::memcpy(*result + offset, ref.Begin(), ref.Size());
            offset += ref.Size();
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void HyperLogLogAllocate(TExpressionContext* context, TUnversionedValue* result)
{
    auto hll = AllocateBytes(context, sizeof(THLL));
    new (hll) THLL();

    result->Type = EValueType::String;
    result->Length = sizeof(THLL);
    result->Data.String = hll;
}

void HyperLogLogAdd(void* hll, uint64_t value)
{
    static_cast<THLL*>(hll)->Add(value);
}

void HyperLogLogMerge(void* hll1, void* hll2)
{
    static_cast<THLL*>(hll1)->Merge(*static_cast<THLL*>(hll2));
}

ui64 HyperLogLogEstimateCardinality(void* hll)
{
    return static_cast<THLL*>(hll)->EstimateCardinality();
}

////////////////////////////////////////////////////////////////////////////////

void HasPermissions(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* ysonAcl,
    TUnversionedValue* ysonSubjectClosureList,
    TUnversionedValue* ysonPermissionList)
{
    using namespace NYTree;
    using namespace NYson;

    auto acl = ConvertTo<NSecurityClient::TSerializableAccessControlList>(
        TYsonString(ysonAcl->Data.String, ysonAcl->Length));
    auto subjectClosure = ConvertTo<THashSet<TString>>(
        TYsonString(ysonSubjectClosureList->Data.String, ysonSubjectClosureList->Length));
    auto permissions = ConvertTo<EPermissionSet>(
        TYsonString(ysonPermissionList->Data.String, ysonPermissionList->Length));

    auto action = CheckPermissionsByAclAndSubjectClosure(acl, subjectClosure, permissions);
    *result = MakeUnversionedBooleanValue(action == NSecurityClient::ESecurityAction::Allow);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoutines

////////////////////////////////////////////////////////////////////////////////

using NCodegen::TRoutineRegistry;

void RegisterQueryRoutinesImpl(TRoutineRegistry* registry)
{
#define REGISTER_ROUTINE(routine) \
    registry->RegisterRoutine(#routine, NRoutines::routine)
#define REGISTER_YPATH_GET_ROUTINE(TYPE) \
    REGISTER_ROUTINE(TryGet ## TYPE); \
    REGISTER_ROUTINE(Get ## TYPE)

    REGISTER_ROUTINE(WriteRow);
    REGISTER_ROUTINE(InsertGroupRow);
    REGISTER_ROUTINE(ScanOpHelper);
    REGISTER_ROUTINE(WriteOpHelper);
    REGISTER_ROUTINE(InsertJoinRow);
    REGISTER_ROUTINE(JoinOpHelper);
    REGISTER_ROUTINE(AllocateJoinKeys);
    REGISTER_ROUTINE(AllocateAlignedBytes);
    REGISTER_ROUTINE(StorePrimaryRow);
    REGISTER_ROUTINE(MultiJoinOpHelper);
    REGISTER_ROUTINE(GroupOpHelper);
    REGISTER_ROUTINE(GroupTotalsOpHelper);
    REGISTER_ROUTINE(StringHash);
    REGISTER_ROUTINE(AllocatePermanentRow);
    REGISTER_ROUTINE(AllocateBytes);
    REGISTER_ROUTINE(IsRowInRowset);
    REGISTER_ROUTINE(IsRowInRanges);
    REGISTER_ROUTINE(TransformTuple);
    REGISTER_ROUTINE(SimpleHash);
    REGISTER_ROUTINE(FarmHashUint64);
    REGISTER_ROUTINE(AddRowToCollector);
    REGISTER_ROUTINE(OrderOpHelper);
    REGISTER_ROUTINE(ThrowException);
    REGISTER_ROUTINE(ThrowQueryException);
    REGISTER_ROUTINE(RegexCreate);
    REGISTER_ROUTINE(RegexDestroy);
    REGISTER_ROUTINE(RegexFullMatch);
    REGISTER_ROUTINE(RegexPartialMatch);
    REGISTER_ROUTINE(RegexReplaceFirst);
    REGISTER_ROUTINE(RegexReplaceAll);
    REGISTER_ROUTINE(RegexExtract);
    REGISTER_ROUTINE(RegexEscape);
    REGISTER_ROUTINE(ToLowerUTF8);
    REGISTER_ROUTINE(GetFarmFingerprint);
    REGISTER_ROUTINE(CompareAny);
    REGISTER_ROUTINE(CompareAnyBoolean);
    REGISTER_ROUTINE(CompareAnyInt64);
    REGISTER_ROUTINE(CompareAnyUint64);
    REGISTER_ROUTINE(CompareAnyDouble);
    REGISTER_ROUTINE(CompareAnyString);
    REGISTER_ROUTINE(ToAny);
    REGISTER_YPATH_GET_ROUTINE(Int64);
    REGISTER_YPATH_GET_ROUTINE(Uint64);
    REGISTER_YPATH_GET_ROUTINE(Double);
    REGISTER_YPATH_GET_ROUTINE(Boolean);
    REGISTER_YPATH_GET_ROUTINE(String);
    REGISTER_YPATH_GET_ROUTINE(Any);
    REGISTER_ROUTINE(AnyToInt64);
    REGISTER_ROUTINE(AnyToUint64);
    REGISTER_ROUTINE(AnyToDouble);
    REGISTER_ROUTINE(AnyToBoolean);
    REGISTER_ROUTINE(AnyToString);
    REGISTER_ROUTINE(ListContains);
    REGISTER_ROUTINE(AnyToYsonString);
    REGISTER_ROUTINE(HyperLogLogAllocate);
    REGISTER_ROUTINE(HyperLogLogAdd);
    REGISTER_ROUTINE(HyperLogLogMerge);
    REGISTER_ROUTINE(HyperLogLogEstimateCardinality);
    REGISTER_ROUTINE(HasPermissions);
#undef REGISTER_TRY_GET_ROUTINE
#undef REGISTER_ROUTINE

    registry->RegisterRoutine("memcmp", std::memcmp);
}

TRoutineRegistry* GetQueryRoutineRegistry()
{
    static TRoutineRegistry registry;
    static std::once_flag onceFlag;
    std::call_once(onceFlag, &RegisterQueryRoutinesImpl, &registry);
    return &registry;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
