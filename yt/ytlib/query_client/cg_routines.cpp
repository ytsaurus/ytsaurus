#include "cg_routines.h"
#include "callbacks.h"
#include "cg_types.h"
#include "evaluation_helpers.h"
#include "helpers.h"
#include "query_statistics.h"

#include <yt/ytlib/chunk_client/chunk_spec.h>

#include <yt/ytlib/table_client/row_buffer.h>
#include <yt/ytlib/table_client/schemaful_reader.h>
#include <yt/ytlib/table_client/schemaful_writer.h>
#include <yt/ytlib/table_client/unordered_schemaful_reader.h>
#include <yt/ytlib/table_client/unversioned_row.h>
#include <yt/ytlib/table_client/pipe.h>

#include <yt/core/yson/lexer.h>
#include <yt/core/yson/parser.h>
#include <yt/core/yson/writer.h>
#include <yt/core/yson/token.h>

#include <yt/core/ytree/ypath_resolver.h>
#include <yt/core/ytree/convert.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/misc/farm_hash.h>

#include <yt/core/profiling/timing.h>

#include <contrib/libs/re2/re2/re2.h>

#include <util/charset/utf8.h>

#include <mutex>

#include <string.h>


namespace llvm {

template <bool Cross>
class TypeBuilder<re2::RE2*, Cross>
    : public TypeBuilder<void*, Cross>
{ };

} // namespace llvm

////////////////////////////////////////////////////////////////////////////////

namespace NYT {
namespace NQueryClient {
namespace NRoutines {

using namespace NConcurrency;
using namespace NTableClient;

static const auto& Logger = QueryClientLogger;

////////////////////////////////////////////////////////////////////////////////

void WriteRow(TExecutionContext* context, TWriteOpClosure* closure, TValue* values)
{
    CHECK_STACK();

    auto* statistics = context->Statistics;

    if (context->IsOrdered && statistics->RowsWritten >= context->Limit) {
        throw TInterruptedCompleteException();
    }

    if (statistics->RowsWritten >= context->OutputRowLimit) {
        throw TInterruptedIncompleteException();
    }

    ++statistics->RowsWritten;

    auto& batch = closure->OutputRowsBatch;

    const auto& rowBuffer = closure->OutputBuffer;

    Y_ASSERT(batch.size() < batch.capacity());

    batch.push_back(rowBuffer->Capture(values, closure->RowSize));

    // NB: Aggregate flag is neither set from TCG value nor cleared during row allocation.
    size_t id = 0;
    for (auto* value = batch.back().Begin(); value < batch.back().End(); ++value) {
        const_cast<TUnversionedValue*>(value)->Aggregate = false;
        const_cast<TUnversionedValue*>(value)->Id = id++;
    }

    if (batch.size() == batch.capacity()) {
        auto& writer = context->Writer;
        bool shouldNotWait;
        {
            NProfiling::TAggregatingTimingGuard timingGuard(&statistics->WriteTime);
            shouldNotWait = writer->Write(batch);
        }

        if (!shouldNotWait) {
            NProfiling::TAggregatingTimingGuard timingGuard(&statistics->AsyncTime);
            WaitFor(writer->GetReadyEvent())
                .ThrowOnError();
        }
        batch.clear();
        rowBuffer->Clear();
    }
}

void ScanOpHelper(
    TExecutionContext* context,
    void** consumeRowsClosure,
    void (*consumeRows)(void** closure, TRowBuffer*, const TValue** rows, i64 size))
{
    auto& reader = context->Reader;

    std::vector<TRow> rows;
    rows.reserve(context->IsOrdered && context->Limit < RowsetProcessingSize
        ? context->Limit
        : RowsetProcessingSize);

    if (rows.capacity() == 0) {
        return;
    }

    std::vector<const TValue*> values;
    values.reserve(rows.capacity());

    auto* statistics = context->Statistics;

    auto rowBuffer = New<TRowBuffer>(TIntermediateBufferTag());

    while (true) {
        bool hasMoreData;
        {
            NProfiling::TAggregatingTimingGuard timingGuard(&statistics->ReadTime);
            hasMoreData = reader->Read(&rows);
        }

        bool shouldWait = rows.empty();

        // Remove null rows.
        rows.erase(
            std::remove_if(rows.begin(), rows.end(), [] (TRow row) {
                return !row;
            }),
            rows.end());

        if (statistics->RowsRead + rows.size() >= context->InputRowLimit) {
            YCHECK(statistics->RowsRead <= context->InputRowLimit);
            rows.resize(context->InputRowLimit - statistics->RowsRead);
            statistics->IncompleteInput = true;
            hasMoreData = false;
        }
        statistics->RowsRead += rows.size();
        for (const auto& row : rows) {
            statistics->BytesRead += GetDataWeight(row);
        }

        for (auto row : rows) {
            values.push_back(row.Begin());
        }

        consumeRows(consumeRowsClosure, rowBuffer.Get(), values.data(), values.size());
        rows.clear();
        values.clear();
        rowBuffer->Clear();

        if (!hasMoreData) {
            break;
        }

        if (shouldWait) {
            NProfiling::TAggregatingTimingGuard timingGuard(&statistics->AsyncTime);
            WaitFor(reader->GetReadyEvent())
                .ThrowOnError();
        }
    }
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
            Y_ASSERT(CompareRows(closure->LastKey, closure->LastKey + rowSize, key, key + rowSize) <= 0);
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


char* AllocateAlignedBytes(TRowBuffer* buffer, size_t byteCount)
{
    return buffer
        ->GetPool()
        ->AllocateAligned(byteCount);
}

typedef std::pair<size_t, size_t> TSlot;

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

void StorePrimaryRow(
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

        *reinterpret_cast<TSlot*>(key + item.KeySize) = TSlot(0, 0);

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
        closure->ProcessJoinBatch();

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
}

class TJoinBatchState
{
public:
    TJoinBatchState(
        void** consumeRowsClosure,
        void (*consumeRows)(void** closure, TRowBuffer*, const TValue** rows, i64 size),
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

        while (currentKey != keysToRows.end()) {
            bool hasMoreData = reader->Read(&foreignRows);
            bool shouldWait = foreignRows.empty();

            if (isPartiallySorted) {
                processForeignSequence(foreignRows.begin(), foreignRows.end());
            } else {
                processSortedForeignSequence(foreignRows.begin(), foreignRows.end());
            }

            foreignRows.clear();

            if (!hasMoreData) {
                break;
            }

            if (shouldWait) {
                NProfiling::TAggregatingTimingGuard timingGuard(&context->Statistics->AsyncTime);
                WaitFor(reader->GetReadyEvent())
                    .ThrowOnError();
            }
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

        while (true) {
            bool hasMoreData = reader->Read(&foreignRows);
            bool shouldWait = foreignRows.empty();

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

            if (!hasMoreData) {
                break;
            }

            if (shouldWait) {
                NProfiling::TAggregatingTimingGuard timingGuard(&context->Statistics->AsyncTime);
                WaitFor(reader->GetReadyEvent())
                    .ThrowOnError();
            }
        }

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
    void (* const ConsumeRows)(void** closure, TRowBuffer*, const TValue** rows, i64 size);

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
        TRowBuffer* buffer),
    void** consumeRowsClosure,
    void (*consumeRows)(void** closure, TRowBuffer*, const TValue** rows, i64 size))
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

        LOG_DEBUG("Collected %v join keys from %v rows",
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
        Y_ASSERT(std::is_sorted(keys.begin(), keys.end()));

        // Join rowsets.
        // allRows have format (join key... , other columns...)

        auto& joinLookup = closure.Lookup;
        auto isLeft = parameters->IsLeft;

        if (!parameters->IsOrdered) {
            auto reader = parameters->ExecuteForeign(std::move(keys), closure.Buffer);

            LOG_DEBUG("Joining started");

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

                while (true) {
                    bool hasMoreData;
                    {
                        NProfiling::TAggregatingTimingGuard timingGuard(&context->Statistics->ReadTime);
                        hasMoreData = reader->Read(&rows);
                    }

                    bool shouldWait = foreignRows.empty();

                    for (auto row : rows) {
                        foreignLookup.insert(foreignRowsBuffer->Capture(row).Begin());
                    }
                    rows.clear();

                    if (!hasMoreData) {
                        break;
                    }

                    if (shouldWait) {
                        NProfiling::TAggregatingTimingGuard timingGuard(&context->Statistics->AsyncTime);
                        WaitFor(reader->GetReadyEvent())
                            .ThrowOnError();
                    }
                }
            }

            LOG_DEBUG("Got %v foreign rows", foreignLookup.size());
            LOG_DEBUG("Joining started");

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

        LOG_DEBUG("Joining finished");

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
        TRowBuffer* buffer),
    void** consumeRowsClosure,
    void (*consumeRows)(void** closure, TRowBuffer*, const TValue** rows, i64 size))
{
    TMultiJoinClosure closure;
    closure.Buffer = New<TRowBuffer>(TPermanentBufferTag(), PoolChunkSize, MaxSmallBlockRatio);
    closure.PrimaryRowSize = parameters->PrimaryRowSize;
    closure.BatchSize = parameters->BatchSize;

    for (size_t joinId = 0; joinId < parameters->Items.size(); ++joinId) {
        TMultiJoinClosure::TItem subclosure(
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
        LOG_DEBUG("Joining started");

        std::vector<ISchemafulReaderPtr> readers;
        for (size_t joinId = 0; joinId < closure.Items.size(); ++joinId) {
            closure.ProcessSegment(joinId);

            std::vector<TRow> orderedKeys;
            for (TValue* key : closure.Items[joinId].OrderedKeys) {
                orderedKeys.push_back(TRow(reinterpret_cast<const TUnversionedRowHeader*>(key) - 1));
            }

            auto reader = parameters->Items[joinId].ExecuteForeign(
                orderedKeys,
                closure.Items[joinId].Buffer);
            readers.push_back(reader);
            closure.Items[joinId].Lookup.clear();
            closure.Items[joinId].LastKey = nullptr;
        }

        std::vector<std::vector<TValue*>> sortedForeignSequences;
        for (size_t joinId = 0; joinId < closure.Items.size(); ++joinId) {
            closure.ProcessSegment(joinId);

            auto orderedKeys = std::move(closure.Items[joinId].OrderedKeys);

            LOG_DEBUG("Collected %v join keys",
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
                        if (slot->second == 0) {
                            slot->first = index;
                        }
                        ++slot->second;
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

            while (currentKey != orderedKeys.end()) {
                bool hasMoreData = reader->Read(&foreignRows);
                bool shouldWait = foreignRows.empty();

                for (size_t rowIndex = 0; rowIndex < foreignRows.size(); ++rowIndex) {
                    foreignValues.push_back(closure.Buffer->Capture(foreignRows[rowIndex]).Begin());
                }

                if (isPartiallySorted) {
                    processForeignSequence(foreignValues.begin(), foreignValues.end());
                } else {
                    sortedForeignSequence.insert(
                        sortedForeignSequence.end(),
                        foreignValues.begin(),
                        foreignValues.end());
                }

                foreignRows.clear();
                foreignValues.clear();

                if (!hasMoreData) {
                    break;
                }

                if (shouldWait) {
                    NProfiling::TAggregatingTimingGuard timingGuard(&context->Statistics->AsyncTime);
                    WaitFor(reader->GetReadyEvent())
                        .ThrowOnError();
                }
            }

            if (isPartiallySorted) {
                std::sort(
                    sortedForeignSequence.begin() + unsortedOffset,
                    sortedForeignSequence.end(),
                    foreignSuffixLessComparer);
            }

            processSortedForeignSequence();

            sortedForeignSequences.push_back(std::move(sortedForeignSequence));
        }

        auto intermediateBuffer = New<TRowBuffer>(TIntermediateBufferTag());
        std::vector<const TValue*> joinedRows;
        auto consumeJoinedRows = [&] () {
            // Consume joined rows.
            consumeRows(consumeRowsClosure, intermediateBuffer.Get(), joinedRows.data(), joinedRows.size());
            joinedRows.clear();
            intermediateBuffer->Clear();
        };

        // TODO: Join first row in place or join all rows in place and immediately consume them?

        size_t resultRowSize = closure.PrimaryRowSize;

        for (size_t joinId = 0; joinId < closure.Items.size(); ++joinId) {
            resultRowSize += parameters->Items[joinId].ForeignColumns.size();
        }

        LOG_DEBUG("Started producing joined rows");

        std::vector<size_t> indexes(closure.Items.size(), 0);

        auto joinRows = [&] (TValue* rowValues) {
            size_t incrementIndex = 0;
            while (incrementIndex < closure.Items.size()) {
                TMutableRow joinedRow = intermediateBuffer->AllocateUnversioned(resultRowSize);
                std::copy(rowValues, rowValues + closure.PrimaryRowSize, joinedRow.Begin());

                size_t offset = closure.PrimaryRowSize;
                for (size_t joinId = 0; joinId < closure.Items.size(); ++joinId) {
                    TSlot slot = *(reinterpret_cast<TSlot**>(rowValues + closure.PrimaryRowSize)[joinId]);
                    const auto& foreignIndexes = parameters->Items[joinId].ForeignColumns;

                    if (slot.second != 0) {
                        YCHECK(indexes[joinId] < slot.second);
                        TValue* foreignRow = sortedForeignSequences[joinId][slot.first + indexes[joinId]];

                        if (incrementIndex == joinId) {
                            ++indexes[joinId];
                            if (indexes[joinId] == slot.second) {
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
                        ++incrementIndex;
                        bool isLeft = parameters->Items[joinId].IsLeft;
                        if (!isLeft) {
                            indexes.assign(closure.Items.size(), 0);
                            return;
                        }
                        for (size_t count = foreignIndexes.size(); count > 0; --count) {
                            joinedRow[offset++] = MakeUnversionedSentinelValue(EValueType::Null);
                        }
                    }
                }

                joinedRows.push_back(joinedRow.Begin());

                if (joinedRows.size() >= RowsetProcessingSize) {
                    consumeJoinedRows();
                }
            }
        };

        for (TValue* rowValues : closure.PrimaryRows) {
            joinRows(rowValues);
        }

        consumeJoinedRows();

        closure.PrimaryRows.clear();
        closure.Buffer->Clear();
        for (auto& joinItem : closure.Items) {
            joinItem.Buffer->Clear();
        }

        LOG_DEBUG("Joining finished");
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

    auto inserted = closure->Lookup.insert(row);

    if (inserted.second) {
        if (closure->GroupedRows.size() >= context->GroupRowLimit) {
            throw TInterruptedIncompleteException();
        }

        closure->GroupedRows.push_back(row);
        for (int index = 0; index < closure->KeySize; ++index) {
            closure->Buffer->Capture(&row[index]);
        }

        if (closure->CheckNulls) {
            for (int index = 0; index < closure->KeySize; ++index) {
                if (row[index].Type == EValueType::Null) {
                    THROW_ERROR_EXCEPTION("Null values in group key");
                }
            }
        }
    }

    return *inserted.first;
}

void GroupOpHelper(
    TExecutionContext* context,
    THasherFunction* groupHasher,
    TComparerFunction* groupComparer,
    int keySize,
    bool checkNulls,
    void** collectRowsClosure,
    void (*collectRows)(
        void** closure,
        TGroupByClosure* groupByClosure,
        TRowBuffer* buffer),
    void** consumeRowsClosure,
    void (*consumeRows)(void** closure, TRowBuffer*, const TValue** rows, i64 size))
{
    TGroupByClosure closure(groupHasher, groupComparer, keySize, checkNulls);

    try {
        collectRows(collectRowsClosure, &closure, closure.Buffer.Get());
    } catch (const TInterruptedIncompleteException&) {
        // Set incomplete and continue
        context->Statistics->IncompleteOutput = true;
    }

    LOG_DEBUG("Collected %v group rows",
        closure.GroupedRows.size());

    auto intermediateBuffer = New<TRowBuffer>(TIntermediateBufferTag());

    for (size_t index = 0; index < closure.GroupedRows.size(); index += RowsetProcessingSize) {
        auto size = std::min(RowsetProcessingSize, closure.GroupedRows.size() - index);
        consumeRows(consumeRowsClosure, intermediateBuffer.Get(), closure.GroupedRows.data() + index, size);
        intermediateBuffer->Clear();
    }
}

void AllocatePermanentRow(TExecutionContext* context, TRowBuffer* buffer, int valueCount, TValue** row)
{
    CHECK_STACK();

    *row = buffer->AllocateUnversioned(valueCount).Begin();
}

void AddRow(TTopCollector* topCollector, TValue* row)
{
    topCollector->AddRow(row);
}

void OrderOpHelper(
    TExecutionContext* context,
    TComparerFunction* comparer,
    void** collectRowsClosure,
    void (*collectRows)(void** closure, TTopCollector* topCollector),
    void** consumeRowsClosure,
    void (*consumeRows)(void** closure, TRowBuffer*, const TValue** rows, i64 size),
    size_t rowSize)
{
    auto limit = context->Limit;

    TTopCollector topCollector(limit, comparer, rowSize);
    collectRows(collectRowsClosure, &topCollector);
    auto rows = topCollector.GetRows();

    auto rowBuffer = New<TRowBuffer>(TIntermediateBufferTag());

    for (size_t index = 0; index < rows.size(); index += RowsetProcessingSize) {
        auto size = std::min(RowsetProcessingSize, rows.size() - index);
        consumeRows(consumeRowsClosure, rowBuffer.Get(), rows.data() + index, size);
        rowBuffer->Clear();
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
    } catch (const TInterruptedCompleteException&) {
        // Continue
    }

    LOG_DEBUG("Flushing writer");
    if (!closure.OutputRowsBatch.empty()) {
        bool shouldNotWait;
        {
            NProfiling::TAggregatingTimingGuard timingGuard(&context->Statistics->WriteTime);
            shouldNotWait = context->Writer->Write(closure.OutputRowsBatch);
        }

        if (!shouldNotWait) {
            NProfiling::TAggregatingTimingGuard timingGuard(&context->Statistics->AsyncTime);
            WaitFor(context->Writer->GetReadyEvent())
                .ThrowOnError();
        }
    }

    LOG_DEBUG("Closing writer");
    {
        NProfiling::TAggregatingTimingGuard timingGuard(&context->Statistics->AsyncTime);
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

char IsRowInArray(TComparerFunction* comparer, TValue* values, TSharedRange<TRow>* rows)
{
    auto found = std::lower_bound(rows->Begin(), rows->End(), values, [&] (TRow row, TValue* values) {
        return comparer(const_cast<TValue*>(row.Begin()), values);
    });

    return found != rows->End() && !comparer(values, const_cast<TValue*>(found->Begin()));
}

const TValue* TransformTuple(TComparerFunction* comparer, TValue* values, TSharedRange<TRow>* rows)
{
    auto found = std::lower_bound(rows->Begin(), rows->End(), values, [&] (TRow row, TValue* values) {
        return comparer(const_cast<TValue*>(row.Begin()), values);
    });

    if (found != rows->End() && !comparer(values, const_cast<TValue*>(found->Begin()))) {
        return const_cast<TValue*>(found->Begin());
    }

    return nullptr;
}

size_t StringHash(
    const char* data,
    ui32 length)
{
    return FarmHash(data, length);
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
                break;
            case EValueType::Uint64:
                result = hash64(value->Data.Uint64, result);
                break;
            case EValueType::Boolean:
                result = hash64(value->Data.Boolean, result);
                break;
            case EValueType::String:
                result = hash(
                    value->Data.String,
                    value->Length,
                    result);
                break;
            case EValueType::Null:
                result = hash64(0, result);
                break;
            default:
                Y_UNREACHABLE();
        }
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
    YCHECK(string->Type == EValueType::String);

    return re2::RE2::FullMatch(
        re2::StringPiece(string->Data.String, string->Length),
        *re2);
}

ui8 RegexPartialMatch(re2::RE2* re2, TUnversionedValue* string)
{
    YCHECK(string->Type == EValueType::String);

    return re2::RE2::PartialMatch(
        re2::StringPiece(string->Data.String, string->Length),
        *re2);
}

template <typename StringType>
void CopyString(TExpressionContext* context, TUnversionedValue* result, const StringType& str)
{
    char* data = AllocateBytes(context, str.size());
    memcpy(data, str.c_str(), str.size());
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
    YCHECK(string->Type == EValueType::String);
    YCHECK(rewrite->Type == EValueType::String);

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
    YCHECK(string->Type == EValueType::String);
    YCHECK(rewrite->Type == EValueType::String);

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
    YCHECK(string->Type == EValueType::String);
    YCHECK(rewrite->Type == EValueType::String);

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

    Y_UNREACHABLE();
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
    TStringStream stream;
    NYson::TYsonWriter writer(&stream);

    switch (value->Type) {
        case EValueType::Null: {
            result->Type = EValueType::Null;
            return;
        }
        case EValueType::Any: {
            *result = *value;
            return;
        }
        case EValueType::String: {
            writer.OnStringScalar(TStringBuf(value->Data.String, value->Length));
            break;
        }
        case EValueType::Int64: {
            writer.OnInt64Scalar(value->Data.Int64);
            break;
        }
        case EValueType::Uint64: {
            writer.OnUint64Scalar(value->Data.Uint64);
            break;
        }
        case EValueType::Double: {
            writer.OnDoubleScalar(value->Data.Double);
            break;
        }
        case EValueType::Boolean: {
            writer.OnBooleanScalar(value->Data.Boolean);
            break;
        }
        default:
            Y_UNREACHABLE();
    }

    writer.Flush();
    result->Type = EValueType::Any;
    result->Length = stream.Size();
    result->Data.String = stream.Data();
    *result = context->Capture(*result);
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
                break;
            case EValueType::Uint64:
                writer.OnUint64Scalar(valueArg.Data.Uint64);
                break;
            case EValueType::Double:
                writer.OnDoubleScalar(valueArg.Data.Double);
                break;
            case EValueType::Boolean:
                writer.OnBooleanScalar(valueArg.Data.Boolean);
                break;
            case EValueType::String:
                writer.OnStringScalar(TStringBuf(valueArg.Data.String, valueArg.Length));
                break;
            case EValueType::Any:
                writer.OnRaw(TStringBuf(valueArg.Data.String, valueArg.Length));
                break;
            case EValueType::Null:
                writer.OnEntity();
                break;
            default:
                THROW_ERROR_EXCEPTION("Unexpected type %Qlv of value in key-value pair #%v",
                    valueArg.Type,
                    index);
        }
    }
    writer.OnEndMap();

    *result = context->Capture(MakeUnversionedAnyValue(resultYson));
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
    REGISTER_ROUTINE(StringHash);
    REGISTER_ROUTINE(AllocatePermanentRow);
    REGISTER_ROUTINE(AllocateBytes);
    REGISTER_ROUTINE(IsRowInArray);
    REGISTER_ROUTINE(TransformTuple);
    REGISTER_ROUTINE(SimpleHash);
    REGISTER_ROUTINE(FarmHashUint64);
    REGISTER_ROUTINE(AddRow);
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

} // namespace NQueryClient
} // namespace NYT
