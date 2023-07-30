#include "cg_routines.h"
#include "cg_types.h"

#include <yt/yt/library/query/engine_api/evaluation_helpers.h>

#include <yt/yt/library/query/base/private.h>

#include <yt/yt/client/security_client/acl.h>
#include <yt/yt/client/security_client/helpers.h>

#include <yt/yt/client/query_client/query_statistics.h>

#include <yt/yt/client/table_client/row_buffer.h>
#include <yt/yt/client/table_client/unversioned_reader.h>
#include <yt/yt/client/table_client/unversioned_writer.h>
#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/row_batch.h>
#include <yt/yt/client/table_client/helpers.h>

#include <yt/yt/core/yson/lexer.h>
#include <yt/yt/core/yson/parser.h>
#include <yt/yt/core/yson/pull_parser.h>
#include <yt/yt/core/yson/token.h>
#include <yt/yt/core/yson/writer.h>

#include <yt/yt/core/ytree/ypath_resolver.h>
#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/misc/finally.h>
#include <yt/yt/core/misc/hyperloglog.h>

#include <yt/yt/core/profiling/timing.h>

#include <contrib/libs/re2/re2/re2.h>

#include <library/cpp/yt/memory/chunked_memory_pool_output.h>

#include <library/cpp/yt/farmhash/farm_hash.h>

#include <library/cpp/xdelta3/state/merge.h>

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
using namespace NYTree;

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

    void Checkpoint(i64 processedRows)
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

    YT_ASSERT(batch.size() < WriteRowsetSize);

    batch.push_back(rowBuffer->CaptureRow(MakeRange(values, closure->RowSize)));

    // NB: Flags are neither set from TCG value nor cleared during row allocation.
    // XXX(babenko): fix this
    size_t id = 0;
    for (auto* value = batch.back().Begin(); value < batch.back().End(); ++value) {
        auto mutableValue = const_cast<TUnversionedValue*>(value);
        mutableValue->Id = id++;
        mutableValue->Flags = {};
        if (!IsStringLikeType(value->Type)) {
            mutableValue->Length = 0;
        }
    }

    if (batch.size() == WriteRowsetSize) {
        const auto& writer = context->Writer;
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
    if (context->Limit == 0) {
        return;
    }

    auto startBatchSize = context->Offset + context->Limit;

    TRowBatchReadOptions readOptions{
        .MaxRowsPerRead = context->Ordered ? std::min(startBatchSize, RowsetProcessingSize) : RowsetProcessingSize
    };

    std::vector<const TValue*> values;
    values.reserve(readOptions.MaxRowsPerRead);

    auto& reader = context->Reader;
    auto* statistics = context->Statistics;

    TYielder yielder;

    auto rowBuffer = New<TRowBuffer>(TIntermediateBufferTag());
    std::vector<TUnversionedRow> rows;

    bool interrupt = false;
    while (!interrupt) {
        IUnversionedRowBatchPtr batch;
        {
            TValueIncrementingTimingGuard<TFiberWallTimer> timingGuard(&statistics->ReadTime);
            batch = reader->Read(readOptions);
            if (!batch) {
                break;
            }
            // Materialize rows here.
            // Drop null rows.
            auto batchRows = batch->MaterializeRows();
            rows.reserve(batchRows.size());
            rows.clear();
            for (auto row : batchRows) {
                if (row) {
                    rows.push_back(row);
                }
            }
        }

        if (batch->IsEmpty()) {
            TValueIncrementingTimingGuard<TWallTimer> timingGuard(&statistics->WaitOnReadyEventTime);
            WaitFor(reader->GetReadyEvent())
                .ThrowOnError();
            continue;
        }

        if (statistics->RowsRead + std::ssize(rows) >= context->InputRowLimit) {
            YT_VERIFY(statistics->RowsRead <= context->InputRowLimit);
            rows.resize(context->InputRowLimit - statistics->RowsRead);
            statistics->IncompleteInput = true;
            interrupt = true;
        }

        statistics->RowsRead += rows.size();
        for (auto row : rows) {
            statistics->DataWeightRead += GetDataWeight(row);
            values.push_back(row.Begin());
        }

        interrupt |= consumeRows(consumeRowsClosure, rowBuffer.Get(), values.data(), values.size());

        yielder.Checkpoint(statistics->RowsRead);

        values.clear();
        rowBuffer->Clear();

        if (!context->IsMerge) {
            readOptions.MaxRowsPerRead = std::min(2 * readOptions.MaxRowsPerRead, RowsetProcessingSize);
        }
    }
}

char* AllocateAlignedBytes(TExpressionContext* context, size_t byteCount)
{
    return context
        ->GetPool()
        ->AllocateAligned(byteCount);
}

struct TSlot
{
    size_t Offset;
    size_t Count;
};

TValue* AllocateJoinKeys(
    TExecutionContext* /*context*/,
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
    if (std::ssize(closure->PrimaryRows) >= context->JoinRowLimit) {
        throw TInterruptedIncompleteException();
    }

    closure->PrimaryRows.emplace_back(*primaryValues);

    for (size_t columnIndex = 0; columnIndex < closure->PrimaryRowSize; ++columnIndex) {
        closure->Buffer->CaptureValue(*primaryValues + columnIndex);
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
                closure->Items[joinId].Buffer->CaptureValue(&key[columnIndex]);
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
        closure->BatchSize = std::min<size_t>(2 * closure->BatchSize, MaxJoinBatchSize);

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

void MultiJoinOpHelper(
    TExecutionContext* context,
    TMultiJoinParameters* parameters,
    TJoinComparers* comparers,
    void** collectRowsClosure,
    void (*collectRows)(
        void** closure,
        TMultiJoinClosure* joinClosure,
        TExpressionContext* context),
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

        std::vector<ISchemafulUnversionedReaderPtr> readers;
        for (size_t joinId = 0; joinId < closure.Items.size(); ++joinId) {
            closure.ProcessSegment(joinId);

            std::vector<TRow> orderedKeys;
            for (TValue* key : closure.Items[joinId].OrderedKeys) {
                // NB: Flags are neither set from TCG value nor cleared during row allocation.
                size_t id = 0;
                for (auto* value = key; value < key + closure.Items[joinId].KeySize; ++value) {
                    value->Flags = {};
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

            TRowBatchReadOptions readOptions{
                .MaxRowsPerRead = RowsetProcessingSize
            };

            std::vector<TValue*> foreignValues;
            foreignValues.reserve(readOptions.MaxRowsPerRead);

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

            while (currentKey != orderedKeys.end()) {
                IUnversionedRowBatchPtr foreignBatch;
                TRange<TUnversionedRow> foreignRows;
                {
                    TValueIncrementingTimingGuard<TFiberWallTimer> timingGuard(&context->Statistics->ReadTime);
                    foreignBatch = reader->Read(readOptions);
                    if (!foreignBatch) {
                        break;
                    }
                    // Materialize rows here.
                    foreignRows = foreignBatch->MaterializeRows();
                }

                if (foreignBatch->IsEmpty()) {
                    TValueIncrementingTimingGuard<TWallTimer> timingGuard(&context->Statistics->WaitOnReadyEventTime);
                    WaitFor(reader->GetReadyEvent())
                        .ThrowOnError();
                    continue;
                }

                for (auto row : foreignRows) {
                    foreignValues.push_back(closure.Buffer->CaptureRow(row).Begin());
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

        i64 processedRows = 0;

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
    TValue* row,
    bool allAggregatesFirst)
{
    CHECK_STACK();

    if (closure->LastKey && !closure->PrefixEqComparer(row, closure->LastKey)) {
        closure->ProcessSegment();
    }

    // Any prefix but ordered scan.
    if (context->Ordered && static_cast<i64>(closure->GroupedRowCount) >= context->Offset + context->Limit) {
        if (allAggregatesFirst) {
            return nullptr;
        }

        YT_VERIFY(static_cast<i64>(closure->GroupedRowCount) == context->Offset + context->Limit);
        auto found = closure->Lookup.find(row);
        return found != closure->Lookup.end() ? *found : nullptr;
    }

    // FIXME: Incorrect in case of grouping by prefix.
    bool limitReached = std::ssize(closure->GroupedRows) == context->GroupRowLimit;

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
        YT_VERIFY(std::ssize(closure->GroupedRows) <= context->GroupRowLimit);

        for (int index = 0; index < closure->KeySize; ++index) {
            closure->Buffer->CaptureValue(&row[index]);
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
        TExpressionContext* context),
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
    i64 processedRows = 0;

    auto intermediateBuffer = New<TRowBuffer>(TIntermediateBufferTag());

    auto flushGroupedRows = [&] (bool isBoundary, const TValue** begin, const TValue** end) {
        auto finished = false;

        if (context->Ordered && processedRows < context->Offset) {
            i64 skip = std::min(context->Offset - processedRows, end - begin);
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
    TExecutionContext* /*context*/,
    void** collectRowsClosure,
    void (*collectRows)(
        void** closure,
        TExpressionContext* context))
{
    auto buffer = New<TRowBuffer>(TIntermediateBufferTag());
    collectRows(collectRowsClosure, buffer.Get());
}

void AllocatePermanentRow(
    TExecutionContext* /*executionContext*/,
    TExpressionContext* expressionContext,
    int valueCount,
    TValue** row)
{
    CHECK_STACK();

    *row = expressionContext->AllocateUnversioned(valueCount).Begin();
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

    auto rowCount = static_cast<i64>(rows.size());
    for (i64 index = context->Offset; index < rowCount; index += RowsetProcessingSize) {
        auto size = std::min(RowsetProcessingSize, rowCount - index);
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
    TWriteOpClosure closure(context->MemoryChunkProvider);
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
    auto it = std::lower_bound(
        ranges->Begin(),
        ranges->End(),
        values,
        [&] (TRowRange range, TValue* values) {
            ui32 length = std::min(range.second.GetCount(), valuesCount);
            return CompareValueRanges(range.second.FirstNElements(length), MakeRange(values, length)) < 0;
        });

    if (it == ranges->End()) {
        return false;
    }

    ui32 length = std::min(it->first.GetCount(), valuesCount);
    return CompareValueRanges(it->first.FirstNElements(length), MakeRange(values, length)) <= 0;
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
        switch (value->Type) {
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
            regexp->AsString())
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

template <typename TStringType>
void CopyString(TExpressionContext* context, TUnversionedValue* result, const TStringType& str)
{
    char* data = AllocateBytes(context, str.size());
    memcpy(data, str.data(), str.size());
    *result = MakeUnversionedStringValue(TStringBuf(data, str.size()));
}

template <typename TStringType>
void CopyAny(TExpressionContext* context, TUnversionedValue* result, const TStringType& str)
{
    char* data = AllocateBytes(context, str.size());
    memcpy(data, str.c_str(), str.size());
    *result = MakeUnversionedAnyValue(TStringBuf(data, str.size()));
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

    std::string str(string->Data.String, string->Length);
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

    std::string str(string->Data.String, string->Length);
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

    std::string str;
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

static void* XdeltaAllocate(void* opaque, size_t size)
{
    if (opaque) {
        return reinterpret_cast<uint8_t*>(NRoutines::AllocateBytes(static_cast<NYT::NCodegen::TExpressionContext*>(opaque), size));
    }
    return reinterpret_cast<uint8_t*>(malloc(size));
}

static void XdeltaFree(void* opaque, void* ptr)
{
    if (!opaque) {
        free(ptr);
    }
}

int XdeltaMerge(
    void* context,
    const uint8_t* lhsData,
    size_t lhsSize,
    const uint8_t* rhsData,
    size_t rhsSize,
    const uint8_t** resultData,
    size_t* resultOffset,
    size_t* resultSize)
{
    NXdeltaAggregateColumn::TSpan result;
    XDeltaContext ctx{
        .opaque = context,
        .allocate = XdeltaAllocate,
        .free = XdeltaFree
    };
    auto code = NXdeltaAggregateColumn::MergeStates(&ctx, lhsData, lhsSize, rhsData, rhsSize, &result);
    if (code == 0) {
        ThrowException("Failed to merge xdelta states");
    }
    *resultData = result.Data;
    *resultOffset = result.Offset;
    *resultSize = result.Size;

    return code;
}

#define DEFINE_YPATH_GET_IMPL2(PREFIX, TYPE, STATEMENT_OK, STATEMENT_FAIL) \
    void PREFIX ## Get ## TYPE( \
        [[maybe_unused]] TExpressionContext* context, \
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
        *result = MakeUnversionedNullValue();) \
    DEFINE_YPATH_GET_IMPL2(, TYPE, STATEMENT_OK, \
        THROW_ERROR_EXCEPTION("Value of type %Qlv is not found at YPath %v", \
            EValueType::TYPE, \
            ypath->AsStringBuf());)

#define DEFINE_YPATH_GET(TYPE) \
    DEFINE_YPATH_GET_IMPL(TYPE, \
        *result = MakeUnversioned ## TYPE ## Value(*value);)

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
    void AnyTo ## TYPE([[maybe_unused]] TExpressionContext* context, TUnversionedValue* result, TUnversionedValue* anyValue) \
    { \
        if (anyValue->Type == EValueType::Null) { \
            *result = MakeUnversionedNullValue(); \
            return; \
        } \
        NYson::TToken token; \
        auto anyString = anyValue->AsStringBuf(); \
        NYson::ParseToken(anyString, &token); \
        if (token.GetType() == NYson::ETokenType::TYPE) { \
            STATEMENT_OK \
        } else { \
            THROW_ERROR_EXCEPTION("Cannot convert value %Qv of type \"any\" to %Qlv", \
                anyString, \
                EValueType::TYPE); \
        } \
    }

#define DEFINE_CONVERT_ANY_NUMERIC_IMPL(TYPE) \
    void AnyTo ## TYPE(TExpressionContext* /*context*/, TUnversionedValue* result, TUnversionedValue* anyValue) \
    { \
        if (anyValue->Type == EValueType::Null) { \
            *result = MakeUnversionedNullValue(); \
            return; \
        } \
        NYson::TToken token; \
        auto anyString = anyValue->AsStringBuf(); \
        NYson::ParseToken(anyString, &token); \
        if (token.GetType() == NYson::ETokenType::Int64) { \
            *result = MakeUnversioned ## TYPE ## Value(token.GetInt64Value()); \
        } else if (token.GetType() == NYson::ETokenType::Uint64) { \
            *result = MakeUnversioned ## TYPE ## Value(token.GetUint64Value()); \
        } else if (token.GetType() == NYson::ETokenType::Double) { \
            *result = MakeUnversioned ## TYPE ## Value(token.GetDoubleValue()); \
        } else { \
            THROW_ERROR_EXCEPTION("Cannot convert value %Qv of type \"any\" to %Qlv", \
                anyString, \
                EValueType::TYPE); \
        } \
    }

DEFINE_CONVERT_ANY_NUMERIC_IMPL(Int64)
DEFINE_CONVERT_ANY_NUMERIC_IMPL(Uint64)
DEFINE_CONVERT_ANY_NUMERIC_IMPL(Double)
DEFINE_CONVERT_ANY(Boolean, *result = MakeUnversionedBooleanValue(token.GetBooleanValue());)
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
    lexer.ParseToken(lhsInput, &lhsToken);
    lexer.ParseToken(rhsInput, &rhsToken);

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
    lexer.ParseToken(lhsInput, &lhsToken); \
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
    lexer.ParseToken(lhsInput, &lhsToken);
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
    // TODO(babenko): for some reason, flags are garbage here.
    value->Flags = {};
    *result = NTableClient::EncodeUnversionedAnyValue(*value, context->GetPool());
}

////////////////////////////////////////////////////////////////////////////////

void ToLowerUTF8(TExpressionContext* context, char** result, int* resultLength, char* source, int sourceLength)
{
    auto lowered = ToLowerUTF8(TStringBuf(source, sourceLength));
    *result = AllocateBytes(context, lowered.size());
    for (int i = 0; i < std::ssize(lowered); i++) {
        (*result)[i] = lowered[i];
    }
    *resultLength = lowered.size();
}

TFingerprint GetFarmFingerprint(const TUnversionedValue* begin, const TUnversionedValue* end)
{
    return NYT::NTableClient::GetFarmFingerprint({begin, end});
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
        writer.OnKeyedItem(nameArg.AsStringBuf());

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
                writer.OnStringScalar(valueArg.AsStringBuf());
                break;
            case EValueType::Any:
                writer.OnRaw(valueArg.AsStringBuf());
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

    *result = context->CaptureValue(MakeUnversionedAnyValue(resultYson));
}

////////////////////////////////////////////////////////////////////////////////

template <ENodeType NodeType, typename TElement, typename TValue>
bool ListContainsImpl(const INodePtr& node, const TValue& value)
{
    for (const auto& element : node->AsList()->GetChildren()) {
        if (element->GetType() == NodeType && ConvertTo<TElement>(element) == value) {
            return true;
        }
    }
    return false;
}

void ListContains(
    TExpressionContext* /*context*/,
    TUnversionedValue* result,
    TUnversionedValue* ysonList,
    TUnversionedValue* what)
{
    auto node = ConvertToNode(FromUnversionedValue<TYsonStringBuf>(*ysonList));

    bool found;
    switch (what->Type) {
        case EValueType::String:
            found = ListContainsImpl<ENodeType::String, TString>(node, what->AsString());
            break;
        case EValueType::Int64:
            found = ListContainsImpl<ENodeType::Int64, i64>(node, what->Data.Int64);
            break;
        case EValueType::Uint64:
            found = ListContainsImpl<ENodeType::Uint64, ui64>(node, what->Data.Uint64);
            break;
        case EValueType::Boolean:
            found = ListContainsImpl<ENodeType::Boolean, bool>(node, what->Data.Boolean);
            break;
        case EValueType::Double:
            found = ListContainsImpl<ENodeType::Double, double>(node, what->Data.Double);
            break;
        default:
            THROW_ERROR_EXCEPTION("ListContains is not implemented for %Qlv values",
                what->Type);
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
    auto refs = output.Finish();
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

extern "C" void NumericToString(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* value
)
{
    if (value->Type == EValueType::Null) {
        result->Type = EValueType::Null;
        return;
    }

    TString resultYson;
    TStringOutput output(resultYson);
    TYsonWriter writer(&output, EYsonFormat::Text);

    switch (value->Type) {
        case EValueType::Int64:
            writer.OnInt64Scalar(value->Data.Int64);
            break;
        case EValueType::Uint64:
            writer.OnUint64Scalar(value->Data.Uint64);
            break;
        case EValueType::Double:
            writer.OnDoubleScalar(value->Data.Double);
            break;
        default:
            YT_ABORT();
    }

    *result = context->CaptureValue(MakeUnversionedStringValue(resultYson));
}

////////////////////////////////////////////////////////////////////////////////

#define DEFINE_CONVERT_STRING(TYPE) \
    extern "C" void StringTo ## TYPE(TExpressionContext* /*context*/, TUnversionedValue* result, TUnversionedValue* value) \
    { \
        if (value->Type == EValueType::Null) { \
            *result = MakeUnversionedNullValue(); \
            return; \
        } \
        NYson::TToken token; \
        auto valueString = value->AsStringBuf(); \
        NYson::ParseToken(valueString, &token); \
        if (token.GetType() == NYson::ETokenType::Int64) { \
            *result = MakeUnversioned ## TYPE ## Value(token.GetInt64Value()); \
        } else if (token.GetType() == NYson::ETokenType::Uint64) { \
            *result = MakeUnversioned ## TYPE ## Value(token.GetUint64Value()); \
        } else if (token.GetType() == NYson::ETokenType::Double) { \
            *result = MakeUnversioned ## TYPE ## Value(token.GetDoubleValue()); \
        } else { \
            THROW_ERROR_EXCEPTION("Cannot convert value %Qv of type %Qlv to \"string\"", \
                valueString, \
                EValueType::TYPE); \
        } \
    }

DEFINE_CONVERT_STRING(Int64)
DEFINE_CONVERT_STRING(Uint64)
DEFINE_CONVERT_STRING(Double)

////////////////////////////////////////////////////////////////////////////////

void HyperLogLogAllocate(TExpressionContext* context, TUnversionedValue* result)
{
    auto* hll = AllocateBytes(context, sizeof(THLL));
    new (hll) THLL();
    *result = MakeUnversionedStringValue(TStringBuf(hll, sizeof(THLL)));
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
    TExpressionContext* /*context*/,
    TUnversionedValue* result,
    TUnversionedValue* ysonAcl,
    TUnversionedValue* ysonSubjectClosureList,
    TUnversionedValue* ysonPermissionList)
{
    using namespace NYTree;
    using namespace NYson;

    auto acl = ConvertTo<NSecurityClient::TSerializableAccessControlList>(
        FromUnversionedValue<TYsonStringBuf>(*ysonAcl));
    // NB: "subjectClosure" and "permissions" are being passed as strings.
    auto subjectClosure = ConvertTo<THashSet<TString>>(
        TYsonStringBuf(FromUnversionedValue<TStringBuf>(*ysonSubjectClosureList)));
    auto permissions = ConvertTo<EPermissionSet>(
        TYsonStringBuf(FromUnversionedValue<TStringBuf>(*ysonPermissionList)));

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
    REGISTER_ROUTINE(XdeltaMerge);
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
    REGISTER_ROUTINE(NumericToString);
    REGISTER_ROUTINE(StringToInt64);
    REGISTER_ROUTINE(StringToUint64);
    REGISTER_ROUTINE(StringToDouble);
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
