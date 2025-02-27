#include "cg_routines.h"
#include "cg_types.h"
#include "web_assembly_data_transfer.h"

#include <yt/yt/library/web_assembly/api/compartment.h>
#include <yt/yt/library/web_assembly/api/function.h>
#include <yt/yt/library/web_assembly/api/pointer.h>

#include <yt/yt/library/web_assembly/engine/intrinsics.h>
#include <yt/yt/library/web_assembly/engine/wavm_private_imports.h>

#include <yt/yt/library/query/base/private.h>

#include <yt/yt/library/query/engine/time/dates.h>

#include <yt/yt/library/query/engine_api/position_independent_value.h>
#include <yt/yt/library/query/engine_api/position_independent_value_transfer.h>

#include <yt/yt/client/security_client/acl.h>
#include <yt/yt/client/security_client/helpers.h>

#include <yt/yt/client/query_client/query_statistics.h>

#include <yt/yt/client/table_client/composite_compare.h>
#include <yt/yt/client/table_client/logical_type.h>
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

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/ypath_resolver.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/misc/finally.h>
#include <yt/yt/core/misc/hyperloglog.h>

#include <yt/yt/core/profiling/timing.h>

#include <contrib/libs/re2/re2/re2.h>

#include <library/cpp/yt/memory/chunked_memory_pool_output.h>

#include <library/cpp/yt/farmhash/farm_hash.h>

#include <library/cpp/yt/string/guid.h>

#include <library/cpp/xdelta3/state/merge.h>

#include <util/charset/utf8.h>
#include <util/digest/multi.h>

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

using namespace NWebAssembly;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = QueryClientLogger;

////////////////////////////////////////////////////////////////////////////////

// NB: Since pointer to current compartment is stored inside of a thread local,
// calls to context-switching functions should be guarded via this function.
template <typename TFunction>
auto SaveAndRestoreCurrentCompartment(const TFunction& function) -> decltype(function())
{
    auto* savedCompartment = GetCurrentCompartment();

    auto finally = Finally([&] {
        YT_VERIFY(GetCurrentCompartment() == nullptr);
        SetCurrentCompartment(savedCompartment);
    });

    SetCurrentCompartment(nullptr);

    return function();
}

////////////////////////////////////////////////////////////////////////////////

namespace NRoutines {

////////////////////////////////////////////////////////////////////////////////

using namespace NConcurrency;
using namespace NTableClient;
using namespace NProfiling;
using namespace NWebAssembly;
using namespace NYson;
using namespace NYTree;
using namespace NChunkClient;

////////////////////////////////////////////////////////////////////////////////

using THLL = NYT::THyperLogLog<14>;
static_assert(std::is_trivially_destructible<THLL>::value);

////////////////////////////////////////////////////////////////////////////////

static constexpr auto YieldThreshold = TDuration::MilliSeconds(30);

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
            SaveAndRestoreCurrentCompartment([&] {
                Yield();
            });
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

using TRowsConsumer = bool (*)(void** closure, TExpressionContext*, const TPIValue** rows, i64 size);
using TUnversionedRowsConsumer = bool (*)(void** closure, TExpressionContext*, const TValue** rows, i64 size);

bool WriteRow(TExecutionContext* context, TWriteOpClosure* closure, TPIValue* values)
{
    auto* statistics = context->Statistics;

    if (statistics->RowsWritten >= context->OutputRowLimit) {
        throw TInterruptedIncompleteException();
    }

    ++statistics->RowsWritten;

    auto& batch = closure->OutputRowsBatch;

    auto& outputContext = closure->OutputContext;

    YT_ASSERT(batch.size() < WriteRowsetSize);

    batch.push_back(
        CopyAndConvertFromPI(
            &outputContext,
            TRange(values, closure->RowSize),
            EAddressSpace::WebAssembly));

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
            shouldNotWait = SaveAndRestoreCurrentCompartment([&] {
                return writer->Write(batch);
            });
        }

        if (!shouldNotWait) {
            TValueIncrementingTimingGuard<TWallTimer> timingGuard(&statistics->WaitOnReadyEventTime);
            SaveAndRestoreCurrentCompartment([&] {
                WaitForFast(writer->GetReadyEvent())
                    .ThrowOnError();
            });
        }
        batch.clear();
        outputContext.Clear();
    }

    return false;
}

void ScanOpHelper(
    TExecutionContext* context,
    void** consumeRowsClosure,
    TUnversionedRowsConsumer consumeRowsFunction,
    TRowSchemaInformation* rowSchemaInformation)
{
    auto consumeRows = PrepareFunction(consumeRowsFunction);

    auto finalLogger = Finally([&] {
        YT_LOG_DEBUG("Finalizing scan helper");
    });
    if (context->Limit == 0) {
        return;
    }

    auto startBatchSize = context->Offset + context->Limit;

    TRowBatchReadOptions readOptions{
        .MaxRowsPerRead = context->Ordered
            ? std::min(startBatchSize, RowsetProcessingBatchSize)
            : RowsetProcessingBatchSize
    };

    std::vector<const TValue*> values;
    values.reserve(readOptions.MaxRowsPerRead);

    auto& reader = context->Reader;
    auto* statistics = context->Statistics;

    TYielder yielder;

    auto scanContext = MakeExpressionContext(
        TIntermediateBufferTag(),
        context->MemoryChunkProvider);
    std::vector<TUnversionedRow> rows;

    bool interrupt = false;
    while (!interrupt) {
        IUnversionedRowBatchPtr batch;
        {
            TValueIncrementingTimingGuard<TFiberWallTimer> timingGuard(&statistics->ReadTime);
            batch = SaveAndRestoreCurrentCompartment([&] {
                return reader->Read(readOptions);
            });
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
            SaveAndRestoreCurrentCompartment([&] {
                WaitForFast(reader->GetReadyEvent())
                    .ThrowOnError();
            });
            continue;
        }

        if (statistics->RowsRead + std::ssize(rows) >= context->InputRowLimit) {
            YT_VERIFY(statistics->RowsRead <= context->InputRowLimit);
            rows.resize(context->InputRowLimit - statistics->RowsRead);
            statistics->IncompleteInput = true;
            interrupt = true;
        }

        statistics->RowsRead += rows.size();

        statistics->DataWeightRead += rowSchemaInformation->RowWeightWithNoStrings * rows.size();
        i64 stringLikeColumnsDataWeight = 0;

        for (auto row : rows) {
            values.push_back(row.Begin());

            for (int index : rowSchemaInformation->StringLikeIndices) {
                stringLikeColumnsDataWeight += row[index].Type == EValueType::Null
                    ? 0
                    : row[index].Length;
            }
        }

        statistics->DataWeightRead += stringLikeColumnsDataWeight;

        if (auto* compartment = GetCurrentCompartment()) {
            auto copiedRangesGuard = CopyRowRangeIntoCompartment(
                values,
                stringLikeColumnsDataWeight,
                *rowSchemaInformation,
                compartment);
            auto copiedRangesPointersGuard = CopyIntoCompartment(
                TRange(std::bit_cast<uintptr_t*>(copiedRangesGuard.second.data()), copiedRangesGuard.second.size()),
                compartment);
            auto** valuesOffset = std::bit_cast<const TValue**>(copiedRangesPointersGuard.GetCopiedOffset());
            interrupt |= consumeRows(consumeRowsClosure, &scanContext, valuesOffset, values.size());
        } else {
            // TODO(dtorilov): This is a fix of YT-21907. Should use consumer with PI conversion here.
            for (auto row : rows) {
                for (int index : rowSchemaInformation->StringLikeIndices) {
                    MakePositionIndependentFromUnversioned(std::bit_cast<TPIValue*>(&row[index]), row[index]);
                }
            }

            interrupt |= consumeRows(consumeRowsClosure, &scanContext, values.data(), values.size());
        }

        yielder.Checkpoint(statistics->RowsRead);

        values.clear();
        scanContext.Clear();

        if (!context->IsMerge) {
            readOptions.MaxRowsPerRead = std::min(2 * readOptions.MaxRowsPerRead, RowsetProcessingBatchSize);
        }
    }
}

char* AllocateAlignedBytes(TExpressionContext* context, size_t byteCount)
{
    return context->AllocateAligned(byteCount, EAddressSpace::WebAssembly);
}

struct TSlot
{
    size_t Offset;
    size_t Count;
};

TPIValue* AllocateJoinKeys(
    TExecutionContext* /*context*/,
    TMultiJoinClosure* closure,
    TPIValue** keyPtrs)
{
    for (size_t joinId = 0; joinId < closure->Items.size(); ++joinId) {
        auto& joinItem = closure->Items[joinId];
        i64 length = GetUnversionedRowByteSize(joinItem.KeySize) + sizeof(TSlot);
        auto* offset = AllocateAlignedBytes(&joinItem.Context, length);
        auto* data = ConvertPointerFromWasmToHost(offset, length);
        auto row = TMutableRow::Create(data, joinItem.KeySize);
        auto* rowBeginOffset = ConvertPointerFromHostToWasm(row.Begin(), row.GetCount());
        ConvertPointerFromWasmToHost(keyPtrs)[joinId] = std::bit_cast<TPIValue*>(rowBeginOffset);
    }

    size_t primaryRowSize = closure->PrimaryRowSize * sizeof(TPIValue) + sizeof(TSlot*) * closure->Items.size();

    return reinterpret_cast<TPIValue*>(AllocateAlignedBytes(&closure->Context, primaryRowSize));
}

bool StorePrimaryRow(
    TExecutionContext* context,
    TMultiJoinClosure* closure,
    TPIValue** primaryValues,
    TPIValue** keysPtr)
{
    if (std::ssize(closure->PrimaryRows) >= context->JoinRowLimit) {
        throw TInterruptedIncompleteException();
    }

    closure->PrimaryRows.emplace_back(*ConvertPointerFromWasmToHost(primaryValues));

    for (size_t columnIndex = 0; columnIndex < closure->PrimaryRowSize; ++columnIndex) {
        CapturePIValue(
            &closure->Context,
            *ConvertPointerFromWasmToHost(primaryValues) + columnIndex,
            EAddressSpace::WebAssembly,
            EAddressSpace::WebAssembly);
    }

    for (size_t joinId = 0; joinId < closure->Items.size(); ++joinId) {
        auto& item = closure->Items[joinId];
        auto* key = ConvertPointerFromWasmToHost(keysPtr, closure->Items.size())[joinId];

        if (!item.LastKey || !item.PrefixEqComparer(key, item.LastKey)) {
            closure->ProcessSegment(joinId);
            item.LastKey = key;
            item.Lookup.clear();
            // Key will be reallocated further.
        }

        *std::bit_cast<TSlot*>(ConvertPointerFromWasmToHost(key) + item.KeySize) = TSlot{0, 0};

        auto inserted = item.Lookup.insert(key);
        if (inserted.second) {
            for (size_t columnIndex = 0; columnIndex < item.KeySize; ++columnIndex) {
                CapturePIValue(
                    &closure->Items[joinId].Context,
                    &key[columnIndex],
                    EAddressSpace::WebAssembly,
                    EAddressSpace::WebAssembly);
            }

            i64 length = GetUnversionedRowByteSize(item.KeySize) + sizeof(TSlot);
            auto* offset = AllocateAlignedBytes(&item.Context, length);
            auto* data = ConvertPointerFromWasmToHost(offset, length);
            auto row = TMutableRow::Create(data, item.KeySize);
            auto* rowBeginOffset = ConvertPointerFromHostToWasm(row.Begin(), row.GetCount());
            ConvertPointerFromWasmToHost(keysPtr, closure->Items.size())[joinId] = std::bit_cast<TPIValue*>(rowBeginOffset);
        }

        auto* insertedOffset = std::bit_cast<TSlot*>(*inserted.first + item.KeySize);
        auto** arrayOffset = std::bit_cast<TSlot**>(*ConvertPointerFromWasmToHost(primaryValues) + closure->PrimaryRowSize);
        auto** array = ConvertPointerFromWasmToHost(arrayOffset, closure->Items.size());
        array[joinId] = insertedOffset;
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
                &item.Context,
                GetUnversionedRowByteSize(item.KeySize) + sizeof(TSlot));
            auto* dataAtHost = ConvertPointerFromWasmToHost(data, GetUnversionedRowByteSize(item.KeySize) + sizeof(TSlot));
            auto row = TMutableRow::Create(dataAtHost, item.KeySize);
            auto* rowBeginOffset = ConvertPointerFromHostToWasm(row.Begin(), row.GetCount());
            ConvertPointerFromWasmToHost(keysPtr)[joinId] = std::bit_cast<TPIValue*>(rowBeginOffset);
        }
    }

    size_t primaryRowSize = closure->PrimaryRowSize * sizeof(TValue) + sizeof(TSlot*) * closure->Items.size();

    *ConvertPointerFromWasmToHost(primaryValues) = std::bit_cast<TPIValue*>(AllocateAlignedBytes(&closure->Context, primaryRowSize));

    return false;
}

namespace NDetail {

struct TJoinComparersCallbacks
{
    TCompartmentFunction<TComparerFunction> PrefixEqComparer;
    TCompartmentFunction<THasherFunction> SuffixHasher;
    TCompartmentFunction<TComparerFunction> SuffixEqComparer;
    TCompartmentFunction<TComparerFunction> SuffixLessComparer;
    TCompartmentFunction<TComparerFunction> ForeignPrefixEqComparer;
    TCompartmentFunction<TComparerFunction> ForeignSuffixLessComparer;
    TCompartmentFunction<TTernaryComparerFunction> FullTernaryComparer;
};

std::vector<TJoinComparersCallbacks> MakeJoinComparersCallbacks(TRange<TJoinComparers> comparers)
{
    std::vector<TJoinComparersCallbacks> result;
    result.reserve(comparers.Size());
    for (size_t joinId = 0; joinId < comparers.Size(); ++joinId) {
        result.push_back({
            PrepareFunction(comparers[joinId].PrefixEqComparer),
            PrepareFunction(comparers[joinId].SuffixHasher),
            PrepareFunction(comparers[joinId].SuffixEqComparer),
            PrepareFunction(comparers[joinId].SuffixLessComparer),
            PrepareFunction(comparers[joinId].ForeignPrefixEqComparer),
            PrepareFunction(comparers[joinId].ForeignSuffixLessComparer),
            PrepareFunction(comparers[joinId].FullTernaryComparer),
        });
    }

    return result;
}

} // namespace NDetail

void MultiJoinOpHelper(
    TExecutionContext* context,
    TMultiJoinParameters* parameters,
    TJoinComparers* comparersOffsets,
    void** collectRowsClosure,
    void (*collectRowsFunction)(
        void** closure,
        TMultiJoinClosure* joinClosure,
        TExpressionContext* context),
    void** consumeRowsClosure,
    TRowsConsumer consumeRowsFunction)
{
    auto comparers = NDetail::MakeJoinComparersCallbacks(
        TRange(
            ConvertPointerFromWasmToHost(comparersOffsets, parameters->Items.size()),
            parameters->Items.size()));
    auto collectRows = PrepareFunction(collectRowsFunction);
    auto consumeRows = PrepareFunction(consumeRowsFunction);

    auto finalLogger = Finally([&] {
        YT_LOG_DEBUG("Finalizing multijoin helper");
    });

    TMultiJoinClosure closure{
        .Context = MakeExpressionContext(TPermanentBufferTag(), context->MemoryChunkProvider),
    };

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

    bool finished = false;

    closure.ProcessJoinBatch = [&] {
        if (finished) {
            return true;
        }

        YT_LOG_DEBUG("Joining started");
        auto finalLogger = Finally([&] {
            YT_LOG_DEBUG("Joining finished");
        });

        auto foreignContext = MakeExpressionContext(
            TForeignExecutorBufferTag(),
            context->MemoryChunkProvider);

        TYielder yielder;

        std::vector<ISchemafulUnversionedReaderPtr> readers;
        for (size_t joinId = 0; joinId < closure.Items.size(); ++joinId) {
            closure.ProcessSegment(joinId);

            std::vector<TPIValueRange> orderedKeys;
            orderedKeys.reserve(closure.Items[joinId].OrderedKeys.size());
            for (auto* key : closure.Items[joinId].OrderedKeys) {
                // NB: Flags are neither set from TCG value nor cleared during row allocation.
                size_t id = 0;
                auto* items = ConvertPointerFromWasmToHost(key, closure.Items[joinId].KeySize);
                for (size_t index = 0; index < closure.Items[joinId].KeySize; ++index) {
                    auto& value = items[index];
                    value.Flags = {};
                    value.Id = id++;
                }
                auto row = TRow(ConvertPointerFromWasmToHost(std::bit_cast<const TUnversionedRowHeader*>(key) - 1));
                orderedKeys.emplace_back(key, row.GetCount());
            }

            yielder.Checkpoint(closure.Items[joinId].OrderedKeys.size());

            auto foreignExecutorCopy = CopyAndConvertFromPI(&foreignContext, orderedKeys, EAddressSpace::WebAssembly);
            SaveAndRestoreCurrentCompartment([&] {
                auto reader = parameters->Items[joinId].ExecuteForeign(
                    foreignExecutorCopy,
                    foreignContext.GetRowBuffer());
                readers.push_back(reader);
            });

            closure.Items[joinId].Lookup.clear();
            closure.Items[joinId].LastKey = nullptr;
        }

        std::vector<std::vector<TPIValue*>> sortedForeignSequences;
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

            std::vector<TPIValue*> sortedForeignSequence;
            size_t unsortedOffset = 0;
            TPIValue* lastForeignKey = nullptr;

            TRowBatchReadOptions readOptions{
                .MaxRowsPerRead = RowsetProcessingBatchSize
            };

            std::vector<TPIValue*> foreignValues;
            foreignValues.reserve(readOptions.MaxRowsPerRead);

            // Sort-merge join
            auto currentKey = orderedKeys.begin();

            auto processSortedForeignSequence = [&] {
                size_t index = 0;
                while (index != sortedForeignSequence.size() && currentKey != orderedKeys.end()) {
                    int cmpResult = fullTernaryComparer(*currentKey, sortedForeignSequence[index]);
                    if (cmpResult == 0) {
                        auto* slot = std::bit_cast<TSlot*>(ConvertPointerFromWasmToHost(*currentKey + keySize));
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
                    foreignBatch = SaveAndRestoreCurrentCompartment([&] {
                        return reader->Read(readOptions);
                    });
                    if (!foreignBatch) {
                        break;
                    }
                    // Materialize rows here.
                    foreignRows = foreignBatch->MaterializeRows();
                }

                if (foreignBatch->IsEmpty()) {
                    TValueIncrementingTimingGuard<TWallTimer> timingGuard(&context->Statistics->WaitOnReadyEventTime);
                    SaveAndRestoreCurrentCompartment([&] {
                        WaitFor(reader->GetReadyEvent())
                            .ThrowOnError();
                    });
                    continue;
                }

                for (auto row : foreignRows) {
                    auto captured = CaptureUnversionedValueRange(&closure.Context, row.Elements());
                    foreignValues.push_back(captured.Begin());
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

        auto intermediateContext = MakeExpressionContext(
            TIntermediateBufferTag(),
            context->MemoryChunkProvider);
        std::vector<const TPIValue*> joinedRows;

        i64 processedRows = 0;

        auto consumeJoinedRows = [&] () -> bool {
            // Consume joined rows.
            processedRows += joinedRows.size();

            bool finished = false;
            if (auto* compartment = GetCurrentCompartment()) {
                auto guard = CopyIntoCompartment(
                    TRange(std::bit_cast<uintptr_t*>(joinedRows.begin()), joinedRows.size()),
                    compartment);
                auto** offset = std::bit_cast<const TPIValue**>(guard.GetCopiedOffset());
                finished = consumeRows(consumeRowsClosure, &intermediateContext, offset, joinedRows.size());
            } else {
                finished = consumeRows(consumeRowsClosure, &intermediateContext, joinedRows.data(), joinedRows.size());
            }

            joinedRows.clear();
            intermediateContext.Clear();
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

        auto joinRow = [&] (TPIValue* rowValues) -> bool {
            size_t incrementIndex = 0;
            while (incrementIndex < closure.Items.size()) {
                auto joinedRow = AllocatePIValueRange(&intermediateContext, resultRowSize, EAddressSpace::WebAssembly);

                for (size_t index = 0; index < closure.PrimaryRowSize; ++index) {
                    CopyPositionIndependent(
                        &ConvertPointerFromWasmToHost(joinedRow.Begin())[index],
                        ConvertPointerFromWasmToHost(rowValues)[index]);
                }

                size_t offset = closure.PrimaryRowSize;
                for (size_t joinId = 0; joinId < closure.Items.size(); ++joinId) {
                    auto** arrayAtHost = ConvertPointerFromWasmToHost(
                        std::bit_cast<TSlot**>(rowValues + closure.PrimaryRowSize),
                        closure.Items.size());
                    auto* slotPointer = ConvertPointerFromWasmToHost(arrayAtHost[joinId]);
                    auto slot = *slotPointer;

                    const auto& foreignIndexes = parameters->Items[joinId].ForeignColumns;

                    if (slot.Count != 0) {
                        YT_VERIFY(indexes[joinId] < slot.Count);
                        TPIValue* foreignRow = sortedForeignSequences[joinId][slot.Offset + indexes[joinId]];

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
                            CopyPositionIndependent(
                                &ConvertPointerFromWasmToHost(joinedRow.Begin())[offset++],
                                *ConvertPointerFromWasmToHost(&foreignRow[columnIndex]));
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
                            MakePositionIndependentSentinelValue(
                                &ConvertPointerFromWasmToHost(joinedRow.Begin())[offset++],
                                EValueType::Null);
                        }
                    }
                }

                joinedRows.push_back(joinedRow.Begin());

                if (joinedRows.size() >= RowsetProcessingBatchSize) {
                    if (consumeJoinedRows()) {
                        return true;
                    }
                }
            }

            return false;
        };

        for (auto* rowValues : closure.PrimaryRows) {
            if (joinRow(rowValues)) {
                finished = true;
                break;
            }
        };

        if (!finished) {
            finished = consumeJoinedRows();
        }

        closure.PrimaryRows.clear();
        closure.Context.Clear();
        for (auto& joinItem : closure.Items) {
            joinItem.Context.Clear();
        }

        return finished;
    };

    try {
        // Collect join ids.
        collectRows(collectRowsClosure, &closure, &closure.Context);
    } catch (const TInterruptedIncompleteException&) {
        // Set incomplete and continue
        context->Statistics->IncompleteOutput = true;
    }

    closure.ProcessJoinBatch();
}

using TArrayJoinPredicate = bool (*)(void** closure, TExpressionContext*, const TPIValue* row);

bool ArrayJoinOpHelper(
    TExpressionContext* context,
    TArrayJoinParameters* parameters,
    TPIValue* row,
    TPIValue* arrays,
    void** consumeRowsClosure,
    TRowsConsumer consumeRowsFunction,
    void** predicateClosure,
    TArrayJoinPredicate predicateFunction)
{
    auto consumeRows = PrepareFunction(consumeRowsFunction);
    auto predicate = PrepareFunction(predicateFunction);

    int arrayCount = parameters->FlattenedTypes.size();
    std::vector<TPIValue*> nestedRows;

    for (int index = 0; index < arrayCount; ++index) {
        auto* arrayAtHost = ConvertPointerFromWasmToHost(&arrays[index]);

        if (arrayAtHost->Type == EValueType::Null) {
            continue;
        }

        TMemoryInput memoryInput;
        TString buffer;
        if (HasCurrentCompartment()) {
            buffer = arrayAtHost->AsStringBuf();
            memoryInput = TMemoryInput(TStringBuf(buffer));
        } else {
            memoryInput = TMemoryInput(FromPositionIndependentValue<NYson::TYsonStringBuf>(*arrayAtHost).AsStringBuf());
        }

        auto parser = TYsonPullParser(&memoryInput, EYsonType::Node);
        auto cursor = TYsonPullParserCursor(&parser);

        auto listItemType = parameters->FlattenedTypes[index];
        TPIValue parsedValue;
        int currentArrayIndex = 0;

        cursor.ParseList([&] (TYsonPullParserCursor* cursor) {
            auto currentType = cursor->GetCurrent().GetType();
            switch (currentType) {
                case EYsonItemType::EntityValue: {
                    MakePositionIndependentNullValue(&parsedValue);
                    break;
                }
                case EYsonItemType::Int64Value: {
                    THROW_ERROR_EXCEPTION_IF(
                        listItemType != EValueType::Int64,
                        "Type mismatch in array join");

                    auto value = cursor->GetCurrent().UncheckedAsInt64();
                    MakePositionIndependentInt64Value(&parsedValue, value);
                    break;
                }
                case EYsonItemType::Uint64Value: {
                    THROW_ERROR_EXCEPTION_IF(
                        listItemType != EValueType::Uint64,
                        "Type mismatch in array join");

                    auto value = cursor->GetCurrent().UncheckedAsUint64();
                    MakePositionIndependentUint64Value(&parsedValue, value);
                    break;
                }
                case EYsonItemType::DoubleValue: {
                    THROW_ERROR_EXCEPTION_IF(
                        listItemType != EValueType::Double,
                        "Type mismatch in array join");

                    auto value = cursor->GetCurrent().UncheckedAsDouble();
                    MakePositionIndependentDoubleValue(&parsedValue, value);
                    break;
                }
                case EYsonItemType::StringValue: {
                    THROW_ERROR_EXCEPTION_IF(
                        listItemType != EValueType::String,
                        "Type mismatch in array join");

                    auto value = cursor->GetCurrent().UncheckedAsString();
                    MakePositionIndependentStringValue(&parsedValue, value);
                    break;
                }
                default:
                    THROW_ERROR_EXCEPTION("Type mismatch in array join");
            }

            if (currentArrayIndex >= std::ssize(nestedRows)) {
                auto mutableRange = AllocatePIValueRange(context, arrayCount, EAddressSpace::WebAssembly);
                for (int leadingRowIndex = 0; leadingRowIndex < index; ++leadingRowIndex) {
                    MakePositionIndependentNullValue(ConvertPointerFromWasmToHost(&mutableRange[leadingRowIndex]));
                }
                nestedRows.push_back(mutableRange.Begin());
            }

            CopyPositionIndependent(
                ConvertPointerFromWasmToHost(&nestedRows[currentArrayIndex][index]),
                parsedValue);
            currentArrayIndex++;
            cursor->Next();
        });

        for (int trailingIndex = currentArrayIndex; trailingIndex < std::ssize(nestedRows); ++trailingIndex) {
            MakePositionIndependentNullValue(ConvertPointerFromWasmToHost(&nestedRows[trailingIndex][index]));
        }
    }

    int valueCount = parameters->SelfJoinedColumns.size() + parameters->ArrayJoinedColumns.size();
    auto filteredRows = std::vector<const TPIValue*>();

    for (const auto* nestedRow : nestedRows) {
        if (predicate(predicateClosure, context, nestedRow)) {
            int joinedRowIndex = 0;
            auto joinedRow = AllocatePIValueRange(context, valueCount, EAddressSpace::WebAssembly);

            for (int index : parameters->SelfJoinedColumns) {
                CopyPositionIndependent(
                    ConvertPointerFromWasmToHost(&joinedRow[joinedRowIndex++]),
                    *ConvertPointerFromWasmToHost(&row[index]));
            }

            for (int index : parameters->ArrayJoinedColumns) {
                CopyPositionIndependent(
                    ConvertPointerFromWasmToHost(&joinedRow[joinedRowIndex++]),
                    *ConvertPointerFromWasmToHost(&nestedRow[index]));
            }

            filteredRows.push_back(joinedRow.Begin());
        }
    }

    if (parameters->IsLeft && filteredRows.empty()) {
        int joinedRowIndex = 0;
        auto joinedRow = AllocatePIValueRange(context, valueCount, EAddressSpace::WebAssembly);

        for (int index : parameters->SelfJoinedColumns) {
            CopyPositionIndependent(
                ConvertPointerFromWasmToHost(&joinedRow[joinedRowIndex++]),
                *ConvertPointerFromWasmToHost(&row[index]));
        }

        for (int index : parameters->ArrayJoinedColumns) {
            Y_UNUSED(index);
            MakePositionIndependentNullValue(ConvertPointerFromWasmToHost(&joinedRow[joinedRowIndex++]));
        }

        filteredRows.push_back(joinedRow.Begin());
    }

    if (auto* compartment = GetCurrentCompartment()) {
        auto guard = CopyIntoCompartment(
            TRange(std::bit_cast<uintptr_t*>(filteredRows.data()), std::ssize(filteredRows)),
            compartment);
        auto** begin = std::bit_cast<const TPIValue**>(guard.GetCopiedOffset());
        return consumeRows(consumeRowsClosure, context, begin, filteredRows.size());
    }

    return consumeRows(consumeRowsClosure, context, filteredRows.data(), filteredRows.size());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoutines

////////////////////////////////////////////////////////////////////////////////

using TWebAssemblyRowsConsumer = NWebAssembly::TCompartmentFunction<bool(void**, TExpressionContext*, const TPIValue**, i64)>;

DEFINE_ENUM(EGroupOpProcessingStage,
    ((LeftBorder)  (0))
    ((Inner)       (1))
    ((RightBorder) (2))
);

class TGroupByClosure
{
public:
    TGroupByClosure(
        IMemoryChunkProviderPtr chunkProvider,
        NWebAssembly::TCompartmentFunction<TComparerFunction> prefixEqComparer,
        NWebAssembly::TCompartmentFunction<THasherFunction> groupHasher,
        NWebAssembly::TCompartmentFunction<TComparerFunction> groupComparer,
        int groupKeySize,
        int groupStateSize,
        bool combineGroupOpWithOrderOp,
        int orderKeySize,
        i64 orderOpLimit,
        NWebAssembly::TCompartmentFunction<TComparerFunction> orderOpComparer,
        bool shouldCheckForNullGroupKey,
        bool allAggregatesAreFirst,
        void** consumeIntermediateClosure,
        TWebAssemblyRowsConsumer consumeIntermediate,
        void** consumeAggregatedClosure,
        TWebAssemblyRowsConsumer consumeAggregated,
        void** consumeDeltaClosure,
        TWebAssemblyRowsConsumer consumeDelta,
        void** consumeTotalsClosure,
        TWebAssemblyRowsConsumer consumeTotals);

    // If the stream tag has changed, we flush to keep the segments sorted by primary key.
    Y_FORCE_INLINE void UpdateTagAndFlushIfNeeded(const TExecutionContext* context, EStreamTag tag);

    // The grouping key cannot be null if `with totals` is used,
    // since the result of `totals` is contained in a row with a null group key.
    Y_FORCE_INLINE void ValidateGroupKeyIsNotNull(TPIValue* row) const;

    // If the common prefix of primary key and group key has changed,
    // no new lines can be added to the grouping. Thus, we can flush.
    Y_FORCE_INLINE void FlushIntermediatesIfCurrentGroupSetIsFinished(const TExecutionContext* context, TPIValue* row);

    // If the request exceeds the given memory limit, we interrupt processing.
    // NB: We do not guarantee the correctness of the result.
    Y_FORCE_INLINE void ValidateGroupedRowCount(i64 groupRowLimit) const;

    // True when the common prefix of the grouping key and the primary key has changed.
    Y_FORCE_INLINE bool IsCurrentGroupSetFinished(TPIValue* row) const;

    // If current group set is finished and, we can stop the query execution when the row limit is reached.
    Y_FORCE_INLINE bool CanEarlyStopProcessing(const TExecutionContext* context, TPIValue* row) const;

    // If all aggregate functions are `first()`, we can stop the query execution when the row limit is reached.
    Y_FORCE_INLINE bool AreAllAggregatesFirst() const;

    // Returns grouped row in the lookup table.
    Y_FORCE_INLINE const TPIValue* InsertIntermediate(const TExecutionContext* context, TPIValue* row);

    // Returns row itself.
    Y_FORCE_INLINE const TPIValue* InsertAggregated(const TExecutionContext* context, TPIValue* row);

    // Returns row itself.
    Y_FORCE_INLINE const TPIValue* InsertTotals(const TExecutionContext* context, TPIValue* row);

    // Flushes grouped rows.
    Y_FORCE_INLINE void Flush(const TExecutionContext* context, EStreamTag incomingTag);

    // Returns context that holds grouped rows.
    Y_FORCE_INLINE TExpressionContext* GetContext();

    // Number of processed rows for logging.
    Y_FORCE_INLINE i64 GetProcessedRowCount() const;

    // Notifies that the next flush is the last flush.
    Y_FORCE_INLINE void SetClosingSegment();

    // At the end of the group operation all rows should be flushed.
    Y_FORCE_INLINE bool IsFlushed() const;

    // We combine grouping and ordering if query has `ORDER BY` and sorting key does not contain aggregates.
    Y_FORCE_INLINE bool IsCombinedWithOrderOp() const;

    // Returns tagged pointer when incoming row is inserted and is new.
    Y_FORCE_INLINE const TPIValue* InsertIntermediateWhenCombinedWithOrderOp(TPIValue* row);

    i64 GetGroupedRowCount() const;

private:
    TExpressionContext Context_;
    TExpressionContext AggregatedContext_;
    TExpressionContext TotalsContext_;

    // The grouping key and the primary key may have a common prefix of length P.
    // This function compares prefixes of length P.
    const NWebAssembly::TCompartmentFunction<TComparerFunction> PrefixEqComparer_;

    TLookupRows GroupedIntermediateRows_;
    const int GroupKeySize_;
    const int GroupStateSize_;
    const int OrderKeySize_;

    std::unique_ptr<TTopCollector> TopCollector_ = nullptr;

    // When executing a query with `with totals`, we store data for `totals` using the null grouping key.
    // Thus, rows with a null grouping key should lead to an execution error.
    const bool ShouldCheckForNullGroupKey_;

    // If all aggregate functions are `first()`, we can stop the query execution when the limit is reached.
    const bool AllAggregatesAreFirst_;

    void** const ConsumeIntermediateClosure_;
    const TWebAssemblyRowsConsumer ConsumeIntermediate_;

    void** const ConsumeAggregatedClosure_;
    const TWebAssemblyRowsConsumer ConsumeAggregated_;

    void** const ConsumeDeltaClosure_;
    const TWebAssemblyRowsConsumer ConsumeDelta_;

    void** const ConsumeTotalsClosure_;
    const TWebAssemblyRowsConsumer ConsumeTotals_;

    const TPIValue* LastKey_ = nullptr;
    std::vector<const TPIValue*> Intermediate_;
    std::vector<const TPIValue*> Aggregated_;
    std::vector<const TPIValue*> Totals_;

    // Defines the stage of the stream processing.
    EGroupOpProcessingStage CurrentSegment_ = EGroupOpProcessingStage::LeftBorder;

    // Grouped rows can be flushed and cleared during aggregation.
    // So we have to count grouped rows separately.
    i64 GroupedRowCount_ = 0;

    EStreamTag LastTag_ = EStreamTag::Intermediate;

    // We perform flushes (and yields) during grouping.
    NRoutines::TYielder Yielder_{};
    i64 ProcessedRows_ = 0;
    TExpressionContext FlushContext_;

    template <typename TFlushFunction>
    Y_FORCE_INLINE void FlushWithBatching(
        const TExecutionContext* context,
        const TPIValue** begin,
        const TPIValue** end,
        const TFlushFunction& flush);

    Y_FORCE_INLINE void FlushIntermediate(const TExecutionContext* context, const TPIValue** begin, const TPIValue** end);
    Y_FORCE_INLINE void FlushAggregated(const TExecutionContext* context, const TPIValue** begin, const TPIValue** end);
    Y_FORCE_INLINE void FlushDelta(const TExecutionContext* context, const TPIValue** begin, const TPIValue** end);
    Y_FORCE_INLINE void FlushTotals(const TExecutionContext* context, const TPIValue** begin, const TPIValue** end);
};

TGroupByClosure::TGroupByClosure(
    IMemoryChunkProviderPtr chunkProvider,
    NWebAssembly::TCompartmentFunction<TComparerFunction> prefixEqComparer,
    NWebAssembly::TCompartmentFunction<THasherFunction> groupHasher,
    NWebAssembly::TCompartmentFunction<TComparerFunction> groupComparer,
    int groupKeySize,
    int groupStateSize,
    bool combineGroupOpWithOrderOp,
    int orderKeySize,
    i64 orderOpLimit,
    NWebAssembly::TCompartmentFunction<TComparerFunction> orderOpComparer,
    bool shouldCheckForNullGroupKey,
    bool allAggregatesAreFirst,
    void** consumeIntermediateClosure,
    TWebAssemblyRowsConsumer consumeIntermediate,
    void** consumeAggregatedClosure,
    TWebAssemblyRowsConsumer consumeAggregated,
    void** consumeDeltaClosure,
    TWebAssemblyRowsConsumer consumeDelta,
    void** consumeTotalsClosure,
    TWebAssemblyRowsConsumer consumeTotals)
    : Context_(MakeExpressionContext(TPermanentBufferTag(), chunkProvider))
    , AggregatedContext_(MakeExpressionContext(TPermanentBufferTag(), chunkProvider))
    , TotalsContext_(MakeExpressionContext(TPermanentBufferTag(), chunkProvider))
    , PrefixEqComparer_(prefixEqComparer)
    , GroupedIntermediateRows_(
        InitialGroupOpHashtableCapacity,
        groupHasher,
        groupComparer)
    , GroupKeySize_(groupKeySize)
    , GroupStateSize_(groupStateSize)
    , OrderKeySize_(orderKeySize)
    , ShouldCheckForNullGroupKey_(shouldCheckForNullGroupKey)
    , AllAggregatesAreFirst_(allAggregatesAreFirst)
    , ConsumeIntermediateClosure_(consumeIntermediateClosure)
    , ConsumeIntermediate_(consumeIntermediate)
    , ConsumeAggregatedClosure_(consumeAggregatedClosure)
    , ConsumeAggregated_(consumeAggregated)
    , ConsumeDeltaClosure_(consumeDeltaClosure)
    , ConsumeDelta_(consumeDelta)
    , ConsumeTotalsClosure_(consumeTotalsClosure)
    , ConsumeTotals_(consumeTotals)
    , FlushContext_(MakeExpressionContext(TIntermediateBufferTag(), chunkProvider))
{
    GroupedIntermediateRows_.set_empty_key(
        NDetail::TRowComparer::MakeSentinel(NDetail::TRowComparer::ESentinelType::Empty));

    GroupedIntermediateRows_.set_deleted_key(
        NDetail::TRowComparer::MakeSentinel(NDetail::TRowComparer::ESentinelType::Deleted));

    if (combineGroupOpWithOrderOp) {
        TopCollector_ = std::make_unique<TTopCollector>(
            orderOpLimit,
            orderOpComparer,
            GroupKeySize_ + GroupStateSize_ + OrderKeySize_,
            chunkProvider);
    }
}

void TGroupByClosure::UpdateTagAndFlushIfNeeded(const TExecutionContext* context, EStreamTag tag)
{
    if (tag != LastTag_) {
        if (context->Ordered) {
            Flush(context, tag);
        }
    }
    LastTag_ = tag;
}

void TGroupByClosure::ValidateGroupKeyIsNotNull(TPIValue* row) const
{
    if (!ShouldCheckForNullGroupKey_) {
        return;
    }

    row = ConvertPointerFromWasmToHost(row, GroupKeySize_);

    if (std::all_of(
        &row[0],
        &row[GroupKeySize_],
        [] (const TPIValue& value) {
            return value.Type == EValueType::Null;
        }))
    {
        THROW_ERROR_EXCEPTION("Null values are forbidden in group key");
    }
}

void TGroupByClosure::FlushIntermediatesIfCurrentGroupSetIsFinished(const TExecutionContext* context, TPIValue* row)
{
    // NB: If !context->Ordered then PrefixEqComparer_ never lets flush.
    if (IsCurrentGroupSetFinished(row)) {
        Flush(context, EStreamTag::Intermediate);
    }
}

void TGroupByClosure::ValidateGroupedRowCount(i64 groupRowLimit) const
{
    if (std::ssize(Intermediate_) == groupRowLimit) {
        throw TInterruptedIncompleteException();
    }
}

bool TGroupByClosure::IsCurrentGroupSetFinished(TPIValue* row) const
{
    return LastKey_ && !PrefixEqComparer_(row, LastKey_);
}

bool TGroupByClosure::CanEarlyStopProcessing(const TExecutionContext* context, TPIValue* row) const
{
    // NB: We do not support `having` with `limit` yet.
    // If query uses `having`, skipping rows here is incorrect because `having` filters rows.

    // NB: Our semantics of `totals` operation allows to stop grouping when the limit is reached.

    return
        context->Ordered &&
        (GroupedRowCount_ >= context->Offset + context->Limit) &&
        (IsCurrentGroupSetFinished(row) || AllAggregatesAreFirst_);
}

bool TGroupByClosure::AreAllAggregatesFirst() const
{
    return AllAggregatesAreFirst_;
}

const TPIValue* TGroupByClosure::InsertIntermediate(const TExecutionContext* context, TPIValue* row)
{
    auto [it, inserted] = GroupedIntermediateRows_.insert(row);

    if (inserted) {
        YT_ASSERT(*it == row);

        LastKey_ = *it;

        Intermediate_.push_back(row);
        ++GroupedRowCount_;
        YT_VERIFY(std::ssize(Intermediate_) <= context->GroupRowLimit);

        for (int index = 0; index < GroupKeySize_; ++index) {
            CapturePIValue(&Context_, &row[index], EAddressSpace::WebAssembly, EAddressSpace::WebAssembly);
        }
    }

    return *it;
}

const TPIValue* TGroupByClosure::InsertAggregated(const TExecutionContext* /*context*/, TPIValue* row)
{
    LastKey_ = row;

    Aggregated_.push_back(row);
    ++GroupedRowCount_;

    for (int index = 0; index < GroupKeySize_ + GroupStateSize_; ++index) {
        CapturePIValue(&AggregatedContext_, &row[index], EAddressSpace::WebAssembly, EAddressSpace::WebAssembly);
    }

    return row;
}

const TPIValue* TGroupByClosure::InsertTotals(const TExecutionContext* /*context*/, TPIValue* row)
{
    // |LastKey_| should not be updated because bordering intermediate streams should be merged.

    Totals_.push_back(row);

    for (int index = 0; index < GroupKeySize_ + GroupStateSize_; ++index) { // FIXME: first values are null btw.
        CapturePIValue(&TotalsContext_, &row[index], EAddressSpace::WebAssembly, EAddressSpace::WebAssembly);
    }

    return row;
}

void TGroupByClosure::Flush(const TExecutionContext* context, EStreamTag incomingTag)
{
    if (Y_UNLIKELY(IsCombinedWithOrderOp())) {
        YT_VERIFY(CurrentSegment_ == EGroupOpProcessingStage::RightBorder);
        auto rows = TopCollector_->GetRows();
        auto begin = rows.data() + std::min(context->Offset, std::ssize(rows));
        auto end = rows.data() + std::min(context->Offset + context->Limit, std::ssize(rows));
        FlushIntermediate(context, begin, end);
        return;
    }

    if (Y_UNLIKELY(CurrentSegment_ == EGroupOpProcessingStage::RightBorder)) {
        if (!Aggregated_.empty()) {
            FlushAggregated(context, Aggregated_.data(), Aggregated_.data() + Aggregated_.size());
            Aggregated_.clear();
        }

        if (!Intermediate_.empty()) {
            // Can be non-null in last call.
            i64 innerCount = Intermediate_.size() - GroupedIntermediateRows_.size();

            FlushDelta(context, Intermediate_.data(), Intermediate_.data() + innerCount);
            FlushIntermediate(context, Intermediate_.data() + innerCount, Intermediate_.data() + Intermediate_.size());

            Intermediate_.clear();
            GroupedIntermediateRows_.clear();
        }

        if (!Totals_.empty()) {
            FlushTotals(context, Totals_.data(), Totals_.data() + Totals_.size());
            Totals_.clear();
        }

        return;
    }

    switch (LastTag_) {
        case EStreamTag::Aggregated: {
            FlushAggregated(context, Aggregated_.data(), Aggregated_.data() + Aggregated_.size());
            Aggregated_.clear();
            break;
        }

        case EStreamTag::Intermediate: {
            if (Y_UNLIKELY(incomingTag == EStreamTag::Totals)) {
                // Do nothing since totals can be followed with intermediate that should be grouped with current.
                break;
            }

            if (Y_UNLIKELY(CurrentSegment_ == EGroupOpProcessingStage::LeftBorder)) {
                FlushIntermediate(context, Intermediate_.data(), Intermediate_.data() + Intermediate_.size());
                Intermediate_.clear();
            } else if (Y_UNLIKELY(Intermediate_.size() >= RowsetProcessingBatchSize)) {
                // When group key contains full primary key (used with joins), flush will be called on each grouped row.
                // Thus, we batch calls to Flusher.
                FlushDelta(context, Intermediate_.data(), Intermediate_.data() + Intermediate_.size());
                Intermediate_.clear();
            } else if (Y_UNLIKELY(incomingTag == EStreamTag::Aggregated)) {
                FlushDelta(context, Intermediate_.data(), Intermediate_.data() + Intermediate_.size());
                Intermediate_.clear();
            }

            GroupedIntermediateRows_.clear();
            // Otherwise, do nothing. Grouped rows will be flushed later.
            break;
        }

        case EStreamTag::Totals: {
            FlushTotals(context, Totals_.data(), Totals_.data() + Totals_.size());
            Totals_.clear();
            break;
        }

        default: {
            YT_ABORT();
        }
    }

    CurrentSegment_ = EGroupOpProcessingStage::Inner;
}

TExpressionContext* TGroupByClosure::GetContext()
{
    return &Context_;
}

i64 TGroupByClosure::GetProcessedRowCount() const
{
    return ProcessedRows_;
}

void TGroupByClosure::SetClosingSegment()
{
    CurrentSegment_ = EGroupOpProcessingStage::RightBorder;
}

bool TGroupByClosure::IsFlushed() const
{
    return Intermediate_.empty() && Aggregated_.empty() && Totals_.empty();
}

bool TGroupByClosure::IsCombinedWithOrderOp() const
{
    return TopCollector_ != nullptr;
}

const TPIValue* TGroupByClosure::InsertIntermediateWhenCombinedWithOrderOp(TPIValue* row)
{
    auto it = GroupedIntermediateRows_.find(row);
    if (it != GroupedIntermediateRows_.end()) {
        return *it;
    }

    auto* inserted = TopCollector_->AddRow(
        row,
        [&] (const TPIValue* insertedRow) {
            GroupedIntermediateRows_.insert(insertedRow);
        },
        [&] (const TPIValue* evictedRow) {
            GroupedIntermediateRows_.erase(evictedRow);
        });

    if (inserted) {
        ++GroupedRowCount_;
        return std::bit_cast<const TPIValue*>(std::bit_cast<ui64>(inserted) | (1ULL << 48));
    }

    return nullptr;
}

i64 TGroupByClosure::GetGroupedRowCount() const
{
    return GroupedRowCount_;
}

template <typename TFlushFunction>
void TGroupByClosure::FlushWithBatching(
    const TExecutionContext* /*context*/,
    const TPIValue** begin,
    const TPIValue** end,
    const TFlushFunction& flush)
{
    auto guard = TCopyGuard();
    if (auto* compartment = GetCurrentCompartment()) {
        i64 length = end - begin;
        guard = CopyIntoCompartment(TRange(std::bit_cast<uintptr_t*>(begin), length), compartment);
        begin = std::bit_cast<const TPIValue**>(guard.GetCopiedOffset());
        end = begin + length;
    }

    bool finished = false;

    // We cannot skip intermediate rows for ordered queries
    // (when the grouping key is a prefix of primary key),
    // since intermediate rows from the beginning of the processed range
    // can be grouped with rows of the previous range.

    while (!finished && begin < end) {
        i64 size = std::min(begin + RowsetProcessingBatchSize, end) - begin;
        ProcessedRows_ += size;
        finished = flush(&FlushContext_, begin, size);
        FlushContext_.Clear();
        Yielder_.Checkpoint(ProcessedRows_);
        begin += size;
    }
}

void TGroupByClosure::FlushIntermediate(const TExecutionContext* context, const TPIValue** begin, const TPIValue** end)
{
    auto flush = [this] (TExpressionContext* context, const TPIValue** begin, i64 size) {
        return ConsumeIntermediate_(ConsumeIntermediateClosure_, context, begin, size);
    };

    FlushWithBatching(context, begin, end, flush);
}

void TGroupByClosure::FlushAggregated(const TExecutionContext* context, const TPIValue** begin, const TPIValue** end)
{
    auto flush = [this] (TExpressionContext* context, const TPIValue** begin, i64 size) {
        return ConsumeAggregated_(ConsumeAggregatedClosure_, context, begin, size);
    };

    FlushWithBatching(context, begin, end, flush);
}

void TGroupByClosure::FlushDelta(const TExecutionContext* context, const TPIValue** begin, const TPIValue** end)
{
    auto flush = [this] (TExpressionContext* context, const TPIValue** begin, i64 size) {
        return ConsumeDelta_(ConsumeDeltaClosure_, context, begin, size);
    };

    FlushWithBatching(context, begin, end, flush);
}

void TGroupByClosure::FlushTotals(const TExecutionContext* context, const TPIValue** begin, const TPIValue** end)
{
    auto flush = [this] (TExpressionContext* context, const TPIValue** begin, i64 size) {
        return ConsumeTotals_(ConsumeTotalsClosure_, context, begin, size);
    };

    FlushWithBatching(context, begin, end, flush);
}

////////////////////////////////////////////////////////////////////////////////

namespace NRoutines {

////////////////////////////////////////////////////////////////////////////////

// Returns nullptr when no more rows are needed.
// Returns pointer different to |row| if incoming row is intermediate and should be updated.
// Returns tagged pointer if group op is combined with order op and the incoming row is inserted and is new.
const TPIValue* InsertGroupRow(
    TExecutionContext* context,
    TGroupByClosure* closure,
    TPIValue* row,
    ui64 rowTagAsUint)
{
    auto rowTag = static_cast<EStreamTag>(rowTagAsUint);

    if (closure->IsCombinedWithOrderOp()) {
        YT_ASSERT(rowTag == EStreamTag::Intermediate);
        // We intentionally do not call ValidateGroupedRowCount here since the row count is bounded by `LIMIT`.
        // NB(dtorilov): Here we can early stop processing for the NodeThread execution level.
        closure->ValidateGroupKeyIsNotNull(row);
        return closure->InsertIntermediateWhenCombinedWithOrderOp(row);
    }

    closure->UpdateTagAndFlushIfNeeded(context, rowTag);

    switch (rowTag) {
        case EStreamTag::Aggregated: {
            if (closure->CanEarlyStopProcessing(context, row)) {
                return nullptr;
            }

            closure->InsertAggregated(context, row);
            return row;
        }

        case EStreamTag::Intermediate: {
            closure->FlushIntermediatesIfCurrentGroupSetIsFinished(context, row);

            closure->ValidateGroupedRowCount(context->GroupRowLimit);

            if (closure->CanEarlyStopProcessing(context, row)) {
                return nullptr;
            }

            closure->ValidateGroupKeyIsNotNull(row);

            return closure->InsertIntermediate(context, row);
        }

        case EStreamTag::Totals: {
            closure->InsertTotals(context, row);
            return row;
        }

        default: {
            YT_ABORT();
        }
    }
}

using TGroupCollector = void(*)(void** closure, TGroupByClosure* groupByClosure, TExpressionContext* context);

void GroupOpHelper(
    TExecutionContext* context,
    TComparerFunction* prefixEqComparerFunction,
    THasherFunction* groupHasherFunction,
    TComparerFunction* groupComparerFunction,
    int groupKeySize,
    int groupStateSize,
    bool combineGroupOpWithOrderOp,
    int orderKeySize,
    TComparerFunction* orderOpComparerFunction,
    bool shouldCheckForNullGroupKey,
    bool allAggregatesAreFirst,
    void** collectRowsClosure,
    TGroupCollector collectRowsFunction,
    void** consumeIntermediateClosure,
    TRowsConsumer consumeIntermediateFunction,
    void** consumeAggregatedClosure,
    TRowsConsumer consumeAggregatedFunction,
    void** consumeDeltaClosure,
    TRowsConsumer consumeDeltaFunction,
    void** consumeTotalsClosure,
    TRowsConsumer consumeTotalsFunction,
    void** compatConsumeIntermediateClosure,
    TRowsConsumer compatConsumeIntermediateFunction,
    void** compatConsumeAggregatedClosure,
    TRowsConsumer compatConsumeAggregatedFunction,
    void** compatConsumeDeltaClosure,
    TRowsConsumer compatConsumeDeltaFunction,
    void** compatConsumeTotalsClosure,
    TRowsConsumer compatConsumeTotalsFunction)
{
    auto collectRows = PrepareFunction(collectRowsFunction);
    auto prefixEqComparer = PrepareFunction(prefixEqComparerFunction);
    auto groupHasher = PrepareFunction(groupHasherFunction);
    auto groupComparer = PrepareFunction(groupComparerFunction);

    auto orderOpComparer = PrepareFunction(orderOpComparerFunction);

    auto consumeIntermediate = PrepareFunction(consumeIntermediateFunction);
    auto consumeAggregated = PrepareFunction(consumeAggregatedFunction);
    auto consumeDelta = PrepareFunction(consumeDeltaFunction);
    auto consumeTotals = PrepareFunction(consumeTotalsFunction);

    auto compatConsumeIntermediate = PrepareFunction(compatConsumeIntermediateFunction);
    auto compatConsumeAggregated = PrepareFunction(compatConsumeAggregatedFunction);
    auto compatConsumeDelta = PrepareFunction(compatConsumeDeltaFunction);
    auto compatConsumeTotals = PrepareFunction(compatConsumeTotalsFunction);

    auto responseFeatureFlags = SaveAndRestoreCurrentCompartment([&] {
        return WaitForFast(context->ResponseFeatureFlags)
            .ValueOrThrow();
    });

    bool groupByInCompatMode = (!context->RequestFeatureFlags->WithTotalsFinalizesAggregatedOnCoordinator) ||
        (!responseFeatureFlags.WithTotalsFinalizesAggregatedOnCoordinator);

    auto closure = TGroupByClosure(
        context->MemoryChunkProvider,
        prefixEqComparer,
        groupHasher,
        groupComparer,
        groupKeySize,
        groupStateSize,
        combineGroupOpWithOrderOp,
        orderKeySize,
        context->Offset + context->Limit,
        orderOpComparer,
        shouldCheckForNullGroupKey,
        allAggregatesAreFirst,
        groupByInCompatMode ? compatConsumeIntermediateClosure : consumeIntermediateClosure,
        groupByInCompatMode ? compatConsumeIntermediate : consumeIntermediate,
        groupByInCompatMode ? compatConsumeAggregatedClosure : consumeAggregatedClosure,
        groupByInCompatMode ? compatConsumeAggregated : consumeAggregated,
        groupByInCompatMode ? compatConsumeDeltaClosure : consumeDeltaClosure,
        groupByInCompatMode ? compatConsumeDelta : consumeDelta,
        groupByInCompatMode ? compatConsumeTotalsClosure : consumeTotalsClosure,
        groupByInCompatMode ? compatConsumeTotals : consumeTotals);

    auto finalLogger = Finally([&] {
        YT_LOG_DEBUG("Finalizing group helper (ProcessedRows: %v)", closure.GetProcessedRowCount());
    });

    try {
        collectRows(collectRowsClosure, &closure, closure.GetContext());
    } catch (const TInterruptedIncompleteException&) {
        // Set incomplete and continue
        context->Statistics->IncompleteOutput = true;

        // TODO(dtorilov): Since the request processing has exceeded the given limits, just exit here.
    }

    closure.SetClosingSegment();

    closure.Flush(context, EStreamTag::Totals); // Dummy tag.

    context->Statistics->TotalGroupedRowCount += closure.GetGroupedRowCount();

    YT_VERIFY(closure.IsFlushed());
}

////////////////////////////////////////////////////////////////////////////////

using TGroupTotalsCollector = void(*)(void** closure, TExpressionContext* context);

void GroupTotalsOpHelper(
    TExecutionContext* context,
    void** collectRowsClosure,
    TGroupTotalsCollector collectRowsFunction)
{
    auto expressionContext = MakeExpressionContext(TIntermediateBufferTag(), context->MemoryChunkProvider);
    auto collectRows = PrepareFunction(collectRowsFunction);
    collectRows(collectRowsClosure, &expressionContext);
}

void AllocatePermanentRow(
    TExecutionContext* /*executionContext*/,
    TExpressionContext* expressionContext,
    int valueCount,
    TValue** row)
{
    // TODO(dtorilov): Use AllocateUnversioned.
    auto* offset = expressionContext->AllocateAligned(valueCount * sizeof(TPIValue), EAddressSpace::WebAssembly);
    *ConvertPointerFromWasmToHost(row) = std::bit_cast<TValue*>(offset);
}

void AddRowToCollector(TTopCollector* topCollector, TPIValue* row)
{
    topCollector->AddRow(row);
}

void OrderOpHelper(
    TExecutionContext* context,
    TComparerFunction* comparerFunction,
    void** collectRowsClosure,
    void (*collectRowsFunction)(void** closure, TTopCollector* topCollector),
    void** consumeRowsClosure,
    TRowsConsumer consumeRowsFunction,
    size_t rowSize)
{
    auto comparer = PrepareFunction(comparerFunction);
    auto collectRows = PrepareFunction(collectRowsFunction);
    auto consumeRows = PrepareFunction(consumeRowsFunction);

    auto finalLogger = Finally([&] {
        YT_LOG_DEBUG("Finalizing order helper");
    });

    auto topCollector = TTopCollector(
        context->Offset + context->Limit,
        comparer,
        rowSize,
        context->MemoryChunkProvider);
    collectRows(collectRowsClosure, &topCollector);
    auto rows = topCollector.GetRows();

    auto consumerContext = MakeExpressionContext(TIntermediateBufferTag(), context->MemoryChunkProvider);

    TYielder yielder;
    size_t processedRows = 0;

    auto guard = TCopyGuard();
    auto** begin = rows.data();
    if (auto* compartment = GetCurrentCompartment()) {
        guard = CopyIntoCompartment(TRange(std::bit_cast<uintptr_t*>(rows.data()), std::ssize(rows)), compartment);
        begin = std::bit_cast<const TPIValue**>(guard.GetCopiedOffset());
    }

    auto rowCount = std::ssize(rows);
    for (i64 index = context->Offset; index < rowCount; index += RowsetProcessingBatchSize) {
        auto size = std::min(RowsetProcessingBatchSize, rowCount - index);
        processedRows += size;

        bool finished = consumeRows(consumeRowsClosure, &consumerContext, begin + index, size);
        YT_VERIFY(!finished);

        consumerContext.Clear();

        yielder.Checkpoint(processedRows);
    }
}

void WriteOpHelper(
    TExecutionContext* context,
    size_t rowSize,
    void** collectRowsClosure,
    void (*collectRowsFunction)(void** closure, TWriteOpClosure* writeOpClosure))
{
    auto collectRows = PrepareFunction(collectRowsFunction);

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
            SaveAndRestoreCurrentCompartment([&] {
                shouldNotWait = writer->Write(batch);
            });
        }

        if (!shouldNotWait) {
            TValueIncrementingTimingGuard<TWallTimer> timingGuard(&context->Statistics->WaitOnReadyEventTime);
            SaveAndRestoreCurrentCompartment([&] {
                WaitForFast(writer->GetReadyEvent())
                    .ThrowOnError();
            });
        }
    }

    YT_LOG_DEBUG("Closing writer");
    {
        TValueIncrementingTimingGuard<TWallTimer> timingGuard(&context->Statistics->WaitOnReadyEventTime);
        SaveAndRestoreCurrentCompartment([&] {
            WaitForFast(context->Writer->Close())
                .ThrowOnError();
        });
    }
}

char* AllocateBytes(TExpressionContext* context, size_t byteCount)
{
    return context->AllocateUnaligned(byteCount, EAddressSpace::WebAssembly);
}

////////////////////////////////////////////////////////////////////////////////

TPIValue* LookupInRowset(
    TComparerFunction* comparerFunction,
    THasherFunction* hasherFunction,
    TComparerFunction* eqComparerFunction,
    TPIValue* key,
    TSharedRange<TRange<TPIValue>>* rowset,
    std::unique_ptr<TLookupRowInRowsetWebAssemblyContext>* lookupContext)
{
    auto comparer = PrepareFunction(comparerFunction);
    auto hasher = PrepareFunction(hasherFunction);
    auto eqComparer = PrepareFunction(eqComparerFunction);

    if (*lookupContext == nullptr) {
        *lookupContext = std::make_unique<TLookupRowInRowsetWebAssemblyContext>();

        if (auto* compartment = GetCurrentCompartment()) {
            // TODO(dtorilov): Change signature to return TSharedRange<TRange<TPIValue>>.
            auto [guard, rows] = CopyRowRangeIntoCompartment(*rowset, compartment);
            (*lookupContext)->RowsInsideCompartmentGuard = std::move(guard);
            (*lookupContext)->RowsInsideCompartment = rows;
        }
    }

    if (rowset->Size() < 32) {
        if (HasCurrentCompartment()) {
            auto& searchRange = (*lookupContext)->RowsInsideCompartment;
            auto it = std::lower_bound(
                searchRange.begin(),
                searchRange.end(),
                key,
                [&] (TPIValue* rowOffsetInsideCompartment, TPIValue* target) {
                    return comparer(rowOffsetInsideCompartment, target);
                });

            if (it != searchRange.end() && !comparer(key, *it)) {
                return const_cast<TPIValue*>(*it);
            }

            return nullptr;
        } else {
            auto it = std::lower_bound(
                rowset->Begin(),
                rowset->End(),
                key,
                [&] (TRange<TPIValue> row, TPIValue* values) {
                    return comparer(row.Begin(), values);
                });

            if (it != rowset->End() && !comparer(key, it->Begin())) {
                return const_cast<TPIValue*>(it->Begin());
            }

            return nullptr;
        }
    }

    auto& lookupTable = (*lookupContext)->LookupTable;
    if (lookupTable == nullptr) {
        lookupTable = std::make_unique<TLookupRows>(rowset->Size(), hasher, eqComparer);
        lookupTable->set_empty_key(nullptr);

        if (HasCurrentCompartment()) {
            auto& searchRange = (*lookupContext)->RowsInsideCompartment;
            for (auto* row : searchRange) {
                lookupTable->insert(row);
            }
        } else {
            for (auto& row : *rowset) {
                lookupTable->insert(row.Begin());
            }
        }
    }

    auto it = lookupTable->find(key);
    if (it != lookupTable->end()) {
        return const_cast<TPIValue*>(*it);
    }

    return nullptr;
}

char IsRowInRowset(
    TComparerFunction* comparer,
    THasherFunction* hasher,
    TComparerFunction* eqComparer,
    TPIValue* values,
    TSharedRange<TRange<TPIValue>>* rows,
    std::unique_ptr<TLookupRowInRowsetWebAssemblyContext>* lookupRows)
{
    return LookupInRowset(comparer, hasher, eqComparer, values, rows, lookupRows) != nullptr;
}

char IsRowInRanges(
    ui32 valuesCount,
    TPIValue* values,
    TSharedRange<TPIRowRange>* ranges)
{
    values = ConvertPointerFromWasmToHost(values);

    auto it = std::lower_bound(
        ranges->Begin(),
        ranges->End(),
        values,
        [&] (TPIRowRange range, TPIValue* values) {
            ui32 length = std::min(static_cast<ui32>(range.second.Size()), valuesCount);
            return CompareRows(range.second.Begin(), range.second.Begin() + length, values, values + length) < 0;
        });

    if (it == ranges->End()) {
        return false;
    }

    ui32 length = std::min(static_cast<ui32>(it->first.Size()), valuesCount);
    return CompareRows(
        it->first.Begin(),
        it->first.Begin() + length,
        values,
        values + length) <= 0;
}

const TPIValue* TransformTuple(
    TComparerFunction* comparer,
    THasherFunction* hasher,
    TComparerFunction* eqComparer,
    TPIValue* values,
    TSharedRange<TRange<TPIValue>>* rows,
    std::unique_ptr<TLookupRowInRowsetWebAssemblyContext>* lookupRows)
{
    return LookupInRowset(comparer, hasher, eqComparer, values, rows, lookupRows);
}

size_t StringHash(
    const char* data,
    ui32 length)
{
    return FarmFingerprint(ConvertPointerFromWasmToHost(data, length), length);
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
    // TODO(dtorilov): Infer length of error description.
    THROW_ERROR_EXCEPTION("Error while executing UDF")
        << TError(TRuntimeFormat(ConvertPointerFromWasmToHost(error)));
}

void ThrowQueryException(const char* error)
{
    THROW_ERROR_EXCEPTION("Error while executing query")
        << TError(TRuntimeFormat(ConvertPointerFromWasmToHost(error)));
}

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <typename TStringType>
void CopyString(TExpressionContext* context, TValue* result, const TStringType& str)
{
    auto* offset = AllocateBytes(context, str.size());
    ::memcpy(ConvertPointerFromWasmToHost(offset, str.size()), str.data(), str.size());
    *ConvertPointerFromWasmToHost(result) = MakeUnversionedStringValue(TStringBuf(offset, str.size()));
}

template <typename TStringType>
void CopyString(TExpressionContext* context, TPIValue* result, const TStringType& str)
{
    auto* offset = AllocateBytes(context, str.size());
    ::memcpy(ConvertPointerFromWasmToHost(offset, str.size()), str.data(), str.size());
    MakePositionIndependentStringValue(
        ConvertPointerFromWasmToHost(result),
        TStringBuf(
            ConvertPointerFromWasmToHost(offset, str.size()),
            str.size()));
}

template <typename TStringType>
void CopyAny(TExpressionContext* context, TValue* result, const TStringType& str)
{
    auto* offset = AllocateBytes(context, str.size());
    ::memcpy(ConvertPointerFromWasmToHost(offset, str.size()), str.data(), str.size());
    *ConvertPointerFromWasmToHost(result) = MakeUnversionedAnyValue(TStringBuf(offset, str.size()));
}

template <typename TStringType>
void CopyAny(TExpressionContext* context, TPIValue* result, const TStringType& str)
{
    auto* offset = AllocateBytes(context, str.size());
    ::memcpy(ConvertPointerFromWasmToHost(offset, str.size()), str.data(), str.size());
    MakePositionIndependentAnyValue(
        ConvertPointerFromWasmToHost(result),
        TStringBuf(
            ConvertPointerFromWasmToHost(offset, str.size()),
            str.size()));
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

re2::RE2* RegexCreate(TValue* regexp)
{
    auto* regexAtHost = ConvertPointerFromWasmToHost(regexp);
    auto regexString = TStringBuf(
        ConvertPointerFromWasmToHost(regexAtHost->Data.String),
        regexAtHost->Length);

    re2::RE2::Options options;
    options.set_log_errors(false);
    auto re2 = std::make_unique<re2::RE2>(
        re2::StringPiece(regexString.data(), regexString.size()),
        options);
    if (!re2->ok()) {
        THROW_ERROR_EXCEPTION(
            "Error parsing regular expression %Qv",
            regexString)
            << TError(TRuntimeFormat(re2->error().c_str()));
    }
    return re2.release();
}

void RegexDestroy(re2::RE2* re2)
{
    delete re2;
}

bool RegexFullMatch(re2::RE2* re2, TValue* string)
{
    auto* stringAtHost = ConvertPointerFromWasmToHost(string);
    YT_VERIFY(stringAtHost->Type == EValueType::String);

    auto stringBuf = TStringBuf(
        ConvertPointerFromWasmToHost(stringAtHost->Data.String),
        stringAtHost->Length);

    return re2::RE2::FullMatch(
        re2::StringPiece(stringBuf.data(), stringBuf.size()),
        *re2);
}

bool RegexPartialMatch(re2::RE2* re2, TValue* string)
{
    auto* stringAtHost = ConvertPointerFromWasmToHost(string);
    YT_VERIFY(stringAtHost->Type == EValueType::String);
    auto stringBuf = TStringBuf(
        ConvertPointerFromWasmToHost(stringAtHost->Data.String),
        stringAtHost->Length);

    return re2::RE2::PartialMatch(
        re2::StringPiece(stringBuf.data(), stringBuf.size()),
        *re2);
}

void RegexReplaceFirst(
    TExpressionContext* context,
    re2::RE2* re2,
    TValue* string,
    TValue* rewrite,
    TValue* result)
{
    auto* stringAtHost = ConvertPointerFromWasmToHost(string);
    YT_VERIFY(stringAtHost->Type == EValueType::String);
    auto stringBuf = TStringBuf(
        ConvertPointerFromWasmToHost(stringAtHost->Data.String),
        stringAtHost->Length);

    auto* rewriteAtHost = ConvertPointerFromWasmToHost(rewrite);
    YT_VERIFY(rewriteAtHost->Type == EValueType::String);
    auto rewriteAtHostStringBuf = TStringBuf(
        ConvertPointerFromWasmToHost(rewriteAtHost->Data.String),
        rewriteAtHost->Length);

    auto rewritten = std::string(stringBuf);
    re2::RE2::Replace(
        &rewritten,
        *re2,
        re2::StringPiece(rewriteAtHostStringBuf.data(), rewriteAtHostStringBuf.size()));

    NDetail::CopyString(context, result, rewritten);
}

void RegexReplaceAll(
    TExpressionContext* context,
    re2::RE2* re2,
    TValue* string,
    TValue* rewrite,
    TValue* result)
{
    auto* stringAtHost = ConvertPointerFromWasmToHost(string);
    YT_VERIFY(stringAtHost->Type == EValueType::String);
    auto stringBuf = TStringBuf(
        ConvertPointerFromWasmToHost(stringAtHost->Data.String),
        stringAtHost->Length);

    auto* rewriteAtHost = ConvertPointerFromWasmToHost(rewrite);
    YT_VERIFY(rewriteAtHost->Type == EValueType::String);
    auto rewriteAtHostStringBuf = TStringBuf(
        ConvertPointerFromWasmToHost(rewriteAtHost->Data.String),
        rewriteAtHost->Length);

    auto rewritten = std::string(stringBuf);
    re2::RE2::GlobalReplace(
        &rewritten,
        *re2,
        re2::StringPiece(rewriteAtHostStringBuf.data(), rewriteAtHostStringBuf.size()));

    NDetail::CopyString(context, result, rewritten);
}

void RegexExtract(
    TExpressionContext* context,
    re2::RE2* re2,
    TValue* string,
    TValue* rewrite,
    TValue* result)
{
    auto* stringAtHost = ConvertPointerFromWasmToHost(string);
    YT_VERIFY(stringAtHost->Type == EValueType::String);
    auto stringBuf = TStringBuf(
        ConvertPointerFromWasmToHost(stringAtHost->Data.String),
        stringAtHost->Length);

    auto* rewriteAtHost = ConvertPointerFromWasmToHost(rewrite);
    YT_VERIFY(rewriteAtHost->Type == EValueType::String);
    auto rewriteAtHostStringBuf = TStringBuf(
        ConvertPointerFromWasmToHost(rewriteAtHost->Data.String),
        rewriteAtHost->Length);

    auto extracted = std::string(stringBuf);
    re2::RE2::Extract(
        re2::StringPiece(stringBuf.data(), stringBuf.size()),
        *re2,
        re2::StringPiece(rewriteAtHostStringBuf.data(), rewriteAtHostStringBuf.size()),
        &extracted);

    NDetail::CopyString(context, result, extracted);
}

void RegexEscape(
    TExpressionContext* context,
    TValue* string,
    TValue* result)
{
    auto* stringAtHost = ConvertPointerFromWasmToHost(string);
    YT_VERIFY(stringAtHost->Type == EValueType::String);
    auto stringBuf = TStringBuf(
        ConvertPointerFromWasmToHost(stringAtHost->Data.String),
        stringAtHost->Length);

    auto escaped = re2::RE2::QuoteMeta(
        re2::StringPiece(stringBuf.data(), stringBuf.size()));

    NDetail::CopyString(context, result, escaped);
}

static void* XdeltaAllocate(void* opaque, size_t size)
{
    if (opaque) {
        return std::bit_cast<uint8_t*>(
            ConvertPointerFromWasmToHost(
                AllocateBytes(static_cast<TExpressionContext*>(opaque), size),
                size));
    }

    return static_cast<uint8_t*>(malloc(size));
}

static void XdeltaFree(void* opaque, void* ptr)
{
    if (!opaque) {
        free(ptr);
    }
}

int XdeltaMerge(
    TExpressionContext* context,
    const uint8_t* lhsData,
    size_t lhsSize,
    const uint8_t* rhsData,
    size_t rhsSize,
    const uint8_t** resultData,
    size_t* resultOffset,
    size_t* resultSize)
{
    lhsData = ConvertPointerFromWasmToHost(lhsData);
    rhsData = ConvertPointerFromWasmToHost(rhsData);

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
    *ConvertPointerFromWasmToHost(resultData) = ConvertPointerFromHostToWasm(result.Data);
    *ConvertPointerFromWasmToHost(resultOffset) = result.Offset;
    *ConvertPointerFromWasmToHost(resultSize) = result.Size;

    return code;
}

////////////////////////////////////////////////////////////////////////////////

#define DEFINE_YPATH_GET_IMPL2(PREFIX, TYPE, STATEMENT_OK, STATEMENT_FAIL) \
    void PREFIX ## Get ## TYPE( \
        [[maybe_unused]] TExpressionContext* context, \
        TValue* result, \
        TValue* anyValue, \
        TValue* ypath) \
    { \
        TValue* anyValueAtHost = ConvertPointerFromWasmToHost(anyValue); \
        const char* anyValueDataOffset = anyValueAtHost->Data.String; \
        const char* anyValueDataAtHost = ConvertPointerFromWasmToHost(anyValueDataOffset, anyValueAtHost->Length); \
        \
        TValue* ypathAtHost = ConvertPointerFromWasmToHost(ypath); \
        const char* ypathDataOffset = ypathAtHost->Data.String; \
        const char* ypathDataAtHost = ConvertPointerFromWasmToHost(ypathDataOffset, ypathAtHost->Length); \
        \
        auto value = NYTree::TryGet ## TYPE( \
            TStringBuf(anyValueDataAtHost, anyValueAtHost->Length), \
            TString(ypathDataAtHost, ypathAtHost->Length)); \
        if (value) { \
            STATEMENT_OK \
        } else { \
            STATEMENT_FAIL \
        } \
    }

#define DEFINE_YPATH_GET_IMPL(TYPE, STATEMENT_OK) \
    DEFINE_YPATH_GET_IMPL2(Try, TYPE, STATEMENT_OK, \
        TValue* resultAtHost = ConvertPointerFromWasmToHost(result); \
        *resultAtHost = MakeUnversionedNullValue();) \
    DEFINE_YPATH_GET_IMPL2(, TYPE, STATEMENT_OK, \
        THROW_ERROR_EXCEPTION("Value of type %Qlv is not found at YPath %v", \
            EValueType::TYPE, \
            ypathDataAtHost);)

#define DEFINE_YPATH_GET(TYPE) \
    DEFINE_YPATH_GET_IMPL(TYPE, \
        TValue* resultAtHost = ConvertPointerFromWasmToHost(result); \
        *resultAtHost = MakeUnversioned ## TYPE ## Value(*value);)

#define DEFINE_YPATH_GET_STRING \
    DEFINE_YPATH_GET_IMPL(String, \
        NDetail::CopyString(context, result, *value);)

#define DEFINE_YPATH_GET_ANY \
    DEFINE_YPATH_GET_IMPL(Any, \
        NDetail::CopyAny(context, result, *value);)

DEFINE_YPATH_GET(Int64)
DEFINE_YPATH_GET(Uint64)
DEFINE_YPATH_GET(Double)
DEFINE_YPATH_GET(Boolean)
DEFINE_YPATH_GET_STRING
DEFINE_YPATH_GET_ANY

////////////////////////////////////////////////////////////////////////////////

#define DEFINE_CONVERT_ANY(TYPE, STATEMENT_OK) \
    void AnyTo ## TYPE([[maybe_unused]] TExpressionContext* context, TPIValue* result, TPIValue* anyValue) \
    { \
        anyValue = ConvertPointerFromWasmToHost(anyValue); \
        if (anyValue->Type == EValueType::Null) { \
            MakePositionIndependentNullValue(ConvertPointerFromWasmToHost(result)); \
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
    void AnyTo ## TYPE(TExpressionContext* /*context*/, TPIValue* result, TPIValue* anyValue) \
    { \
        anyValue = ConvertPointerFromWasmToHost(anyValue); \
        if (anyValue->Type == EValueType::Null) { \
            MakePositionIndependentNullValue(ConvertPointerFromWasmToHost(result)); \
            return; \
        } \
        NYson::TToken token; \
        auto anyString = anyValue->AsStringBuf(); \
        NYson::ParseToken(anyString, &token); \
        if (token.GetType() == NYson::ETokenType::Int64) { \
            MakePositionIndependent ## TYPE ## Value(ConvertPointerFromWasmToHost(result), token.GetInt64Value()); \
        } else if (token.GetType() == NYson::ETokenType::Uint64) { \
            MakePositionIndependent ## TYPE ## Value(ConvertPointerFromWasmToHost(result), token.GetUint64Value()); \
        } else if (token.GetType() == NYson::ETokenType::Double) { \
            MakePositionIndependent ## TYPE ## Value(ConvertPointerFromWasmToHost(result), token.GetDoubleValue()); \
        } else { \
            THROW_ERROR_EXCEPTION("Cannot convert value %Qv of type \"any\" to %Qlv", \
                anyString, \
                EValueType::TYPE); \
        } \
    }

DEFINE_CONVERT_ANY_NUMERIC_IMPL(Int64)
DEFINE_CONVERT_ANY_NUMERIC_IMPL(Uint64)
DEFINE_CONVERT_ANY_NUMERIC_IMPL(Double)
DEFINE_CONVERT_ANY(
    Boolean,
    MakePositionIndependentBooleanValue(ConvertPointerFromWasmToHost(result),
    token.GetBooleanValue());)
DEFINE_CONVERT_ANY(String, NDetail::CopyString(context, result, token.GetStringValue());)

////////////////////////////////////////////////////////////////////////////////

void ThrowCannotCompareTypes(NYson::ETokenType lhsType, NYson::ETokenType rhsType)
{
    THROW_ERROR_EXCEPTION("Cannot compare values of types %Qlv and %Qlv",
        lhsType,
        rhsType);
}

#define DEFINE_COMPARE_ANY(TYPE, TOKEN_TYPE) \
int CompareAny##TOKEN_TYPE(char* lhsData, i32 lhsLength, TYPE rhsValue) \
{ \
    lhsData = ConvertPointerFromWasmToHost(lhsData); \
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
    lhsData = ConvertPointerFromWasmToHost(lhsData);
    rhsData = ConvertPointerFromWasmToHost(rhsData);

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

void ToAny(TExpressionContext* context, TValue* result, TValue* value)
{
    auto* valueAtHost = ConvertPointerFromWasmToHost(value);

    auto valueCopy = *valueAtHost;
    if (IsStringLikeType(valueAtHost->Type)) {
        valueCopy.Data.String = ConvertPointerFromWasmToHost(valueAtHost->Data.String);
    }

    // TODO(babenko): for some reason, flags are garbage here.
    valueCopy.Flags = {};

    // NB: TRowBuffer should be used with caution while executing via WebAssembly engine.
    TValue buffer = EncodeUnversionedAnyValue(valueCopy, context->GetRowBuffer().Get()->GetPool());

    *(ConvertPointerFromWasmToHost(result)) = buffer;
    if (HasCurrentCompartment()) {
        auto* data = AllocateBytes(context, buffer.Length);
        ::memcpy(ConvertPointerFromWasmToHost(data, buffer.Length), buffer.Data.String, buffer.Length);
        (ConvertPointerFromWasmToHost(result))->Data.String = data;
    }
}

////////////////////////////////////////////////////////////////////////////////

void ToLowerUTF8(TExpressionContext* context, char** result, int* resultLength, char* source, int sourceLength)
{
    auto lowered = ToLowerUTF8(TStringBuf(ConvertPointerFromWasmToHost(source), sourceLength));
    auto* offset = AllocateBytes(context, lowered.size());
    *ConvertPointerFromWasmToHost(result) = offset;
    *ConvertPointerFromWasmToHost(resultLength) = lowered.size();
    ::memcpy(ConvertPointerFromWasmToHost(offset, lowered.size()), lowered.data(), lowered.size());
}

TFingerprint GetFarmFingerprint(const TValue* begin, const TValue* end)
{
    auto asRange = TMutableRange<TValue>(
        ConvertPointerFromWasmToHost(begin, end - begin),
        static_cast<size_t>(end - begin));

    for (auto& item : asRange) {
        if (IsStringLikeType(item.Type)) {
            item.Data.String = ConvertPointerFromWasmToHost(item.Data.String);
        }
    }

    auto finally = Finally([&] {
        for (auto& item : asRange) {
            if (IsStringLikeType(item.Type)) {
                item.Data.String = ConvertPointerFromHostToWasm(item.Data.String);
            }
        }
    });

    // TODO(dtorilov): Do not convert twice.
    auto asPIRange = BorrowFromNonPI(TRange(asRange.Begin(), asRange.End()));

    return GetFarmFingerprint(asPIRange.Begin(), asPIRange.Begin() + asPIRange.Size());
}

////////////////////////////////////////////////////////////////////////////////

extern "C" void MakeMap(
    TExpressionContext* context,
    TValue* result,
    TValue* args,
    int argCount)
{
    if (argCount % 2 != 0) {
        THROW_ERROR_EXCEPTION("\"make_map\" takes a even number of arguments");
    }

    auto arguments = TRange(ConvertPointerFromWasmToHost(args, argCount), argCount);

    TString resultYson;
    TStringOutput output(resultYson);
    NYson::TYsonWriter writer(&output);

    writer.OnBeginMap();
    for (int index = 0; index < argCount / 2; ++index) {
        const auto& nameArg = arguments[index * 2];
        const auto& valueArg = arguments[index * 2 + 1];

        if (nameArg.Type != EValueType::String) {
            THROW_ERROR_EXCEPTION("Invalid type of key in key-value pair #%v: expected %Qlv, got %Qlv",
                index,
                EValueType::String,
                nameArg.Type);
        }

        writer.OnKeyedItem(TStringBuf(
            ConvertPointerFromWasmToHost(nameArg.Data.String, nameArg.Length),
            nameArg.Length));

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
                writer.OnStringScalar(TStringBuf(
                    ConvertPointerFromWasmToHost(valueArg.Data.String, valueArg.Length),
                    valueArg.Length));
                break;
            case EValueType::Any:
                writer.OnRaw(TStringBuf(
                    ConvertPointerFromWasmToHost(valueArg.Data.String, valueArg.Length),
                    valueArg.Length));
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

    NDetail::CopyAny(context, result, resultYson);
}

extern "C" void MakeList(
    TExpressionContext* context,
    TValue* result,
    TValue* args,
    int argCount)
{
    auto arguments = TRange(ConvertPointerFromWasmToHost(args, argCount), argCount);

    TString resultYson;
    TStringOutput output(resultYson);
    NYson::TYsonWriter writer(&output);

    writer.OnBeginList();
    for (int index = 0; index < argCount; ++index) {
        const auto& valueArg = arguments[index];

        writer.OnListItem();

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
                writer.OnStringScalar(TStringBuf(
                    ConvertPointerFromWasmToHost(valueArg.Data.String, valueArg.Length),
                    valueArg.Length));
                break;
            case EValueType::Any:
                writer.OnRaw(TStringBuf(
                    ConvertPointerFromWasmToHost(valueArg.Data.String, valueArg.Length),
                    valueArg.Length));
                break;
            case EValueType::Null:
                writer.OnEntity();
                break;
            default:
                THROW_ERROR_EXCEPTION("Unexpected type %Qlv of value #%v",
                    valueArg.Type,
                    index);
        }
    }
    writer.OnEndList();

    NDetail::CopyAny(context, result, resultYson);
}

bool ListContainsNullImpl(const INodePtr& node)
{
    auto children = node->AsList()->GetChildren();
    return std::any_of(children.begin(), children.end(), [] (const INodePtr& element) {
        return element->GetType() == ENodeType::Entity;
    });
}

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
    TValue* result,
    TValue* ysonList,
    TValue* what)
{
    auto whatAtHost = *ConvertPointerFromWasmToHost(what);
    if (IsStringLikeType(whatAtHost.Type)) {
        whatAtHost.Data.String = ConvertPointerFromWasmToHost(whatAtHost.Data.String, whatAtHost.Length);
    }

    auto ysonListAtHost = *ConvertPointerFromWasmToHost(ysonList);
    YT_VERIFY(IsStringLikeType(ysonListAtHost.Type));
    ysonListAtHost.Data.String = ConvertPointerFromWasmToHost(ysonListAtHost.Data.String, ysonListAtHost.Length);

    auto node = NYTree::ConvertToNode(
        FromUnversionedValue<NYson::TYsonStringBuf>(ysonListAtHost));

    bool found = false;
    switch (whatAtHost.Type) {
        case EValueType::String:
            found = ListContainsImpl<ENodeType::String, TString>(node, TStringBuf(whatAtHost.Data.String, whatAtHost.Length));
            break;
        case EValueType::Int64:
            found = ListContainsImpl<ENodeType::Int64, i64>(node, whatAtHost.Data.Int64);
            break;
        case EValueType::Uint64:
            found = ListContainsImpl<ENodeType::Uint64, ui64>(node, whatAtHost.Data.Uint64);
            break;
        case EValueType::Boolean:
            found = ListContainsImpl<ENodeType::Boolean, bool>(node, whatAtHost.Data.Boolean);
            break;
        case EValueType::Double:
            found = ListContainsImpl<ENodeType::Double, double>(node, whatAtHost.Data.Double);
            break;
        case EValueType::Null:
            found = ListContainsNullImpl(node);
            break;
        default:
            THROW_ERROR_EXCEPTION("ListContains is not implemented for %Qlv values",
                whatAtHost.Type);
    }

    *ConvertPointerFromWasmToHost(result) = MakeUnversionedBooleanValue(found);
}

template <ENodeType NodeType, typename TValue>
bool ListHasIntersectionImpl(const INodePtr& lhsNode, const INodePtr& rhsNode)
{
    THashSet<TValue> values;
    for (const auto& node : rhsNode->AsList()->GetChildren()) {
        switch (node->GetType()) {
            case ENodeType::Entity:
                break;
            case NodeType:
                values.insert(ConvertTo<TValue>(node));
                break;
            default:
                ThrowException("ListHasIntersection args list must contain only elements of the same type");
        }
    }
    for (const auto& node : lhsNode->AsList()->GetChildren()) {
        if (node->GetType() == NodeType && values.contains(ConvertTo<TValue>(node))) {
            return true;
        }
    }
    return false;
}

void ListHasIntersection(
    TExpressionContext* /*context*/,
    TValue* result,
    TValue* lhsYsonList,
    TValue* rhsYsonList)
{
    auto lhsAtHost = *ConvertPointerFromWasmToHost(lhsYsonList);
    YT_VERIFY(IsStringLikeType(lhsAtHost.Type));
    lhsAtHost.Data.String = ConvertPointerFromWasmToHost(lhsAtHost.Data.String, lhsAtHost.Length);
    auto rhsAtHost = *ConvertPointerFromWasmToHost(rhsYsonList);
    YT_VERIFY(IsStringLikeType(rhsAtHost.Type));
    rhsAtHost.Data.String = ConvertPointerFromWasmToHost(rhsAtHost.Data.String, rhsAtHost.Length);

    auto lhsNode = NYTree::ConvertToNode(
        FromUnversionedValue<NYson::TYsonStringBuf>(lhsAtHost));
    auto rhsNode = NYTree::ConvertToNode(
        FromUnversionedValue<NYson::TYsonStringBuf>(rhsAtHost));

    bool found = false;
    const auto rhsNodeList = rhsNode->AsList()->GetChildren();
    size_t i = 0;
    while (i < rhsNodeList.size() && rhsNodeList[i]->GetType() == ENodeType::Entity) {
        ++i;
    }
    if (i < rhsNodeList.size()) {
        auto element = rhsNodeList[i];
        switch (element->GetType()) {
            case ENodeType::String:
                found = ListHasIntersectionImpl<ENodeType::String, TString>(lhsNode, rhsNode);
                break;
            case ENodeType::Int64:
                found = ListHasIntersectionImpl<ENodeType::Int64, i64>(lhsNode, rhsNode);
                break;
            case ENodeType::Uint64:
                found = ListHasIntersectionImpl<ENodeType::Uint64, ui64>(lhsNode, rhsNode);
                break;
            case ENodeType::Boolean:
                found = ListHasIntersectionImpl<ENodeType::Boolean, bool>(lhsNode, rhsNode);
                break;
            case ENodeType::Double:
                found = ListHasIntersectionImpl<ENodeType::Double, double>(lhsNode, rhsNode);
                break;
            default:
                THROW_ERROR_EXCEPTION("ListHasIntersection is not implemented for %Qlv values",
                element->GetType());
        }
    }

    *ConvertPointerFromWasmToHost(result) = MakeUnversionedBooleanValue(found);
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

    // NB: TRowBuffer should be used with caution while executing via WebAssembly engine.
    auto output = TChunkedMemoryPoolOutput(context->GetRowBuffer()->GetPool(), textYsonLengthEstimate);
    {
        TYsonWriter writer(&output, EYsonFormat::Text);
        TMemoryInput input(ConvertPointerFromWasmToHost(any), anyLength);
        TYsonPullParser parser(&input, EYsonType::Node);
        TYsonPullParserCursor cursor(&parser);
        cursor.TransferComplexValue(&writer);
    }
    auto refs = output.Finish();
    if (!HasCurrentCompartment() && refs.size() == 1) {
        *ConvertPointerFromWasmToHost(result) = refs.front().Begin();
        *ConvertPointerFromWasmToHost(resultLength) = refs.front().Size();
    } else {
        *ConvertPointerFromWasmToHost(resultLength) = GetByteSize(refs);
        *ConvertPointerFromWasmToHost(result) = AllocateBytes(context, *ConvertPointerFromWasmToHost(resultLength));
        size_t offset = 0;
        for (const auto& ref : refs) {
            ::memcpy(ConvertPointerFromWasmToHost(*ConvertPointerFromWasmToHost(result) + offset, ref.Size()), ref.Begin(), ref.Size());
            offset += ref.Size();
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

extern "C" void NumericToString(
    TExpressionContext* context,
    TValue* result,
    TValue* value)
{
    auto* valueAtHost = ConvertPointerFromWasmToHost(value);

    if (valueAtHost->Type == EValueType::Null) {
        ConvertPointerFromWasmToHost(result)->Type = EValueType::Null;
        return;
    }

    auto resultYson = TString();
    auto output = TStringOutput(resultYson);
    auto writer = TYsonWriter(&output, EYsonFormat::Text);

    switch (valueAtHost->Type) {
        case EValueType::Int64:
            writer.OnInt64Scalar(valueAtHost->Data.Int64);
            break;
        case EValueType::Uint64:
            writer.OnUint64Scalar(valueAtHost->Data.Uint64);
            break;
        case EValueType::Double:
            writer.OnDoubleScalar(valueAtHost->Data.Double);
            break;
        default:
            YT_ABORT();
    }

    NDetail::CopyString(context, result, resultYson);
}

////////////////////////////////////////////////////////////////////////////////

#define DEFINE_CONVERT_STRING(TYPE) \
    extern "C" void StringTo ## TYPE(TExpressionContext* /*context*/, TValue* result, TValue* value) \
    { \
        auto* resultAtHost = ConvertPointerFromWasmToHost(result); \
        auto* valueAtHost = ConvertPointerFromWasmToHost(value); \
        if (valueAtHost->Type == EValueType::Null) { \
            *resultAtHost = MakeUnversionedNullValue(); \
            return; \
        } \
        NYson::TToken token; \
        auto valueString = TStringBuf(ConvertPointerFromWasmToHost(valueAtHost->Data.String, valueAtHost->Length), valueAtHost->Length); \
        NYson::ParseToken(valueString, &token); \
        if (token.GetType() == NYson::ETokenType::Int64) { \
            *resultAtHost = MakeUnversioned ## TYPE ## Value(token.GetInt64Value()); \
        } else if (token.GetType() == NYson::ETokenType::Uint64) { \
            *resultAtHost = MakeUnversioned ## TYPE ## Value(token.GetUint64Value()); \
        } else if (token.GetType() == NYson::ETokenType::Double) { \
            *resultAtHost = MakeUnversioned ## TYPE ## Value(token.GetDoubleValue()); \
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

void HyperLogLogAllocate(TExpressionContext* context, TValue* result)
{
    auto* hllOffset = AllocateBytes(context, sizeof(THLL));
    auto* hll = ConvertPointerFromWasmToHost(hllOffset, sizeof(THLL));
    new (hll) THLL();

    auto* resultAtHost = ConvertPointerFromWasmToHost(result);
    *resultAtHost = MakeUnversionedStringValue(TStringBuf(hllOffset, sizeof(THLL)));
}

void HyperLogLogAdd(void* hll, uint64_t value)
{
    auto* hllAtHost = ConvertPointerFromWasmToHost(static_cast<THLL*>(hll));
    hllAtHost->Add(value);
}

void HyperLogLogMerge(void* hll1, void* hll2)
{
    auto* hll1AtHost = ConvertPointerFromWasmToHost(static_cast<THLL*>(hll1));
    auto* hll2AtHost = ConvertPointerFromWasmToHost(static_cast<THLL*>(hll2));
    hll1AtHost->Merge(*hll2AtHost);
}

ui64 HyperLogLogEstimateCardinality(void* hll)
{
    auto* hllAtHost = ConvertPointerFromWasmToHost(static_cast<THLL*>(hll));
    return hllAtHost->EstimateCardinality();
}

ui64 HyperLogLogGetFingerprint(TValue* value)
{
    auto valueAtHost = *ConvertPointerFromWasmToHost(value);
    if (IsStringLikeType(valueAtHost.Type)) {
        valueAtHost.Data.String = ConvertPointerFromWasmToHost(valueAtHost.Data.String);
    }
    return NTableClient::GetFarmFingerprint(valueAtHost);
}

////////////////////////////////////////////////////////////////////////////////

struct TChunkReplica
{
    TChunkId LocationUuid;
    i64 Index;
    ui64 NodeId;

    auto operator<=>(const TChunkReplica& other) const = default;
};

using TChunkReplicaList = TCompactVector<TChunkReplica, TypicalReplicaCount>;

TChunkReplicaList UniteReplicas(const TChunkReplicaList& first, const TChunkReplicaList& second)
{
    TChunkReplicaList result;
    result.insert(result.end(), first.begin(), first.end());
    result.insert(result.end(), second.begin(), second.end());

    SortUniqueBy(
        result,
        [] (const TChunkReplica& replica) {
            return std::tie(replica.LocationUuid, replica.Index);
        });

    return result;
}

TChunkReplicaList FilterReplicas(const TChunkReplicaList& first, const TChunkReplicaList& second)
{
    TChunkReplicaList result;
    auto it = first.begin();
    auto jt = second.begin();
    while (it < first.end()) {
        if (jt == second.end() || *it < *jt) {
            result.push_back(*it);
            ++it;
        } else if (*jt < *it) {
            ++jt;
        } else {
            ++it;
            ++jt;
        }
    }
    return result;
}

TYsonItem Consume(TYsonPullParserCursor* cursor, EYsonItemType type)
{
    auto current = cursor->GetCurrent();
    if (current.GetType() != type) {
        THROW_ERROR_EXCEPTION("Unexpected YSON item type: expected %Qlv, got %Qlv",
            type,
            current.GetType());
    }
    cursor->Next();
    return current;
}

TChunkReplica ParseReplica(TYsonPullParserCursor* cursor)
{
    TChunkReplica replica;

    Consume(cursor, EYsonItemType::BeginList);

    replica.LocationUuid = TGuid::FromString(Consume(cursor, EYsonItemType::StringValue).UncheckedAsString());
    replica.Index = Consume(cursor, EYsonItemType::Int64Value).UncheckedAsInt64();
    replica.NodeId = Consume(cursor, EYsonItemType::Uint64Value).UncheckedAsUint64();

    Consume(cursor, EYsonItemType::EndList);

    return replica;
}

void ParseReplicasDelta(
    TUnversionedValue* delta,
    TChunkReplicaList* replicasToAdd,
    TChunkReplicaList* replicasToRemove)
{
    if (delta->Type == EValueType::Null) {
        return;
    }

    TMemoryInput input(delta->Data.String, delta->Length);
    TYsonPullParser parser(&input, EYsonType::ListFragment);
    TYsonPullParserCursor cursor(&parser);

    if (!cursor.TryConsumeFragmentStart()) {
        THROW_ERROR_EXCEPTION("Error parsing YSON as list fragment");
    }

    Consume(&cursor, EYsonItemType::BeginList);
    cursor.ParseList([&] (TYsonPullParserCursor* cursor) {
        replicasToAdd->push_back(ParseReplica(cursor));
    });
    cursor.ParseList([&] (TYsonPullParserCursor* cursor) {
        replicasToRemove->push_back(ParseReplica(cursor));
    });
    Consume(&cursor, EYsonItemType::EndList);
}

void ParseReplicas(TUnversionedValue* value, TChunkReplicaList* replicas)
{
    if (value->Type == EValueType::Null) {
        return;
    }

    TMemoryInput input(value->Data.String, value->Length);
    TYsonPullParser parser(&input, EYsonType::ListFragment);
    TYsonPullParserCursor cursor(&parser);

    if (!cursor.TryConsumeFragmentStart()) {
        THROW_ERROR_EXCEPTION("Error parsing yson as list fragment");
    }

    cursor.ParseList([&] (TYsonPullParserCursor* cursor) {
        replicas->push_back(ParseReplica(cursor));
    });
}

void DumpReplicas(IYsonConsumer* consumer, const TChunkReplicaList& replicas)
{
    BuildYsonFluently(consumer)
        .DoListFor(replicas, [] (TFluentList fluent, const TChunkReplica& replica) {
            fluent
                .Item()
                .BeginList()
                    .Item().Value(TFormattableGuid(replica.LocationUuid).ToStringBuf())
                    .Item().Value(replica.Index)
                    .Item().Value(replica.NodeId)
                .EndList();
        });
}

size_t EstimateReplicasYsonLength(const TChunkReplicaList& replicas)
{
    return (MaxGuidStringSize + 2 * MaxVarInt64Size + 10) * replicas.size() + 10;
}

void StoredReplicaSetMerge(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state1,
    TUnversionedValue* state2)
{
    if (state1->Type == EValueType::Null) {
        *result = *state2;
        return;
    }

    TChunkReplicaList replicasToAdd1;
    TChunkReplicaList replicasToRemove1;
    ParseReplicasDelta(state1, &replicasToAdd1, &replicasToRemove1);

    TChunkReplicaList replicasToAdd2;
    TChunkReplicaList replicasToRemove2;
    ParseReplicasDelta(state2, &replicasToAdd2, &replicasToRemove2);

    auto replicasToAdd = FilterReplicas(UniteReplicas(replicasToAdd1, replicasToAdd2), replicasToRemove2);

    auto bufferSize = EstimateReplicasYsonLength(replicasToAdd);
    char* outputBuffer = AllocateBytes(context, bufferSize);

    TMemoryOutput output(outputBuffer, bufferSize);
    TYsonWriter writer(&output, EYsonFormat::Binary);

    BuildYsonFluently(&writer)
        .BeginList()
            .Item().Do([&] (auto fluent) {
                DumpReplicas(fluent.GetConsumer(), replicasToAdd);
            })
            .Item().Do([] (auto fluent) {
                // Replicas to remove is empty list.
                DumpReplicas(fluent.GetConsumer(), {});
            })
        .EndList();

    writer.Flush();

    *result = MakeUnversionedAnyValue(TStringBuf(outputBuffer, output.Buf() - outputBuffer));
}

void StoredReplicaSetFinalize(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state)
{
    TChunkReplicaList replicasToAdd;
    TChunkReplicaList replicasToRemove;

    ParseReplicasDelta(state, &replicasToAdd, &replicasToRemove);

    auto outputBufferSize = EstimateReplicasYsonLength(replicasToAdd);
    char* outputBuffer = AllocateBytes(context, outputBufferSize);

    TMemoryOutput output(outputBuffer, outputBufferSize);
    TYsonWriter writer(&output, EYsonFormat::Binary);

    DumpReplicas(&writer, replicasToAdd);

    writer.Flush();

    *result = MakeUnversionedAnyValue(TStringBuf(outputBuffer, output.Buf() - outputBuffer));
}

void LastSeenReplicaSetMerge(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state1,
    TUnversionedValue* state2)
{
    constexpr int MaxLastSeenReplicas = 3;

    if (state1->Type == EValueType::Null) {
        *result = *state2;
        return;
    }

    TChunkReplicaList lastSeenReplicas;
    ParseReplicas(state1, &lastSeenReplicas);

    TChunkReplicaList newReplicas;
    ParseReplicas(state2, &newReplicas);
    for (const auto& replica : newReplicas) {
        // Linear complexity should be fine.
        if (std::find(lastSeenReplicas.begin(), lastSeenReplicas.end(), replica) == lastSeenReplicas.end()) {
            lastSeenReplicas.push_back(replica);
        }
    }

    std::optional<bool> isErasure;
    for (const auto& replica : lastSeenReplicas) {
        auto isReplicaErasure = replica.Index < GenericChunkReplicaIndex;
        if (!isErasure.has_value()) {
            isErasure = isReplicaErasure;
        }
        if (isErasure != isReplicaErasure) {
            THROW_ERROR_EXCEPTION("Erasure replicas are mixed with non-erasure");
        }
    }

    if (isErasure && *isErasure) {
        TCompactVector<std::optional<TChunkReplica>, GenericChunkReplicaIndex> erasureLastSeenReplicas;
        erasureLastSeenReplicas.resize(GenericChunkReplicaIndex);
        for (const auto& replica : lastSeenReplicas) {
            YT_VERIFY(replica.Index < std::ssize(erasureLastSeenReplicas));
            erasureLastSeenReplicas[replica.Index] = replica;
        }

        lastSeenReplicas.clear();
        for (const auto& replica : erasureLastSeenReplicas) {
            if (replica.has_value()) {
                lastSeenReplicas.push_back(*replica);
            }
        }
    } else if (std::ssize(lastSeenReplicas) > MaxLastSeenReplicas) {
        auto excessReplicaCount = std::ssize(lastSeenReplicas) - MaxLastSeenReplicas;
        lastSeenReplicas.erase(lastSeenReplicas.begin(), lastSeenReplicas.begin() + excessReplicaCount);
    }

    auto bufferSize = EstimateReplicasYsonLength(lastSeenReplicas);
    char* outputBuffer = AllocateBytes(context, bufferSize);

    TMemoryOutput output(outputBuffer, bufferSize);
    TYsonWriter writer(&output, EYsonFormat::Binary);
    DumpReplicas(&writer, lastSeenReplicas);
    writer.Flush();

    *result = MakeUnversionedAnyValue(TStringBuf(outputBuffer, output.Buf() - outputBuffer));
}

////////////////////////////////////////////////////////////////////////////////

void NodeToWasmUnversionedValue(TExpressionContext* context, TUnversionedValue& unversionedValue, const INodePtr& value)
{
    if (!value) {
        unversionedValue.Type = EValueType::Null;
        return;
    }
    auto ysonString = NYson::ConvertToYsonString(value);
    unversionedValue.Type = EValueType::Any;
    unversionedValue.Length = ysonString.AsStringBuf().size();
    char* data = AllocateBytes(context, unversionedValue.Length);
    std::memcpy(data, ysonString.AsStringBuf().data(), unversionedValue.Length);
    unversionedValue.Data.String = ConvertPointerFromHostToWasm(data, unversionedValue.Length);
}

INodePtr NodeFromWasmUnversionedValue(const TUnversionedValue& unversionedValue)
{
    if (unversionedValue.Type == EValueType::Null) {
        return nullptr;
    }
    auto data = ConvertPointerFromWasmToHost(unversionedValue.Data.String, unversionedValue.Length);
    auto stringBuf = TStringBuf(data, unversionedValue.Length);
    auto ysonString = NYson::TYsonString(stringBuf);
    return ConvertToNode(ysonString);
}

void RemoveRecursively(INodePtr node)
{
    auto parent = node->GetParent();
    while (parent) {
        if (parent->GetType() != ENodeType::Map) {
            break;
        }

        auto parentMap = parent->AsMap();
        parentMap->RemoveChild(node);
        if (parentMap->GetChildCount() == 0) {
            node = parent;
            parent = parentMap->GetParent();
        } else {
            parent = nullptr;
        }
    }
}

INodePtr DictSum(const INodePtr& stateRoot, const INodePtr& deltaRoot)
{
    if (!deltaRoot || deltaRoot->GetType() != ENodeType::Map) {
        return stateRoot;
    }

    if (!stateRoot) {
        return deltaRoot;
    }

    if (stateRoot->GetType() != ENodeType::Map) {
        return nullptr;
    }

    auto oldStateRoot = ConvertToNode(stateRoot);
    std::vector<std::pair<IMapNodePtr, IMapNodePtr>> traversal;
    traversal.push_back({stateRoot->AsMap(), deltaRoot->AsMap()});
    while (!traversal.empty()) {
        auto [state, delta] = traversal.back();
        traversal.pop_back();
        for (auto& [key, deltaChild] : delta->GetChildren()) {
            auto stateChild = state->FindChild(key);
            if (!stateChild) {
                delta->RemoveChild(deltaChild);
                state->AddChild(key, deltaChild);
                continue;
            }

            if (stateChild->GetType() == ENodeType::Int64 && deltaChild->GetType() == ENodeType::Int64) {
                auto intNode = stateChild->AsInt64();
                intNode->SetValue(intNode->GetValue() + deltaChild->AsInt64()->GetValue());
                if (intNode->GetValue() == 0) {
                    RemoveRecursively(intNode);
                }
            } else if (stateChild->GetType() == ENodeType::Map && deltaChild->GetType() == ENodeType::Map) {
                traversal.push_back({stateChild->AsMap(), deltaChild->AsMap()});
            } else {
                return oldStateRoot;
            }
        }
    }

    return stateRoot;
}

void DictSumIteration(
    TExpressionContext* context,
    TUnversionedValue* result,
    TUnversionedValue* state,
    TUnversionedValue* delta)
{
    auto stateNode = NodeFromWasmUnversionedValue(*state);
    auto deltaNode = NodeFromWasmUnversionedValue(*delta);
    auto resultNode = DictSum(stateNode, deltaNode);
    NodeToWasmUnversionedValue(context, *result, resultNode);
}

////////////////////////////////////////////////////////////////////////////////

// TODO(dtorilov): Add unit-test.
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
    auto subjectClosure = ConvertTo<THashSet<std::string>>(
        TYsonStringBuf(FromUnversionedValue<TStringBuf>(*ysonSubjectClosureList)));
    auto permissions = ConvertTo<EPermissionSet>(
        TYsonStringBuf(FromUnversionedValue<TStringBuf>(*ysonPermissionList)));

    auto action = CheckPermissionsByAclAndSubjectClosure(acl, subjectClosure, permissions);
    *result = MakeUnversionedBooleanValue(action == NSecurityClient::ESecurityAction::Allow);
}

////////////////////////////////////////////////////////////////////////////////

class TYsonLengthEvaluationVisitor
    : public TYsonConsumerBase
{
public:
    void OnStringScalar(TStringBuf /*value*/) override
    {
        if (AttributesDepth_ == 0 && Depth_ == 0) {
            THROW_ERROR_EXCEPTION("YSON List or Map expected, but got %v",
                "String");
        }
    }

    void OnInt64Scalar(i64 /*value*/) override
    {
        if (AttributesDepth_ == 0 && Depth_ == 0) {
            THROW_ERROR_EXCEPTION("YSON List or Map expected, but got %v",
                "Int64");
        }
    }

    void OnUint64Scalar(ui64 /*value*/) override
    {
        if (AttributesDepth_ == 0 && Depth_ == 0) {
            THROW_ERROR_EXCEPTION("YSON List or Map expected, but got %v",
                "Uint64");
        }
    }

    void OnDoubleScalar(double /*value*/) override
    {
        if (AttributesDepth_ == 0 && Depth_ == 0) {
            THROW_ERROR_EXCEPTION("YSON List or Map expected, but got %v",
                "Double");
        }
    }

    void OnBooleanScalar(bool /*value*/) override
    {
        if (AttributesDepth_ == 0 && Depth_ == 0) {
            THROW_ERROR_EXCEPTION("YSON List or Map expected, but got %v",
                "Boolean");
        }
    }

    void OnEntity() override
    {
        if (AttributesDepth_ == 0 && Depth_ == 0) {
            THROW_ERROR_EXCEPTION("YSON List or Map expected, but got %v",
                "Entity");
        }
    }

    void OnBeginList() override
    {
        if (AttributesDepth_ == 0 && Depth_ == 0) {
            YT_ASSERT(!IsList_.has_value());
            IsList_ = true;
        }
        ++Depth_;
    }

    void OnListItem() override
    {
        if (AttributesDepth_ == 0 && Depth_ == 1) {
            ++Length_;
        }
    }

    void OnEndList() override
    {
        --Depth_;
        if (AttributesDepth_ == 0 && Depth_ == 0) {
            YT_ASSERT(IsList_.has_value());
            YT_ASSERT(IsList_ == true);
            YT_ASSERT(!Done_);
            Done_ = true;
        }
    }

    void OnBeginMap() override
    {
        if (AttributesDepth_ == 0 && Depth_ == 0) {
            YT_ASSERT(!IsList_.has_value());
            IsList_ = false;
        }
        ++Depth_;
    }

    void OnKeyedItem(TStringBuf /*value*/) override
    {
        if (AttributesDepth_ == 0 && Depth_ == 1) {
            ++Length_;
        }
    }

    void OnEndMap() override
    {
        --Depth_;
        if (AttributesDepth_ == 0 && Depth_ == 0) {
            YT_ASSERT(IsList_.has_value());
            YT_ASSERT(IsList_ == false);
            YT_ASSERT(!Done_);
            Done_ = true;
        }
    }

    void OnBeginAttributes() override
    {
        ++AttributesDepth_;
    }

    void OnEndAttributes() override
    {
        --AttributesDepth_;
    }

    i64 GetLength() const
    {
        return Length_;
    }

    bool Done() const
    {
        return Done_;
    }

private:
    i64 Depth_ = 0;
    i64 AttributesDepth_ = 0;
    i64 Length_ = 0;
    std::optional<bool> IsList_;
    bool Done_ = false;
};

i64 YsonLength(char* data, int length)
{
    data = ConvertPointerFromWasmToHost(data, length);

    TMemoryInput input(data, length);
    TYsonPullParser ysonParser(&input, EYsonType::Node);
    TYsonPullParserCursor ysonCursor(&ysonParser);

    TYsonLengthEvaluationVisitor lengthEvaluator;
    ysonCursor.TransferComplexValue(&lengthEvaluator);
    YT_ASSERT(lengthEvaluator.Done());
    return lengthEvaluator.GetLength();
}

////////////////////////////////////////////////////////////////////////////////

void LikeOpHelper(
    TPIValue* result,
    TPIValue* text,
    EStringMatchOp matchOp,
    TPIValue* pattern,
    bool useEscapeCharacter,
    TPIValue* escapeCharacter,
    TLikeExpressionContext* context)
{
    result = ConvertPointerFromWasmToHost(result);
    text = ConvertPointerFromWasmToHost(text);
    pattern = ConvertPointerFromWasmToHost(pattern);
    escapeCharacter = ConvertPointerFromWasmToHost(escapeCharacter);

    if (text->Type == EValueType::Null ||
        pattern->Type == EValueType::Null ||
        (useEscapeCharacter && escapeCharacter->Type == EValueType::Null))
    {
        result->Type = EValueType::Null;
        return;
    }

    std::unique_ptr<re2::RE2> newRegex;

    re2::RE2* matcher = nullptr;
    if (context->PrecompiledRegex) {
        matcher = context->PrecompiledRegex.get();
    } else {
        TStringBuf escape;
        if (useEscapeCharacter) {
            escape = escapeCharacter->AsStringBuf();
        }

        auto asRe2Pattern = ConvertLikePatternToRegex(pattern->AsStringBuf(), matchOp, escape, useEscapeCharacter);

        re2::RE2::Options options;
        options.set_log_errors(false);
        newRegex = std::make_unique<re2::RE2>(re2::StringPiece(asRe2Pattern.data(), asRe2Pattern.size()), options);
        if (!newRegex->ok()) {
            THROW_ERROR_EXCEPTION("Error parsing regular expression %Qv",
                asRe2Pattern)
                << TError(TRuntimeFormat(matcher->error().c_str()));
        }

        matcher = newRegex.get();
    }

    bool matched = re2::RE2::FullMatch(re2::StringPiece(text->AsStringBuf().data(), text->AsStringBuf().size()), *matcher);

    result->Type = EValueType::Boolean;
    result->Data.Boolean = matched;
}

////////////////////////////////////////////////////////////////////////////////

void CompositeMemberAccessorHelper(
    TExpressionContext* context,
    TPIValue* result,
    EValueType resultType,
    TPIValue* composite,
    TCompositeMemberAccessorPath* path,
    bool hasDictOrListItemAccessor,
    TPIValue* dictOrListItemAccessor)
{
    using NYTree::ParseMapOrAttributesUntilKey;
    using NYTree::ParseListUntilIndex;
    using NYTree::ParseAnyValue;

    MakePositionIndependentNullValue(ConvertPointerFromWasmToHost(result));

    composite = ConvertPointerFromWasmToHost(composite);
    dictOrListItemAccessor = ConvertPointerFromWasmToHost(dictOrListItemAccessor);

    if (composite->Type == EValueType::Null || composite->Length == 0) {
        return;
    }

    TMemoryInput input(composite->AsStringBuf());
    TYsonPullParser parser(&input, EYsonType::Node);
    TYsonPullParserCursor cursor(&parser);

    for (int index = 0; index < std::ssize(path->NestedTypes); ++index) {
        auto type = path->NestedTypes[index];

        if (type == ELogicalMetatype::Struct) {
            if (cursor->GetType() == EYsonItemType::BeginMap) {
                if (!ParseMapOrAttributesUntilKey(&cursor, path->NamedStructMembers[index])) {
                    return;
                }
            } else if (cursor->GetType() == EYsonItemType::BeginList) {
                if (!ParseListUntilIndex(&cursor, path->PositionalStructMembers[index])) {
                    return;
                }
            } else {
                return;
            }
        } else if (type == ELogicalMetatype::Tuple) {
            if (cursor->GetType() == EYsonItemType::BeginList) {
                if (!ParseListUntilIndex(&cursor, path->TupleItemIndices[index])) {
                    return;
                }
            } else {
                return;
            }
        } else {
            YT_ABORT();
        }
    }

    if (hasDictOrListItemAccessor) {
        if (cursor->GetType() == EYsonItemType::BeginMap && dictOrListItemAccessor->Type == EValueType::String) {
            if (!ParseMapOrAttributesUntilKey(&cursor, dictOrListItemAccessor->AsStringBuf())) {
                return;
            }
        } else if (cursor->GetType() == EYsonItemType::BeginList && dictOrListItemAccessor->Type == EValueType::Int64) {
            if (!ParseListUntilIndex(&cursor, dictOrListItemAccessor->Data.Int64)) {
                return;
            }
        } else {
            return;
        }
    }

    switch (resultType) {
        case EValueType::Null:
            break;

        case EValueType::Int64:
            if (auto parsed = TryParseValue<i64>(&cursor)) {
                MakePositionIndependentInt64Value(ConvertPointerFromWasmToHost(result), *parsed);
            }
            break;

        case EValueType::Uint64:
            if (auto parsed = TryParseValue<ui64>(&cursor)) {
                MakePositionIndependentUint64Value(ConvertPointerFromWasmToHost(result), *parsed);
            }
            break;

        case EValueType::Double:
            if (auto parsed = TryParseValue<double>(&cursor)) {
                MakePositionIndependentDoubleValue(ConvertPointerFromWasmToHost(result), *parsed);
            }
            break;

        case EValueType::Boolean:
            if (auto parsed = TryParseValue<bool>(&cursor)) {
                MakePositionIndependentBooleanValue(ConvertPointerFromWasmToHost(result), *parsed);
            }
            break;

        case EValueType::String:
            if (auto parsed = TryParseValue<TString>(&cursor)) {
                NDetail::CopyString(context, result, *parsed);
            }
            break;

        case EValueType::Any:
        case EValueType::Composite: {
            auto parsed = ParseAnyValue(&cursor);
            NDetail::CopyAny(context, result, parsed);
            ConvertPointerFromWasmToHost(result)->Type = resultType;
            break;
        }

        default:
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

int CompareYsonValuesHelper(const char* lhsOffset, ui32 lhsLength, const char* rhsOffset, ui32 rhsLength)
{
    auto* lhs = ConvertPointerFromWasmToHost(lhsOffset, lhsLength);
    auto* rhs = ConvertPointerFromWasmToHost(rhsOffset, rhsLength);
    return NYT::NTableClient::CompareYsonValues(
        TYsonStringBuf(TStringBuf(lhs, lhsLength)),
        TYsonStringBuf(TStringBuf(rhs, rhsLength)));
}

ui64 HashYsonValueHelper(const char* dataOffset, ui32 length)
{
    auto* data = ConvertPointerFromWasmToHost(dataOffset, length);
    return NYT::NTableClient::CompositeFarmHash(TYsonStringBuf(TStringBuf(data, length)));
}

int CompareAny(char* lhsData, i32 lhsLength, char* rhsData, i32 rhsLength)
{
    return CompareYsonValuesHelper(lhsData, lhsLength, rhsData, rhsLength);
}

void ValidateYsonHelper(const char* offset, ui32 length)
{
    auto* data = ConvertPointerFromWasmToHost(offset, length);
    ValidateYson(TYsonStringBuf(TStringBuf(data, length)), /*nestingLevelLimit*/ 256);
}

////////////////////////////////////////////////////////////////////////////////

int memcmp(const void* firstOffset, const void* secondOffset, std::size_t count) // NOLINT
{
    auto* first = ConvertPointerFromWasmToHost(std::bit_cast<char*>(firstOffset), count);
    auto* second = ConvertPointerFromWasmToHost(std::bit_cast<char*>(secondOffset), count);
    return ::memcmp(first, second, count);
}

struct tm* gmtime_r(const time_t* time, struct tm* result) // NOLINT
{
    auto* gmtime = GmTimeR(ConvertPointerFromWasmToHost(time), ConvertPointerFromWasmToHost(result));
    return ConvertPointerFromHostToWasm(gmtime);
}

// This code is borrowed from bigb_hash.cpp.
// It will be removed after full cross-compilation support.
uint64_t BigBHashImpl(char* s, int len)
{
    s = ConvertPointerFromWasmToHost(s);

    TStringBuf uid{s, static_cast<size_t>(len)};
    if (uid.length() == 0) {
        return 0;
    }
    ui64 ans;
    if (uid[0] == 'y' && TryFromString(uid.SubStr(1), ans)) {
        return ans;
    }
    return MultiHash(TStringBuf{"shard"}, uid);
}

void AddRegexToFunctionContext(TFunctionContext* context, re2::RE2* regex) // NOLINT
{
    auto* ptr = context->CreateUntypedObject(regex, [] (void* re) {
        delete static_cast<re2::RE2*>(re);
    });
    context->SetPrivateData(ptr);
}

// bool TFunctionContext::IsLiteralArg(int) const
bool _ZNK3NYT12NQueryClient16TFunctionContext12IsLiteralArgEi(TFunctionContext* context, int index) // NOLINT
{
    return context->IsLiteralArg(index);
}

// void* TFunctionContext::GetPrivateData() const
void* _ZNK3NYT12NQueryClient16TFunctionContext14GetPrivateDataEv(TFunctionContext* context) // NOLINT
{
    return context->GetPrivateData();
}

} // namespace NRoutines

////////////////////////////////////////////////////////////////////////////////

NCodegen::TRoutineRegistry NativeRegistry;
NCodegen::TRoutineRegistry WebAssemblyRegistry;

////////////////////////////////////////////////////////////////////////////////

template <class TSignature>
struct TMakeWebAssemblyIntrinsic;

template <class TResult, class... TArgs>
struct TMakeWebAssemblyIntrinsic<TResult(TArgs...)>
{
    template <TResult(*FunctionPtr)(TArgs...)>
    static TResult Wrapper(WAVM::Runtime::ContextRuntimeData*, TArgs... args)
    {
        auto* compartmentBeforeCall = GetCurrentCompartment();
        auto finally = Finally([&] {
            auto* compartmentAfterCall = GetCurrentCompartment();
            YT_VERIFY(compartmentBeforeCall == compartmentAfterCall);
        });
        return FunctionPtr(args...);
    }
};

#define REGISTER_WEB_ASSEMBLY_INTRINSIC(intrinsic) \
    constexpr auto Intrinsic##intrinsic = &TMakeWebAssemblyIntrinsic<decltype(NRoutines::intrinsic)>::Wrapper<&NRoutines::intrinsic>; \
    static WAVM::Intrinsics::Function IntrinsicFunction##intrinsic( \
        NWebAssembly::getIntrinsicModule_standard(), \
        #intrinsic, \
        (void*)Intrinsic##intrinsic, \
        WAVM::IR::FunctionType(WAVM::IR::FunctionType::Encoding{ \
            std::bit_cast<WAVM::Uptr>(NWebAssembly::TFunctionTypeBuilder<true, decltype(NRoutines::intrinsic) >::Get()) \
        }));

////////////////////////////////////////////////////////////////////////////////

template <class TResult, class... TArgs>
struct RegisterLLVMRoutine
{
    RegisterLLVMRoutine(const char *symbol, bool onlyWebAssembly, TResult(*functionPointer)(TArgs...))
    {
        if (!onlyWebAssembly) {
            NativeRegistry.RegisterRoutine(symbol, functionPointer);
        }

        WebAssemblyRegistry.RegisterRoutine(symbol, functionPointer);
    }
};

#define REGISTER_LLVM_ROUTINE(routine, onlyWebAssembly) \
    static auto RegisteredLLVM##routine = RegisterLLVMRoutine(#routine, onlyWebAssembly, NRoutines::routine);

////////////////////////////////////////////////////////////////////////////////

#define REGISTER_ROUTINE(routine) \
    REGISTER_LLVM_ROUTINE(routine, /*onlyWebAssembly*/ false) \
    REGISTER_WEB_ASSEMBLY_INTRINSIC(routine)

#define REGISTER_YPATH_GET_ROUTINE(TYPE) \
    REGISTER_ROUTINE(TryGet ## TYPE); \
    REGISTER_ROUTINE(Get ## TYPE)

#define REGISTER_WEB_ASSEMBLY_INTRINSIC_IN_LLVM(routine, ...) \
    REGISTER_LLVM_ROUTINE(routine, /*onlyWebAssembly*/ true)

////////////////////////////////////////////////////////////////////////////////

#define REGISTER_TIME_ROUTINE(routine) \
    REGISTER_ROUTINE(routine); \
    REGISTER_ROUTINE(routine ## Localtime)

////////////////////////////////////////////////////////////////////////////////

REGISTER_ROUTINE(WriteRow);
REGISTER_ROUTINE(InsertGroupRow);
REGISTER_ROUTINE(ScanOpHelper);
REGISTER_ROUTINE(WriteOpHelper);
REGISTER_ROUTINE(AllocateJoinKeys);
REGISTER_ROUTINE(AllocateAlignedBytes);
REGISTER_ROUTINE(StorePrimaryRow);
REGISTER_ROUTINE(MultiJoinOpHelper);
REGISTER_ROUTINE(ArrayJoinOpHelper);
REGISTER_ROUTINE(GroupOpHelper);
REGISTER_ROUTINE(GroupTotalsOpHelper);
REGISTER_ROUTINE(StringHash);
REGISTER_ROUTINE(AllocatePermanentRow);
REGISTER_ROUTINE(AllocateBytes);
REGISTER_ROUTINE(IsRowInRowset);
REGISTER_ROUTINE(IsRowInRanges);
REGISTER_ROUTINE(TransformTuple);
// REGISTER_ROUTINE(SimpleHash);
// REGISTER_ROUTINE(FarmHashUint64);
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
REGISTER_ROUTINE(MakeMap);
REGISTER_ROUTINE(MakeList);
REGISTER_ROUTINE(ListContains);
REGISTER_ROUTINE(ListHasIntersection);
REGISTER_ROUTINE(AnyToYsonString);
REGISTER_ROUTINE(NumericToString);
REGISTER_ROUTINE(StringToInt64);
REGISTER_ROUTINE(StringToUint64);
REGISTER_ROUTINE(StringToDouble);
REGISTER_ROUTINE(HyperLogLogAllocate);
REGISTER_ROUTINE(HyperLogLogAdd);
REGISTER_ROUTINE(HyperLogLogMerge);
REGISTER_ROUTINE(HyperLogLogEstimateCardinality);
REGISTER_ROUTINE(HyperLogLogGetFingerprint);
REGISTER_ROUTINE(StoredReplicaSetMerge);
REGISTER_ROUTINE(StoredReplicaSetFinalize);
REGISTER_ROUTINE(LastSeenReplicaSetMerge);
REGISTER_ROUTINE(HasPermissions);
REGISTER_ROUTINE(YsonLength);
REGISTER_ROUTINE(LikeOpHelper);
REGISTER_ROUTINE(CompositeMemberAccessorHelper);
REGISTER_ROUTINE(DictSumIteration);
REGISTER_ROUTINE(CompareYsonValuesHelper);
REGISTER_ROUTINE(HashYsonValueHelper);
REGISTER_ROUTINE(CompareAny);
REGISTER_ROUTINE(ValidateYsonHelper);
REGISTER_TIME_ROUTINE(TimestampFloorHour);
REGISTER_TIME_ROUTINE(TimestampFloorDay);
REGISTER_TIME_ROUTINE(TimestampFloorWeek);
REGISTER_TIME_ROUTINE(TimestampFloorMonth);
REGISTER_TIME_ROUTINE(TimestampFloorQuarter);
REGISTER_TIME_ROUTINE(TimestampFloorYear);
REGISTER_ROUTINE(FormatTimestamp);

REGISTER_ROUTINE(memcmp);
REGISTER_ROUTINE(gmtime_r);
REGISTER_ROUTINE(BigBHashImpl);
REGISTER_ROUTINE(AddRegexToFunctionContext);
REGISTER_ROUTINE(_ZNK3NYT12NQueryClient16TFunctionContext12IsLiteralArgEi);
REGISTER_ROUTINE(_ZNK3NYT12NQueryClient16TFunctionContext14GetPrivateDataEv);

REGISTER_WEB_ASSEMBLY_INTRINSIC_IN_LLVM(emscripten_notify_memory_growth);

FOREACH_WEB_ASSEMBLY_SYSCALL(REGISTER_WEB_ASSEMBLY_INTRINSIC_IN_LLVM);

////////////////////////////////////////////////////////////////////////////////

NCodegen::TRoutineRegistry* GetQueryRoutineRegistry(NCodegen::EExecutionBackend backend)
{
    switch (backend) {
        case NCodegen::EExecutionBackend::Native:
            return &NativeRegistry;
        case NCodegen::EExecutionBackend::WebAssembly:
            return &WebAssemblyRegistry;
        default:
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
