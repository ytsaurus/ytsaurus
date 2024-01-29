#include "cg_routines.h"
#include "cg_types.h"

#include <yt/yt/library/web_assembly/api/compartment.h>
#include <yt/yt/library/web_assembly/api/function.h>

#include <yt/yt/library/query/base/private.h>

#include <yt/yt/library/query/engine_api/position_independent_value.h>
#include <yt/yt/library/query/engine_api/position_independent_value_transfer.h>

#include <yt/yt/client/security_client/acl.h>
#include <yt/yt/client/security_client/helpers.h>

#include <yt/yt/client/query_client/query_statistics.h>

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

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = QueryClientLogger;

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

////////////////////////////////////////////////////////////////////////////////

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

using TRowsConsumer = bool (*)(void** closure, TExpressionContext*, const TPIValue** rows, i64 size);
using TUnversionedRowsConsumer = bool (*)(void** closure, TExpressionContext*, const TValue** rows, i64 size);

bool WriteRow(TExecutionContext* context, TWriteOpClosure* closure, TPIValue* values)
{
    CHECK_STACK();

    auto* statistics = context->Statistics;

    if (statistics->RowsWritten >= context->OutputRowLimit) {
        throw TInterruptedIncompleteException();
    }

    ++statistics->RowsWritten;

    auto& batch = closure->OutputRowsBatch;

    auto& outputContext = closure->OutputContext;

    YT_ASSERT(batch.size() < WriteRowsetSize);

    batch.push_back(CopyAndConvertFromPI(&outputContext, MakeRange(values, closure->RowSize)));

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
            WaitForFast(writer->GetReadyEvent())
                .ThrowOnError();
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

    auto finalLogger = Finally([&] () {
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

    auto scanContext = MakeExpressionContext(TIntermediateBufferTag());
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
            WaitForFast(reader->GetReadyEvent())
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
        statistics->DataWeightRead += rowSchemaInformation->RowWeightWithNoStrings * rows.size();

        for (auto row : rows) {
            values.push_back(row.Begin());

            for (int index : rowSchemaInformation->StringLikeIndices) {
                statistics->DataWeightRead += row[index].Type == EValueType::Null
                    ? 0
                    : row[index].Length;
            }
        }

        interrupt |= consumeRows(consumeRowsClosure, &scanContext, values.data(), values.size());

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
    return context->AllocateAligned(byteCount);
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
        auto& item = closure->Items[joinId];
        char* data = AllocateAlignedBytes(
            &item.Context,
            GetUnversionedRowByteSize(item.KeySize) + sizeof(TSlot));
        auto row = TMutableRow::Create(data, item.KeySize);
        keyPtrs[joinId] = reinterpret_cast<TPIValue*>(row.Begin());
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

    closure->PrimaryRows.emplace_back(*primaryValues);

    for (size_t columnIndex = 0; columnIndex < closure->PrimaryRowSize; ++columnIndex) {
        CapturePIValue(&closure->Context, *primaryValues + columnIndex);
    }

    for (size_t joinId = 0; joinId < closure->Items.size(); ++joinId) {
        auto keyPtr = keysPtr + joinId;
        auto& item = closure->Items[joinId];
        TPIValue* key = *keyPtr;

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
                CapturePIValue(&closure->Items[joinId].Context, &key[columnIndex]);
            }

            char* data = AllocateAlignedBytes(
                &item.Context,
                GetUnversionedRowByteSize(item.KeySize) + sizeof(TSlot));
            auto row = TMutableRow::Create(data, item.KeySize);
            *keyPtr = reinterpret_cast<TPIValue*>(row.Begin());
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
                &item.Context,
                GetUnversionedRowByteSize(item.KeySize) + sizeof(TSlot));
            auto row = TMutableRow::Create(data, item.KeySize);
            keysPtr[joinId] = reinterpret_cast<TPIValue*>(row.Begin());
        }
    }

    size_t primaryRowSize = closure->PrimaryRowSize * sizeof(TValue) + sizeof(TSlot*) * closure->Items.size();

    *primaryValues = reinterpret_cast<TPIValue*>(AllocateAlignedBytes(&closure->Context, primaryRowSize));

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
    auto comparers = NDetail::MakeJoinComparersCallbacks(MakeRange(comparersOffsets, parameters->Items.size()));
    auto collectRows = PrepareFunction(collectRowsFunction);
    auto consumeRows = PrepareFunction(consumeRowsFunction);

    auto finalLogger = Finally([&] () {
        YT_LOG_DEBUG("Finalizing multijoin helper");
    });

    TMultiJoinClosure closure;
    closure.Context = MakeExpressionContext(TPermanentBufferTag(), context->MemoryChunkProvider);
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

    closure.ProcessJoinBatch = [&] () {
        if (finished) {
            return true;
        }

        YT_LOG_DEBUG("Joining started");
        auto finalLogger = Finally([&] {
            YT_LOG_DEBUG("Joining finished");
        });

        auto foreignContext = MakeExpressionContext(TForeignExecutorBufferTag());

        std::vector<ISchemafulUnversionedReaderPtr> readers;
        for (size_t joinId = 0; joinId < closure.Items.size(); ++joinId) {
            closure.ProcessSegment(joinId);

            std::vector<TPIValueRange> orderedKeys;
            orderedKeys.reserve(closure.Items[joinId].OrderedKeys.size());
            for (auto* key : closure.Items[joinId].OrderedKeys) {
                // NB: Flags are neither set from TCG value nor cleared during row allocation.
                size_t id = 0;
                for (auto* value = key; value < key + closure.Items[joinId].KeySize; ++value) {
                    value->Flags = {};
                    value->Id = id++;
                }
                auto row = TRow(reinterpret_cast<const TUnversionedRowHeader*>(key) - 1);
                orderedKeys.emplace_back(key, row.GetCount());
            }

            auto foreignExecutorCopy = CopyAndConvertFromPI(&foreignContext, orderedKeys);
            auto reader = parameters->Items[joinId].ExecuteForeign(
                foreignExecutorCopy,
                foreignContext.GetRowBuffer());
            readers.push_back(reader);

            closure.Items[joinId].Lookup.clear();
            closure.Items[joinId].LastKey = nullptr;
        }

        TYielder yielder;

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
                    auto captured = closure.Context.CaptureRow(row);
                    auto asPositionIndependent = InplaceConvertToPI(captured);
                    foreignValues.push_back(asPositionIndependent.Begin());
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

        auto intermediateContext = MakeExpressionContext(TIntermediateBufferTag());
        std::vector<const TPIValue*> joinedRows;

        i64 processedRows = 0;

        auto consumeJoinedRows = [&] () -> bool {
            // Consume joined rows.
            processedRows += joinedRows.size();
            bool finished = consumeRows(
                consumeRowsClosure,
                &intermediateContext,
                joinedRows.data(),
                joinedRows.size());
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
                auto joinedRow = AllocatePIValueRange(&intermediateContext, resultRowSize);
                for (size_t index = 0; index < closure.PrimaryRowSize; ++index) {
                    CopyPositionIndependent(&joinedRow[index], rowValues[index]);
                }

                size_t offset = closure.PrimaryRowSize;
                for (size_t joinId = 0; joinId < closure.Items.size(); ++joinId) {
                    TSlot slot = *(reinterpret_cast<TSlot**>(rowValues + closure.PrimaryRowSize)[joinId]);
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
                            CopyPositionIndependent(&joinedRow[offset++], foreignRow[columnIndex]);
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
                            MakePositionIndependentSentinelValue(&joinedRow[offset++], EValueType::Null);
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

bool ArrayJoinOpHelper(
    TExpressionContext* context,
    TArrayJoinParameters* parameters,
    TPIValue* row,
    TPIValue* arrays,
    void** consumeRowsClosure,
    TRowsConsumer consumeRowsFunction)
{
    auto consumeRows = PrepareFunction(consumeRowsFunction);

    int arrayCount = parameters->FlattenedTypes.size();
    std::vector<const TPIValue*> rows;
    std::vector<TPIValueRange> flattenedArrays(arrayCount);
    size_t maxArraySize = 0;

    std::vector<TUnversionedValue> unpackedItems;

    for (int index = 0; index < arrayCount; ++index) {
        if (arrays[index].Type == EValueType::Null) {
            continue;
        }
        TMemoryInput memoryInput(FromPositionIndependentValue<NYson::TYsonStringBuf>(arrays[index]).AsStringBuf());

        TYsonPullParser parser(&memoryInput, EYsonType::Node);
        TYsonPullParserCursor cursor(&parser);

        auto listItemType = parameters->FlattenedTypes[index];


        unpackedItems.clear();

        cursor.ParseList([&] (TYsonPullParserCursor* cursor) {
            auto currentType = cursor->GetCurrent().GetType();
            if (currentType == NYson::EYsonItemType::EntityValue) {
                unpackedItems.emplace_back(MakeUnversionedNullValue());
                cursor->Next();
                return;
            }
            switch (listItemType) {
                case EValueType::Int64: {
                    THROW_ERROR_EXCEPTION_IF(
                        currentType != EYsonItemType::Int64Value,
                        "Type mismatch in array join");

                    auto value = cursor->GetCurrent().UncheckedAsInt64();
                    unpackedItems.emplace_back(MakeUnversionedInt64Value(value));
                    break;
                }
                case EValueType::Uint64: {
                    THROW_ERROR_EXCEPTION_IF(
                        currentType != EYsonItemType::Uint64Value,
                        "Type mismatch in array join");

                    auto value = cursor->GetCurrent().UncheckedAsUint64();
                    unpackedItems.emplace_back(MakeUnversionedUint64Value(value));
                    break;
                }
                case EValueType::Double: {
                    THROW_ERROR_EXCEPTION_IF(
                        currentType != EYsonItemType::DoubleValue,
                        "Type mismatch in array join");

                    auto value = cursor->GetCurrent().UncheckedAsDouble();
                    unpackedItems.emplace_back(MakeUnversionedDoubleValue(value));
                    break;
                }
                case EValueType::String: {
                    THROW_ERROR_EXCEPTION_IF(
                        currentType != EYsonItemType::StringValue,
                        "Type mismatch in array join");

                    auto value = cursor->GetCurrent().UncheckedAsString();
                    unpackedItems.emplace_back(MakeUnversionedStringValue(value));
                    break;

                }
                default:
                    YT_ABORT();
            }
            cursor->Next();
        });

        maxArraySize = std::max(maxArraySize, unpackedItems.size());
        flattenedArrays[index] = CapturePIValueRange(context, MakeRange(unpackedItems), true);
    }

    if (maxArraySize == 0 && parameters->IsLeft) {
        maxArraySize = 1;
    }

    rows.reserve(maxArraySize);

    for (int rowIndex = 0; rowIndex < static_cast<int>(maxArraySize); ++rowIndex) {
        auto joinedRow = AllocatePIValueRange(
            context,
            parameters->SelfJoinedColumns.size() + parameters->ArrayJoinedColumns.size());

        int joinedRowIndex = 0;
        for (int index : parameters->SelfJoinedColumns) {
            CopyPositionIndependent(&joinedRow[joinedRowIndex++], row[index]);
        }
        for (int index : parameters->ArrayJoinedColumns) {
            if (rowIndex >= static_cast<int>(flattenedArrays[index].Size())) {
                MakePositionIndependentNullValue(&joinedRow[joinedRowIndex++]);
            } else {
                CopyPositionIndependent(&joinedRow[joinedRowIndex++], flattenedArrays[index][rowIndex]);
            }
        }

        rows.push_back(joinedRow.Begin());
    }

    return consumeRows(consumeRowsClosure, context, rows.data(), rows.size());
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
        int keySize,
        bool shouldCheckForNullGroupKey,
        bool allAggregatesAreFirst,
        void** consumeIntermediateClosure,
        TWebAssemblyRowsConsumer consumeIntermediate,
        void** consumeFinalClosure,
        TWebAssemblyRowsConsumer consumeFinal,
        void** consumeDeltaFinalClosure,
        TWebAssemblyRowsConsumer consumeDeltaFinal,
        void** consumeTotalsClosure,
        TWebAssemblyRowsConsumer consumeTotals);

    // If the stream tag has changed, we flush to keep the segments sorted by primary key.
    Y_FORCE_INLINE void UpdateTagAndFlushIfNeeded(const TExecutionContext* context, EStreamTag tag);

    // The grouping key cannot be null if `with totals` is used,
    // since the result of `totals` is contained in a row with a null group key.
    Y_FORCE_INLINE void ValidateGroupKeyIsNotNull(TPIValue* row) const;

    // If the common prefix of primary key and group key has changed,
    // no new lines can be added to the grouping. Thus, we can flush.
    Y_FORCE_INLINE void FlushIfCurrentGroupSetIsFinished(const TExecutionContext* context, TPIValue* row);

    // If the request exceeds the given memory limit, we interrupt processing.
    // NB: We do not guarantee the correctness of the result.
    Y_FORCE_INLINE void ValidateGroupedRowCount(i64 groupRowLimit) const;

    // Early query execution finish if input is ordered and we reached `limit` + `offset` rows.
    Y_FORCE_INLINE bool IsUserRowLimitReached(const TExecutionContext* context) const;

    // If all aggregate functions are `first()`, we can stop the query execution when the limit is reached.
    Y_FORCE_INLINE bool AreAllAggregatesFirst() const;

    // If row's group key exists in the lookup table, the row should be grouped.
    // Returns grouped row in the lookup table.
    Y_FORCE_INLINE const TPIValue* InsertIfRowBelongsToExistingGroup(const TExecutionContext* context, TPIValue* row);

    // Returns grouped row in the lookup table.
    Y_FORCE_INLINE const TPIValue* InsertIntermediate(const TExecutionContext* context, TPIValue* row);

    // Returns row itself.
    Y_FORCE_INLINE const TPIValue* InsertFinal(const TExecutionContext* context, TPIValue* row);

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

private:
    TExpressionContext Context_;
    TExpressionContext FinalContext_;
    TExpressionContext TotalsContext_;

    // The grouping key and the primary key may have a common prefix of length P.
    // This function compares prefixes of length P.
    const NWebAssembly::TCompartmentFunction<TComparerFunction> PrefixEqComparer_;

    TLookupRows GroupedIntermediateRows_;
    const int KeySize_;

    // When executing a query with `with totals`, we store data for `totals` using the null grouping key.
    // Thus, rows with a null grouping key should lead to an execution error.
    const bool ShouldCheckForNullGroupKey_;

    // If all aggregate functions are `first()`, we can stop the query execution when the limit is reached.
    const bool AllAggregatesAreFirst_;

    void** const ConsumeIntermediateClosure_;
    const TWebAssemblyRowsConsumer ConsumeIntermediate_;

    void** const ConsumeFinalClosure_;
    const TWebAssemblyRowsConsumer ConsumeFinal_;

    void** const ConsumeDeltaFinalClosure_;
    const TWebAssemblyRowsConsumer ConsumeDeltaFinal_;

    void** const ConsumeTotalsClosure_;
    const TWebAssemblyRowsConsumer ConsumeTotals_;

    const TPIValue* LastKey_ = nullptr;
    std::vector<const TPIValue*> Intermediate_;
    std::vector<const TPIValue*> Final_;
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
    TExpressionContext FlushContext_ = MakeExpressionContext(TIntermediateBufferTag());

    template <typename TFlushFunction>
    Y_FORCE_INLINE void FlushWithBatching(
        const TExecutionContext* context,
        const TPIValue** begin,
        const TPIValue** end,
        const TFlushFunction& flush);

    Y_FORCE_INLINE void FlushIntermediate(const TExecutionContext* context, const TPIValue** begin, const TPIValue** end);
    Y_FORCE_INLINE void FlushFinal(const TExecutionContext* context, const TPIValue** begin, const TPIValue** end);
    Y_FORCE_INLINE void FlushDeltaFinal(const TExecutionContext* context, const TPIValue** begin, const TPIValue** end);
    Y_FORCE_INLINE void FlushTotals(const TExecutionContext* context, const TPIValue** begin, const TPIValue** end);
};

TGroupByClosure::TGroupByClosure(
    IMemoryChunkProviderPtr chunkProvider,
    NWebAssembly::TCompartmentFunction<TComparerFunction> prefixEqComparer,
    NWebAssembly::TCompartmentFunction<THasherFunction> groupHasher,
    NWebAssembly::TCompartmentFunction<TComparerFunction> groupComparer,
    int keySize,
    bool shouldCheckForNullGroupKey,
    bool allAggregatesAreFirst,
    void** consumeIntermediateClosure,
    TWebAssemblyRowsConsumer consumeIntermediate,
    void** consumeFinalClosure,
    TWebAssemblyRowsConsumer consumeFinal,
    void** consumeDeltaFinalClosure,
    TWebAssemblyRowsConsumer consumeDeltaFinal,
    void** consumeTotalsClosure,
    TWebAssemblyRowsConsumer consumeTotals)
    : Context_(MakeExpressionContext(TPermanentBufferTag(), chunkProvider))
    , FinalContext_(MakeExpressionContext(TPermanentBufferTag(), chunkProvider))
    , TotalsContext_(MakeExpressionContext(TPermanentBufferTag(), chunkProvider))
    , PrefixEqComparer_(prefixEqComparer)
    , GroupedIntermediateRows_(
        InitialGroupOpHashtableCapacity,
        groupHasher,
        groupComparer)
    , KeySize_(keySize)
    , ShouldCheckForNullGroupKey_(shouldCheckForNullGroupKey)
    , AllAggregatesAreFirst_(allAggregatesAreFirst)
    , ConsumeIntermediateClosure_(consumeIntermediateClosure)
    , ConsumeIntermediate_(consumeIntermediate)
    , ConsumeFinalClosure_(consumeFinalClosure)
    , ConsumeFinal_(consumeFinal)
    , ConsumeDeltaFinalClosure_(consumeDeltaFinalClosure)
    , ConsumeDeltaFinal_(consumeDeltaFinal)
    , ConsumeTotalsClosure_(consumeTotalsClosure)
    , ConsumeTotals_(consumeTotals)
{
    GroupedIntermediateRows_.set_empty_key(nullptr);
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

    if (std::all_of(
        &row[0],
        &row[KeySize_],
        [] (const TPIValue& value) {
            return value.Type == EValueType::Null;
        }))
    {
        THROW_ERROR_EXCEPTION("Null values are forbidden in group key");
    }
}

void TGroupByClosure::FlushIfCurrentGroupSetIsFinished(const TExecutionContext* context, TPIValue* row)
{
    // NB: if !context->Ordered then PrefixEqComparer_ never lets flush.
    if (LastKey_ && !PrefixEqComparer_(row, LastKey_)) {
        Flush(context, EStreamTag::Intermediate);
    }
}

void TGroupByClosure::ValidateGroupedRowCount(i64 groupRowLimit) const
{
    if (std::ssize(Intermediate_) == groupRowLimit) {
        throw TInterruptedIncompleteException();
    }
}

bool TGroupByClosure::IsUserRowLimitReached(const TExecutionContext* context) const
{
    // NB: We do not support `having` with `limit` yet.
    // If query uses `having`, skipping rows here is incorrect because `having` filters rows.

    // NB: Our semantics of `totals` operation allows to stop grouping when the limit is reached.

    return context->Ordered &&
        GroupedRowCount_ >= context->Offset + context->Limit;
}

bool TGroupByClosure::AreAllAggregatesFirst() const
{
    return AllAggregatesAreFirst_;
}

const TPIValue* TGroupByClosure::InsertIfRowBelongsToExistingGroup(const TExecutionContext* context, TPIValue* row)
{
    YT_VERIFY(GroupedRowCount_ == context->Offset + context->Limit);
    auto it = GroupedIntermediateRows_.find(row);
    if (it != GroupedIntermediateRows_.end()) {
        return *it;
    }
    return nullptr;
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

        for (int index = 0; index < KeySize_; ++index) {
            CapturePIValue(&Context_, &row[index]);
        }
    }

    return *it;
}

const TPIValue* TGroupByClosure::InsertFinal(const TExecutionContext* /*context*/, TPIValue* row)
{
    LastKey_ = row;

    Final_.push_back(row);
    ++GroupedRowCount_;

    for (int index = 0; index < KeySize_; ++index) {
        CapturePIValue(&FinalContext_, &row[index]);
    }

    return row;
}

const TPIValue* TGroupByClosure::InsertTotals(const TExecutionContext* /*context*/, TPIValue* row)
{
    // |LastKey_| should not be updated because bordering intermediate streams should be merged.

    Totals_.push_back(row);

    for (int index = 0; index < KeySize_; ++index) {
        CapturePIValue(&TotalsContext_, &row[index]);
    }

    return row;
}

void TGroupByClosure::Flush(const TExecutionContext* context, EStreamTag incomingTag)
{
    if (Y_UNLIKELY(CurrentSegment_ == EGroupOpProcessingStage::RightBorder)) {
        if (!Final_.empty()) {
            FlushFinal(context, Final_.data(), Final_.data() + Final_.size());
            Final_.clear();
        }

        if (!Intermediate_.empty()) {
            // Can be non-null in last call.
            i64 innerCount = Intermediate_.size() - GroupedIntermediateRows_.size();

            FlushDeltaFinal(context, Intermediate_.data(), Intermediate_.data() + innerCount);
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
        case EStreamTag::Final: {
            FlushFinal(context, Final_.data(), Final_.data() + Final_.size());
            Final_.clear();
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
                FlushDeltaFinal(context, Intermediate_.data(), Intermediate_.data() + Intermediate_.size());
                Intermediate_.clear();
            } else if (Y_UNLIKELY(incomingTag == EStreamTag::Final)) {
                FlushDeltaFinal(context, Intermediate_.data(), Intermediate_.data() + Intermediate_.size());
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
    return Intermediate_.empty() && Final_.empty() && Totals_.empty();
}

template <typename TFlushFunction>
void TGroupByClosure::FlushWithBatching(
    const TExecutionContext* context,
    const TPIValue** begin,
    const TPIValue** end,
    const TFlushFunction& flush)
{
    bool finished = false;

    // FIXME(dtorilov): We cannot skip intermediate rows for ordered queries
    // (when the grouping key is a prefix of primary key),
    // since intermediate rows from the beginning of the processed range
    // can be grouped with rows of the previous range.

    if (context->Ordered && ProcessedRows_ < context->Offset) {
        i64 skip = std::min(context->Offset - ProcessedRows_, end - begin);
        ProcessedRows_ += skip;
        begin += skip;
    }

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

void TGroupByClosure::FlushFinal(const TExecutionContext* context, const TPIValue** begin, const TPIValue** end)
{
    auto flush = [this] (TExpressionContext* context, const TPIValue** begin, i64 size) {
        return ConsumeFinal_(ConsumeFinalClosure_, context, begin, size);
    };

    FlushWithBatching(context, begin, end, flush);
}

void TGroupByClosure::FlushDeltaFinal(const TExecutionContext* context, const TPIValue** begin, const TPIValue** end)
{
    auto flush = [this] (TExpressionContext* context, const TPIValue** begin, i64 size) {
        return ConsumeDeltaFinal_(ConsumeDeltaFinalClosure_, context, begin, size);
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
const TPIValue* InsertGroupRow(
    TExecutionContext* context,
    TGroupByClosure* closure,
    TPIValue* row,
    ui64 rowTagAsUint)
{
    auto rowTag = static_cast<EStreamTag>(rowTagAsUint);

    closure->UpdateTagAndFlushIfNeeded(context, rowTag);

    switch (rowTag) {
        case EStreamTag::Final: {
            if (closure->IsUserRowLimitReached(context)) {
                return nullptr;
            }

            closure->InsertFinal(context, row);
            return row;
        }

        case EStreamTag::Intermediate: {
            closure->FlushIfCurrentGroupSetIsFinished(context, row);

            closure->ValidateGroupedRowCount(context->GroupRowLimit);

            if (closure->IsUserRowLimitReached(context)) {
                if (closure->AreAllAggregatesFirst()) {
                    return nullptr;
                }

                return closure->InsertIfRowBelongsToExistingGroup(context, row);
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
    int keySize,
    int /*valuesCount*/,
    bool shouldCheckForNullGroupKey,
    bool allAggregatesAreFirst,
    void** collectRowsClosure,
    TGroupCollector collectRowsFunction,
    void** consumeIntermediateClosure,
    TRowsConsumer consumeIntermediateFunction,
    void** consumeFinalClosure,
    TRowsConsumer consumeFinalFunction,
    void** consumeDeltaFinalClosure,
    TRowsConsumer consumeDeltaFinalFunction,
    void** consumeTotalsClosure,
    TRowsConsumer consumeTotalsFunction)
{
    auto collectRows = PrepareFunction(collectRowsFunction);
    auto prefixEqComparer = PrepareFunction(prefixEqComparerFunction);
    auto groupHasher = PrepareFunction(groupHasherFunction);
    auto groupComparer = PrepareFunction(groupComparerFunction);
    auto consumeIntermediate = PrepareFunction(consumeIntermediateFunction);
    auto consumeFinal = PrepareFunction(consumeFinalFunction);
    auto consumeDeltaFinal = PrepareFunction(consumeDeltaFinalFunction);
    auto consumeTotals = PrepareFunction(consumeTotalsFunction);

    TGroupByClosure closure(
        context->MemoryChunkProvider,
        prefixEqComparer,
        groupHasher,
        groupComparer,
        keySize,
        shouldCheckForNullGroupKey,
        allAggregatesAreFirst,
        consumeIntermediateClosure,
        consumeIntermediate,
        consumeFinalClosure,
        consumeFinal,
        consumeDeltaFinalClosure,
        consumeDeltaFinal,
        consumeTotalsClosure,
        consumeTotals);

    auto finalLogger = Finally([&] () {
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

    YT_VERIFY(closure.IsFlushed());
}

////////////////////////////////////////////////////////////////////////////////

using TGroupTotalsCollector = void(*)(void** closure, TExpressionContext* context);

void GroupTotalsOpHelper(
    TExecutionContext* /*context*/,
    void** collectRowsClosure,
    TGroupTotalsCollector collectRowsFunction)
{
    auto context = MakeExpressionContext(TIntermediateBufferTag());
    auto collectRows = PrepareFunction(collectRowsFunction);
    collectRows(collectRowsClosure, &context);
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

    auto finalLogger = Finally([&] () {
        YT_LOG_DEBUG("Finalizing order helper");
    });

    auto limit = context->Offset + context->Limit;

    TTopCollector topCollector(limit, comparer, rowSize, context->MemoryChunkProvider);
    collectRows(collectRowsClosure, &topCollector);
    auto rows = topCollector.GetRows();

    auto consumerContext = MakeExpressionContext(TIntermediateBufferTag());

    TYielder yielder;
    size_t processedRows = 0;

    auto rowCount = static_cast<i64>(rows.size());
    for (i64 index = context->Offset; index < rowCount; index += RowsetProcessingBatchSize) {
        auto size = std::min(RowsetProcessingBatchSize, rowCount - index);
        processedRows += size;

        bool finished = consumeRows(consumeRowsClosure, &consumerContext, rows.data() + index, size);
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
            shouldNotWait = writer->Write(batch);
        }

        if (!shouldNotWait) {
            TValueIncrementingTimingGuard<TWallTimer> timingGuard(&context->Statistics->WaitOnReadyEventTime);
            WaitForFast(writer->GetReadyEvent())
                .ThrowOnError();
        }
    }

    YT_LOG_DEBUG("Closing writer");
    {
        TValueIncrementingTimingGuard<TWallTimer> timingGuard(&context->Statistics->WaitOnReadyEventTime);
        WaitForFast(context->Writer->Close())
            .ThrowOnError();
    }
}

char* AllocateBytes(TExpressionContext* context, size_t byteCount)
{
    return context->AllocateUnaligned(byteCount);
}

////////////////////////////////////////////////////////////////////////////////

TPIValue* LookupInRowset(
    TComparerFunction* comparerFunction,
    THasherFunction* hasherFunction,
    TComparerFunction* eqComparerFunction,
    TPIValue* key,
    TSharedRange<TRange<TPIValue>>* rowset,
    std::unique_ptr<TLookupRows>* lookupTable)
{
    auto comparer = PrepareFunction(comparerFunction);
    auto hasher = PrepareFunction(hasherFunction);
    auto eqComparer = PrepareFunction(eqComparerFunction);

    if (rowset->Size() < 32) {
        auto found = std::lower_bound(
            rowset->Begin(),
            rowset->End(),
            key,
            [&] (TRange<TPIValue> row, TPIValue* values) {
                return comparer(row.Begin(), values);
            });

        if (found != rowset->End() && !comparer(key, found->Begin())) {
            return const_cast<TPIValue*>(found->Begin());
        }

        return nullptr;
    }

    if (!*lookupTable) {
        *lookupTable = std::make_unique<TLookupRows>(rowset->Size(), hasher, eqComparer);
        (*lookupTable)->set_empty_key(nullptr);

        for (auto& row: *rowset) {
            (*lookupTable)->insert(row.Begin());
        }
    }

    auto found = (*lookupTable)->find(key);
    if (found != (*lookupTable)->end()) {
        return const_cast<TPIValue*>(*found);
    }

    return nullptr;
}

char IsRowInRowset(
    TComparerFunction* comparer,
    THasherFunction* hasher,
    TComparerFunction* eqComparer,
    TPIValue* values,
    TSharedRange<TRange<TPIValue>>* rows,
    std::unique_ptr<TLookupRows>* lookupRows)
{
    return LookupInRowset(comparer, hasher, eqComparer, values, rows, lookupRows) != nullptr;
}

char IsRowInRanges(
    ui32 valuesCount,
    TPIValue* values,
    TSharedRange<TPIRowRange>* ranges)
{
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
    std::unique_ptr<TLookupRows>* lookupRows)
{
    return LookupInRowset(comparer, hasher, eqComparer, values, rows, lookupRows);
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

re2::RE2* RegexCreate(TValue* regexp)
{
    auto piRegexp = BorrowFromNonPI(regexp);
    re2::RE2::Options options;
    options.set_log_errors(false);
    auto re2 = std::make_unique<re2::RE2>(re2::StringPiece(piRegexp.GetPIValue()->AsStringBuf().Data(), piRegexp.GetPIValue()->AsStringBuf().Size()), options);
    if (!re2->ok()) {
        THROW_ERROR_EXCEPTION(
            "Error parsing regular expression %Qv",
            piRegexp.GetPIValue()->AsStringBuf())
            << TError(re2->error().c_str());
    }
    return re2.release();
}

void RegexDestroy(re2::RE2* re2)
{
    delete re2;
}

ui8 RegexFullMatch(re2::RE2* re2, TValue* string)
{
    auto piString = BorrowFromNonPI(string);

    YT_VERIFY(piString.GetPIValue()->Type == EValueType::String);

    return re2::RE2::FullMatch(
        re2::StringPiece(piString.GetPIValue()->AsStringBuf().Data(), piString.GetPIValue()->AsStringBuf().Size()),
        *re2);
}

ui8 RegexPartialMatch(re2::RE2* re2, TValue* string)
{
    auto piString = BorrowFromNonPI(string);

    YT_VERIFY(piString.GetPIValue()->Type == EValueType::String);

    return re2::RE2::PartialMatch(
        re2::StringPiece(piString.GetPIValue()->AsStringBuf().Data(), piString.GetPIValue()->AsStringBuf().size()),
        *re2);
}

template <typename TStringType>
void CopyString(TExpressionContext* context, TPIValue* result, const TStringType& str)
{
    char* data = AllocateBytes(context, str.size());
    ::memcpy(data, str.data(), str.size());
    MakePositionIndependentStringValue(result, TStringBuf(data, str.size()));
}

template <typename TStringType>
void CopyAny(TExpressionContext* context, TPIValue* result, const TStringType& str)
{
    char* data = AllocateBytes(context, str.size());
    ::memcpy(data, str.data(), str.size());
    MakePositionIndependentAnyValue(result, TStringBuf(data, str.size()));
}

void RegexReplaceFirst(
    TExpressionContext* context,
    re2::RE2* re2,
    TValue* string,
    TValue* rewrite,
    TValue* result)
{
    auto piString = BorrowFromNonPI(string);
    auto piRewrite = BorrowFromNonPI(rewrite);
    auto piResult = BorrowFromNonPI(result);

    YT_VERIFY(piString.GetPIValue()->Type == EValueType::String);
    YT_VERIFY(piRewrite.GetPIValue()->Type == EValueType::String);

    std::string str(piString.GetPIValue()->AsStringBuf().Data(), piString.GetPIValue()->AsStringBuf().Size());
    re2::RE2::Replace(
        &str,
        *re2,
        re2::StringPiece(piRewrite.GetPIValue()->AsStringBuf().Data(), piRewrite.GetPIValue()->AsStringBuf().Size()));

    CopyString(context, piResult.GetPIValue(), str);
}

void RegexReplaceAll(
    TExpressionContext* context,
    re2::RE2* re2,
    TValue* string,
    TValue* rewrite,
    TValue* result)
{
    auto piString = BorrowFromNonPI(string);
    auto piRewrite = BorrowFromNonPI(rewrite);
    auto piResult = BorrowFromNonPI(result);

    YT_VERIFY(piString.GetPIValue()->Type == EValueType::String);
    YT_VERIFY(piRewrite.GetPIValue()->Type == EValueType::String);

    std::string str(piString.GetPIValue()->AsStringBuf().Data(), piString.GetPIValue()->AsStringBuf().Size());
    re2::RE2::GlobalReplace(
        &str,
        *re2,
        re2::StringPiece(piRewrite.GetPIValue()->AsStringBuf().Data(), piRewrite.GetPIValue()->AsStringBuf().Size()));

    CopyString(context, piResult.GetPIValue(), str);
}

void RegexExtract(
    TExpressionContext* context,
    re2::RE2* re2,
    TValue* string,
    TValue* rewrite,
    TValue* result)
{
    auto piString = BorrowFromNonPI(string);
    auto piRewrite = BorrowFromNonPI(rewrite);
    auto piResult = BorrowFromNonPI(result);

    YT_VERIFY(piString.GetPIValue()->Type == EValueType::String);
    YT_VERIFY(piRewrite.GetPIValue()->Type == EValueType::String);

    std::string str;
    re2::RE2::Extract(
        re2::StringPiece(piString.GetPIValue()->AsStringBuf().Data(), piString.GetPIValue()->AsStringBuf().Size()),
        *re2,
        re2::StringPiece(piRewrite.GetPIValue()->AsStringBuf().Data(), piRewrite.GetPIValue()->AsStringBuf().Size()),
        &str);

    CopyString(context, piResult.GetPIValue(), str);
}

void RegexEscape(
    TExpressionContext* context,
    TValue* string,
    TValue* result)
{
    auto piString = BorrowFromNonPI(string);
    auto piResult = BorrowFromNonPI(result);

    auto str = re2::RE2::QuoteMeta(
        re2::StringPiece(piString.GetPIValue()->AsStringBuf().Data(), piString.GetPIValue()->AsStringBuf().Size()));

    CopyString(context, piResult.GetPIValue(), str);
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
        TValue* result, \
        TValue* anyValue, \
        TValue* ypath) \
    { \
        auto piResult = BorrowFromNonPI(result); \
        auto piAnyValue = BorrowFromNonPI(anyValue); \
        auto piYpath = BorrowFromNonPI(ypath); \
        auto value = NYTree::TryGet ## TYPE( \
            piAnyValue.GetPIValue()->AsStringBuf(), \
            TString(piYpath.GetPIValue()->AsStringBuf())); \
        if (value) { \
            STATEMENT_OK \
        } else { \
            STATEMENT_FAIL \
        } \
    }

#define DEFINE_YPATH_GET_IMPL(TYPE, STATEMENT_OK) \
    DEFINE_YPATH_GET_IMPL2(Try, TYPE, STATEMENT_OK, \
        MakePositionIndependentNullValue(piResult.GetPIValue());) \
    DEFINE_YPATH_GET_IMPL2(, TYPE, STATEMENT_OK, \
        THROW_ERROR_EXCEPTION("Value of type %Qlv is not found at YPath %v", \
            EValueType::TYPE, \
            piYpath.GetPIValue()->AsStringBuf());)

#define DEFINE_YPATH_GET(TYPE) \
    DEFINE_YPATH_GET_IMPL(TYPE, \
        MakePositionIndependent ## TYPE ## Value(piResult.GetPIValue(), *value);)

#define DEFINE_YPATH_GET_STRING \
    DEFINE_YPATH_GET_IMPL(String, \
        CopyString(context, piResult.GetPIValue(), *value);)

#define DEFINE_YPATH_GET_ANY \
    DEFINE_YPATH_GET_IMPL(Any, \
        CopyAny(context, piResult.GetPIValue(), *value);)

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
        if (anyValue->Type == EValueType::Null) { \
            MakePositionIndependentNullValue(result); \
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
        if (anyValue->Type == EValueType::Null) { \
            MakePositionIndependentNullValue(result); \
            return; \
        } \
        NYson::TToken token; \
        auto anyString = anyValue->AsStringBuf(); \
        NYson::ParseToken(anyString, &token); \
        if (token.GetType() == NYson::ETokenType::Int64) { \
            MakePositionIndependent ## TYPE ## Value(result, token.GetInt64Value()); \
        } else if (token.GetType() == NYson::ETokenType::Uint64) { \
            MakePositionIndependent ## TYPE ## Value(result, token.GetUint64Value()); \
        } else if (token.GetType() == NYson::ETokenType::Double) { \
            MakePositionIndependent ## TYPE ## Value(result, token.GetDoubleValue()); \
        } else { \
            THROW_ERROR_EXCEPTION("Cannot convert value %Qv of type \"any\" to %Qlv", \
                anyString, \
                EValueType::TYPE); \
        } \
    }

DEFINE_CONVERT_ANY_NUMERIC_IMPL(Int64)
DEFINE_CONVERT_ANY_NUMERIC_IMPL(Uint64)
DEFINE_CONVERT_ANY_NUMERIC_IMPL(Double)
DEFINE_CONVERT_ANY(Boolean, MakePositionIndependentBooleanValue(result, token.GetBooleanValue());)
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

void ToAny(TExpressionContext* context, TValue* result, TValue* value)
{
    auto piResult = BorrowFromNonPI(result);
    auto piValue = BorrowFromNonPI(value);
    // TODO(babenko): for some reason, flags are garbage here.
    value->Flags = {};
    ToAny(piResult.GetPIValue(), piValue.GetPIValue(), context);
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

TFingerprint GetFarmFingerprint(const TValue* begin, const TValue* end)
{
    auto asRange = TRange<TValue>(begin, static_cast<size_t>(end - begin));
    auto asPIRange = BorrowFromNonPI(asRange);
    return GetFarmFingerprint(asPIRange.Begin(), asPIRange.Begin() + asPIRange.Size());
}

////////////////////////////////////////////////////////////////////////////////

extern "C" void MakeMap(
    TExpressionContext* context,
    TValue* result,
    TValue* args,
    int argCount)
{
    auto piResult = BorrowFromNonPI(result);

    auto argumentRange = TRange<TValue>(args, static_cast<size_t>(argCount));
    auto piArgs = BorrowFromNonPI(argumentRange);
    if (argCount % 2 != 0) {
        THROW_ERROR_EXCEPTION("\"make_map\" takes a even number of arguments");
    }

    TString resultYson;
    TStringOutput output(resultYson);
    NYson::TYsonWriter writer(&output);

    writer.OnBeginMap();
    for (int index = 0; index < argCount / 2; ++index) {
        const auto& nameArg = piArgs[index * 2];
        const auto& valueArg = piArgs[index * 2 + 1];

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

    MakePositionIndependentAnyValue(piResult.GetPIValue(), resultYson);
    CapturePIValue(context, piResult.GetPIValue());
}

extern "C" void MakeList(
    TExpressionContext* context,
    TValue* result,
    TValue* args,
    int argCount)
{
    auto piResult = BorrowFromNonPI(result);

    auto argumentRange = TRange<TValue>(args, static_cast<size_t>(argCount));
    auto piArgs = BorrowFromNonPI(argumentRange);

    TString resultYson;
    TStringOutput output(resultYson);
    NYson::TYsonWriter writer(&output);

    writer.OnBeginList();
    for (int index = 0; index < argCount; ++index) {
        const auto& valueArg = piArgs[index];

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
                writer.OnStringScalar(valueArg.AsStringBuf());
                break;
            case EValueType::Any:
                writer.OnRaw(valueArg.AsStringBuf());
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

    MakePositionIndependentAnyValue(piResult.GetPIValue(), resultYson);
    CapturePIValue(context, piResult.GetPIValue());
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
    auto piResult = BorrowFromNonPI(result);
    auto piYsonList = BorrowFromNonPI(ysonList);
    auto piWhat = BorrowFromNonPI(what);

    const auto node = NYTree::ConvertToNode(
        FromPositionIndependentValue<NYson::TYsonStringBuf>(*piYsonList.GetPIValue()));

    bool found;
    switch (piWhat.GetPIValue()->Type) {
        case EValueType::String:
            found = ListContainsImpl<ENodeType::String, TString>(node, piWhat.GetPIValue()->AsStringBuf());
            break;
        case EValueType::Int64:
            found = ListContainsImpl<ENodeType::Int64, i64>(node, piWhat.GetPIValue()->Data.Int64);
            break;
        case EValueType::Uint64:
            found = ListContainsImpl<ENodeType::Uint64, ui64>(node, piWhat.GetPIValue()->Data.Uint64);
            break;
        case EValueType::Boolean:
            found = ListContainsImpl<ENodeType::Boolean, bool>(node, piWhat.GetPIValue()->Data.Boolean);
            break;
        case EValueType::Double:
            found = ListContainsImpl<ENodeType::Double, double>(node, piWhat.GetPIValue()->Data.Double);
            break;
        default:
            THROW_ERROR_EXCEPTION("ListContains is not implemented for %Qlv values",
                piWhat.GetPIValue()->Type);
    }

    MakePositionIndependentBooleanValue(piResult.GetPIValue(), found);
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
    TValue* result,
    TValue* value
)
{
    if (value->Type == EValueType::Null) {
        result->Type = EValueType::Null;
        return;
    }

    auto piResult = BorrowFromNonPI(result);
    auto piValue = BorrowFromNonPI(value);

    TString resultYson;
    TStringOutput output(resultYson);
    TYsonWriter writer(&output, EYsonFormat::Text);

    switch (piValue.GetPIValue()->Type) {
        case EValueType::Int64:
            writer.OnInt64Scalar(piValue.GetPIValue()->Data.Int64);
            break;
        case EValueType::Uint64:
            writer.OnUint64Scalar(piValue.GetPIValue()->Data.Uint64);
            break;
        case EValueType::Double:
            writer.OnDoubleScalar(piValue.GetPIValue()->Data.Double);
            break;
        default:
            YT_ABORT();
    }

    MakePositionIndependentStringValue(piResult.GetPIValue(), resultYson);
    CapturePIValue(context, piResult.GetPIValue());
}

////////////////////////////////////////////////////////////////////////////////

#define DEFINE_CONVERT_STRING(TYPE) \
    extern "C" void StringTo ## TYPE(TExpressionContext* /*context*/, TValue* result, TValue* value) \
    { \
        auto piResult = BorrowFromNonPI(result); \
        auto piValue = BorrowFromNonPI(value); \
        if (piValue.GetPIValue()->Type == EValueType::Null) { \
            MakePositionIndependentNullValue(piResult.GetPIValue()); \
            return; \
        } \
        NYson::TToken token; \
        auto valueString = piValue.GetPIValue()->AsStringBuf(); \
        NYson::ParseToken(valueString, &token); \
        if (token.GetType() == NYson::ETokenType::Int64) { \
            MakePositionIndependent ## TYPE ## Value(piResult.GetPIValue(), token.GetInt64Value()); \
        } else if (token.GetType() == NYson::ETokenType::Uint64) { \
            MakePositionIndependent ## TYPE ## Value(piResult.GetPIValue(), token.GetUint64Value()); \
        } else if (token.GetType() == NYson::ETokenType::Double) { \
            MakePositionIndependent ## TYPE ## Value(piResult.GetPIValue(), token.GetDoubleValue()); \
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
    auto piResult = BorrowFromNonPI(result);

    auto* hll = AllocateBytes(context, sizeof(THLL));
    new (hll) THLL();
    MakePositionIndependentStringValue(piResult.GetPIValue(), TStringBuf(hll, sizeof(THLL)));
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
    TCompactVector<TChunkReplica, TypicalReplicaCount> result;
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

void ReplicaSetMerge(
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

void ReplicaSetFinalize(
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
    auto subjectClosure = ConvertTo<THashSet<TString>>(
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
        newRegex = std::make_unique<re2::RE2>(re2::StringPiece(asRe2Pattern.Data(), asRe2Pattern.Size()), options);
        if (!newRegex->ok()) {
            THROW_ERROR_EXCEPTION("Error parsing regular expression %Qv",
                asRe2Pattern)
                << TError(matcher->error().c_str());
        }

        matcher = newRegex.get();
    }

    bool matched = re2::RE2::FullMatch(re2::StringPiece(text->AsStringBuf().Data(), text->AsStringBuf().Size()), *matcher);

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

    MakePositionIndependentNullValue(result);

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
                MakePositionIndependentInt64Value(result, *parsed);
            }
            break;

        case EValueType::Uint64:
            if (auto parsed = TryParseValue<ui64>(&cursor)) {
                MakePositionIndependentUint64Value(result, *parsed);
            }
            break;

        case EValueType::Double:
            if (auto parsed = TryParseValue<double>(&cursor)) {
                MakePositionIndependentDoubleValue(result, *parsed);
            }
            break;

        case EValueType::Boolean:
            if (auto parsed = TryParseValue<bool>(&cursor)) {
                MakePositionIndependentBooleanValue(result, *parsed);
            }
            break;

        case EValueType::String:
            if (auto parsed = TryParseValue<TString>(&cursor)) {
                CopyString(context, result, *parsed);
            }
            break;

        case EValueType::Any:
        case EValueType::Composite: {
            auto parsed = ParseAnyValue(&cursor);
            CopyAny(context, result, parsed);
            result->Type = resultType;
            break;
        }

        default:
            YT_ABORT();
    }
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
    REGISTER_ROUTINE(ArrayJoinOpHelper);
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
    REGISTER_ROUTINE(MakeMap);
    REGISTER_ROUTINE(MakeList);
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
    REGISTER_ROUTINE(ReplicaSetMerge);
    REGISTER_ROUTINE(ReplicaSetFinalize);
    REGISTER_ROUTINE(HasPermissions);
    REGISTER_ROUTINE(YsonLength);
    REGISTER_ROUTINE(LikeOpHelper);
    REGISTER_ROUTINE(CompositeMemberAccessorHelper);
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
