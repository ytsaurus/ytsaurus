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

#include <yt/core/ytree/ypath_resolver.h>

#include <yt/core/yson/parser.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/misc/farm_hash.h>

#include <yt/core/profiling/scoped_timer.h>

#include <contrib/libs/re2/re2/re2.h>

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

void WriteRow(TExecutionContext* context, TWriteOpClosure* closure, TRow row)
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
    batch.push_back(rowBuffer->Capture(row));

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
    void (*consumeRows)(void** closure, TRowBuffer*, TRow* rows, i64 size))
{
    auto& reader = context->Reader;

    std::vector<TRow> rows;
    rows.reserve(context->IsOrdered && context->Limit < RowsetProcessingSize
        ? context->Limit
        : RowsetProcessingSize);

    auto* statistics = context->Statistics;

    auto rowBuffer = New<TRowBuffer>(TIntermadiateBufferTag());

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

        consumeRows(consumeRowsClosure, rowBuffer.Get(), rows.data(), rows.size());
        rows.clear();
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
    TMutableRow* keyPtr,
    TRow row)
{
    CHECK_STACK();

    TMutableRow key = *keyPtr;

    i64 chainIndex = closure->ChainedRows.size();

    if (chainIndex >= context->JoinRowLimit) {
        throw TInterruptedIncompleteException();
    }

    closure->ChainedRows.emplace_back(TChainedRow{closure->Buffer->Capture(row), key, -1});

    if (!closure->LastKey || !closure->PrefixEqComparer(key, closure->LastKey)) {
        if (closure->LastKey) {
            Y_ASSERT(CompareRows(closure->LastKey, key, closure->CommonKeyPrefixDebug) <= 0);
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
        *keyPtr = closure->Buffer->AllocateUnversioned(closure->KeySize);
    } else {
        auto& startIndex = inserted.first->second.first;
        closure->ChainedRows.back().Key = inserted.first->first;
        closure->ChainedRows.back().NextRowIndex = startIndex;
        startIndex = chainIndex;
    }

    if (closure->ChainedRows.size() >= closure->BatchSize) {
        closure->ProcessJoinBatch();
        *keyPtr = closure->Buffer->AllocateUnversioned(closure->KeySize);
    }
}

class TJoinBatchState
{
public:
    TJoinBatchState(
        void** consumeRowsClosure,
        void (*consumeRows)(void** closure, TRowBuffer*, TRow* rows, i64 size),
        const std::vector<size_t>& selfColumns,
        const std::vector<size_t>& foreignColumns)
        : ConsumeRowsClosure(consumeRowsClosure)
        , ConsumeRows(consumeRows)
        , SelfColumns(selfColumns)
        , ForeignColumns(foreignColumns)
        , IntermediateBuffer(New<TRowBuffer>(TIntermadiateBufferTag()))
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

    void JoinRow(TRow row, TRow foreignRow)
    {
        auto joinedRow = IntermediateBuffer->AllocateUnversioned(SelfColumns.size() + ForeignColumns.size());

        for (size_t column = 0; column < SelfColumns.size(); ++column) {
            joinedRow[column] = row[SelfColumns[column]];
        }

        for (size_t column = 0; column < ForeignColumns.size(); ++column) {
            joinedRow[column + SelfColumns.size()] = foreignRow[ForeignColumns[column]];
        }

        JoinedRows.push_back(joinedRow);

        if (JoinedRows.size() >= RowsetProcessingSize) {
            ConsumeJoinedRows();
        }
    }

    void JoinRowNull(TRow row)
    {
        auto joinedRow = IntermediateBuffer->AllocateUnversioned(SelfColumns.size() + ForeignColumns.size());

        for (size_t column = 0; column < SelfColumns.size(); ++column) {
            joinedRow[column] = row[SelfColumns[column]];
        }

        for (size_t column = 0; column < ForeignColumns.size(); ++column) {
            joinedRow[column + SelfColumns.size()] = MakeUnversionedSentinelValue(EValueType::Null);
        }

        JoinedRows.push_back(joinedRow);

        if (JoinedRows.size() >= RowsetProcessingSize) {
            ConsumeJoinedRows();
        }
    }

    void JoinRows(const std::vector<TChainedRow>& chainedRows, int startIndex, TRow foreignRow)
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
        const std::vector<std::pair<TRow, int>>& keysToRows,
        const std::vector<TChainedRow>& chainedRows,
        TComparerFunction* fullEqComparer,
        TComparerFunction* fullLessComparer,
        const ISchemafulReaderPtr& reader,
        bool isLeft)
    {
        std::vector<TRow> foreignRows;
        foreignRows.reserve(RowsetProcessingSize);

        // Sort-merge join
        auto currentKey = keysToRows.begin();
        auto lastJoined = keysToRows.end();
        while (currentKey != keysToRows.end()) {
            bool hasMoreData = reader->Read(&foreignRows);
            bool shouldWait = foreignRows.empty();

            auto foreignIt = foreignRows.begin();
            while (foreignIt != foreignRows.end() && currentKey != keysToRows.end()) {
                int startIndex = currentKey->second;
                if (fullEqComparer(currentKey->first, *foreignIt)) {
                    JoinRows(chainedRows, startIndex, *foreignIt);
                    ++foreignIt;
                    lastJoined = currentKey;
                } else if (fullLessComparer(currentKey->first, *foreignIt)) {
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
                auto it = joinLookup->find(foreignRow);

                if (it == joinLookup->end()) {
                    continue;
                }

                int startIndex = it->second.first;
                bool& isJoined = it->second.second;
                JoinRows(chainedRows, startIndex, foreignRow);
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
    void** ConsumeRowsClosure;
    void (*ConsumeRows)(void** closure, TRowBuffer*, TRow* rows, i64 size);

    std::vector<size_t> SelfColumns;
    std::vector<size_t> ForeignColumns;

    TRowBufferPtr IntermediateBuffer;
    std::vector<TRow> JoinedRows;

};

void JoinOpHelper(
    TExecutionContext* context,
    TJoinParameters* parameters,
    THasherFunction* lookupHasher,
    TComparerFunction* lookupEqComparer,
    TComparerFunction* sortLessComparer,
    TComparerFunction* prefixEqComparer,
    TComparerFunction* fullEqComparer,
    TComparerFunction* fullLessComparer,
    int keySize,
    void** collectRowsClosure,
    void (*collectRows)(
        void** closure,
        TJoinClosure* joinClosure,
        TRowBuffer* buffer),
    void** consumeRowsClosure,
    void (*consumeRows)(void** closure, TRowBuffer*, TRow* rows, i64 size))
{
    TJoinClosure closure(lookupHasher, lookupEqComparer, prefixEqComparer, keySize, parameters->BatchSize);
    closure.CommonKeyPrefixDebug = parameters->CommonKeyPrefixDebug;

    closure.ProcessSegment = [&] () {
        auto offset = closure.KeysToRows.size();
        for (const auto& item : closure.Lookup) {
            closure.KeysToRows.emplace_back(item.first, item.second.first);
        }

        std::sort(closure.KeysToRows.begin() + offset, closure.KeysToRows.end(), [&] (
            const std::pair<TRow, int>& lhs,
            const std::pair<TRow, int>& rhs)
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
            keys.push_back(item.first);
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
            auto pipe = New<NTableClient::TSchemafulPipe>();

            TQueryPtr foreignQuery;
            TDataRanges dataSource;

            std::tie(foreignQuery, dataSource) = parameters->GetForeignQuery(
                std::move(keys),
                closure.Buffer);

            context->ExecuteCallback(foreignQuery, dataSource, pipe->GetWriter())
                .Subscribe(BIND([pipe] (const TErrorOr<TQueryStatistics>& error) {
                    if (!error.IsOK()) {
                        pipe->Fail(error);
                    }
                }));

            LOG_DEBUG("Joining started");

            std::vector<TRow> foreignRows;
            foreignRows.reserve(RowsetProcessingSize);

            auto reader = pipe->GetReader();

            if (parameters->IsSortMergeJoin) {
                // Sort-merge join
                batchState.SortMergeJoin(
                    context,
                    keysToRows,
                    chainedRows,
                    fullEqComparer,
                    fullLessComparer,
                    reader,
                    isLeft);
            } else {
                batchState.HashJoin(context, &joinLookup, chainedRows, reader, isLeft);
            }
        } else {
            NApi::IUnversionedRowsetPtr rowset;

            {
                TQueryPtr foreignQuery;
                TDataRanges dataSource;

                std::tie(foreignQuery, dataSource) = parameters->GetForeignQuery(
                    std::move(keys),
                    closure.Buffer);

                ISchemafulWriterPtr writer;
                TFuture<NApi::IUnversionedRowsetPtr> rowsetFuture;
                // Any schema, it is not used.
                std::tie(writer, rowsetFuture) = NApi::CreateSchemafulRowsetWriter(TTableSchema());
                NProfiling::TAggregatingTimingGuard timingGuard(&context->Statistics->AsyncTime);

                WaitFor(context->ExecuteCallback(foreignQuery, dataSource, writer))
                    .ThrowOnError();

                YCHECK(rowsetFuture.IsSet());
                rowset = rowsetFuture.Get()
                    .ValueOrThrow();
            }

            auto foreignRows = rowset->GetRows();

            LOG_DEBUG("Got %v foreign rows", foreignRows.Size());

            TJoinLookupRows foreignLookup(
                InitialGroupOpHashtableCapacity,
                lookupHasher,
                lookupEqComparer);

            for (auto row : foreignRows) {
                foreignLookup.insert(row);
            }

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

const TRow* InsertGroupRow(
    TExecutionContext* context,
    TGroupByClosure* closure,
    TMutableRow row)
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

    return &*inserted.first;
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
    void (*consumeRows)(void** closure, TRowBuffer*, TRow* rows, i64 size))
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

    auto intermediateBuffer = New<TRowBuffer>(TIntermadiateBufferTag());

    for (size_t index = 0; index < closure.GroupedRows.size(); index += RowsetProcessingSize) {
        auto size = std::min(RowsetProcessingSize, closure.GroupedRows.size() - index);
        consumeRows(consumeRowsClosure, intermediateBuffer.Get(), closure.GroupedRows.data() + index, size);
        intermediateBuffer->Clear();
    }
}

void AllocatePermanentRow(TExecutionContext* context, TRowBuffer* buffer, int valueCount, TMutableRow* row)
{
    CHECK_STACK();

    *row = buffer->AllocateUnversioned(valueCount);
}

void AddRow(TTopCollector* topCollector, TRow row)
{
    topCollector->AddRow(row);
}

void OrderOpHelper(
    TExecutionContext* context,
    TComparerFunction* comparer,
    void** collectRowsClosure,
    void (*collectRows)(void** closure, TTopCollector* topCollector),
    void** consumeRowsClosure,
    void (*consumeRows)(void** closure, TRowBuffer* ,TRow* rows, i64 size),
    int rowSize)
{
    auto limit = context->Limit;

    TTopCollector topCollector(limit, comparer);
    collectRows(collectRowsClosure, &topCollector);
    auto rows = topCollector.GetRows(rowSize);

    auto rowBuffer = New<TRowBuffer>(TIntermadiateBufferTag());

    for (size_t index = 0; index < rows.size(); index += RowsetProcessingSize) {
        auto size = std::min(RowsetProcessingSize, rows.size() - index);
        consumeRows(consumeRowsClosure, rowBuffer.Get(), rows.data() + index, size);
        rowBuffer->Clear();
    }
}

void WriteOpHelper(
    TExecutionContext* context,
    void** collectRowsClosure,
    void (*collectRows)(void** closure, TWriteOpClosure* writeOpClosure))
{
    TWriteOpClosure closure;

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

char IsRowInArray(
    TComparerFunction* comparer,
    TRow row,
    TSharedRange<TRow>* rows)
{
    return std::binary_search(rows->Begin(), rows->End(), row, comparer);
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
    const auto hash64 = [&, MurmurHashConstant] (ui64 data, ui64 value) {
        value ^= FarmFingerprint(data);
        value *= MurmurHashConstant;
        return value;
    };

    // Hash string. Like Murmurhash.
    const auto hash = [&, MurmurHashConstant] (const void* voidData, int length, ui64 seed) {
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
    THROW_ERROR_EXCEPTION("Error while executing UDF: %s", error);
}

void ThrowQueryException(const char* error)
{
    THROW_ERROR_EXCEPTION("Error while executing query: %s", error);
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

DEFINE_YPATH_GET(Int64)
DEFINE_YPATH_GET(Uint64)
DEFINE_YPATH_GET(Double)
DEFINE_YPATH_GET(Boolean)
DEFINE_YPATH_GET_STRING

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
    REGISTER_ROUTINE(GroupOpHelper);
    REGISTER_ROUTINE(StringHash);
    REGISTER_ROUTINE(AllocatePermanentRow);
    REGISTER_ROUTINE(AllocateBytes);
    REGISTER_ROUTINE(IsRowInArray);
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
    REGISTER_YPATH_GET_ROUTINE(Int64);
    REGISTER_YPATH_GET_ROUTINE(Uint64);
    REGISTER_YPATH_GET_ROUTINE(Double);
    REGISTER_YPATH_GET_ROUTINE(Boolean);
    REGISTER_YPATH_GET_ROUTINE(String);
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
