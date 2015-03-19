#include "stdafx.h"
#include "cg_routines.h"
#include "cg_types.h"

#include "helpers.h"
#include "callbacks.h"
#include "query_statistics.h"
#include "evaluation_helpers.h"

#include <ytlib/new_table_client/unversioned_row.h>
#include <ytlib/new_table_client/schemaful_reader.h>
#include <ytlib/new_table_client/schemaful_writer.h>
#include <ytlib/new_table_client/unordered_schemaful_reader.h>
#include <ytlib/new_table_client/row_buffer.h>

#include <ytlib/chunk_client/chunk_spec.h>

#include <core/concurrency/scheduler.h>

#include <core/profiling/scoped_timer.h>

#include <core/misc/farm_hash.h>

#include <mutex>

namespace NYT {
namespace NQueryClient {
namespace NRoutines {

using namespace NConcurrency;

static const auto& Logger = QueryClientLogger;

////////////////////////////////////////////////////////////////////////////////

struct TRowComparer
{
    typedef char (*TComparerFunc)(TRow, TRow);
    TComparerFunc Ptr_;
    TRowComparer(TComparerFunc ptr)
        : Ptr_(ptr)
    { }

    bool operator () (const TRow& key, const TOwningRow& current) const
    {
        return Ptr_(key, current.Get());
    }

    bool operator () (const TOwningRow& current, const TRow& key) const
    {
        return Ptr_(current.Get(), key);
    }
};

////////////////////////////////////////////////////////////////////////////////

void CaptureValue(TValue* value, TChunkedMemoryPool* pool)
{
    if (IsStringLikeType(EValueType(value->Type))) {
        char* dst = pool->AllocateUnaligned(value->Length);
        memcpy(dst, value->Data.String, value->Length);
        value->Data.String = dst;
    }
}

////////////////////////////////////////////////////////////////////////////////

#ifndef NDEBUG
#define CHECK_STACK() \
    { \
        int dummy; \
        size_t currentStackSize = executionContext->StackSizeGuardHelper - reinterpret_cast<intptr_t>(&dummy); \
        YCHECK(currentStackSize < 10000); \
    }
#else
#define CHECK_STACK() (void) 0;
#endif

////////////////////////////////////////////////////////////////////////////////

void WriteRow(TRow row, TExecutionContext* executionContext)
{
    CHECK_STACK();

    if (executionContext->StopFlag = CountRow(&executionContext->Limit)) {
        return;
    }

    if (executionContext->StopFlag = CountRow(&executionContext->OutputRowLimit)) {
        executionContext->Statistics->IncompleteOutput = true;
        return;
    }
    
    ++executionContext->Statistics->RowsWritten;

    auto* batch = executionContext->Batch;
    auto* writer = executionContext->Writer;
    auto* rowBuffer = executionContext->OutputBuffer;

    YASSERT(batch->size() < batch->capacity());

    batch->push_back(rowBuffer->Capture(row));

    if (batch->size() == batch->capacity()) {
        bool shouldNotWait;
        {
            NProfiling::TAggregatingTimingGuard timingGuard(&executionContext->Statistics->WriteTime);
            shouldNotWait = writer->Write(*batch);
        }

        if (!shouldNotWait) {
            NProfiling::TAggregatingTimingGuard timingGuard(&executionContext->Statistics->AsyncTime);
            auto error = WaitFor(writer->GetReadyEvent());
            THROW_ERROR_EXCEPTION_IF_FAILED(error);
        }
        batch->clear();
        rowBuffer->Clear();
    }
}

void ScanOpHelper(
    TExecutionContext* executionContext,
    int dataSplitsIndex,
    void** consumeRowsClosure,
    void (*consumeRows)(void** closure, TRow* rows, int size, char* stopFlag))
{
    auto* reader = executionContext->Reader;

    {
        LOG_DEBUG("Waiting for ready event in generated code while opening");
        NProfiling::TAggregatingTimingGuard timingGuard(&executionContext->Statistics->AsyncTime);
        auto error = WaitFor(reader->Open(executionContext->Schema));
        LOG_DEBUG(error, "Ready event in generated code while opening");
        error.ThrowOnError();
    }

    std::vector<TRow> rows;
    rows.reserve(MaxRowsPerRead);

    executionContext->StopFlag = false;

    while (true) {
        executionContext->IntermediateBuffer->Clear();

        bool hasMoreData;
        {
            NProfiling::TAggregatingTimingGuard timingGuard(&executionContext->Statistics->ReadTime);
            hasMoreData = reader->Read(&rows);
        }

        bool shouldWait = rows.empty();

        if (executionContext->InputRowLimit < rows.size()) {
            rows.resize(executionContext->InputRowLimit);
            executionContext->Statistics->IncompleteInput = true;
            hasMoreData = false;
        }
        executionContext->InputRowLimit -= rows.size();
        executionContext->Statistics->RowsRead += rows.size();

        consumeRows(consumeRowsClosure, rows.data(), rows.size(), &executionContext->StopFlag);
        rows.clear();

        if (!hasMoreData || executionContext->StopFlag) {
            break;
        }

        if (shouldWait) {
            LOG_DEBUG("Waiting for ready event in generated code while reading");
            NProfiling::TAggregatingTimingGuard timingGuard(&executionContext->Statistics->AsyncTime);
            auto error = WaitFor(reader->GetReadyEvent());
            LOG_DEBUG(error, "Ready event in generated code while reading");
            THROW_ERROR_EXCEPTION_IF_FAILED(error);
        }
    }
}

void InsertJoinRow(
    TExecutionContext* executionContext,
    TLookupRows* lookupRows,
    std::vector<TRow>* rows,
    TRow* rowPtr,
    int valueCount)
{
    CHECK_STACK();

    TRow row = *rowPtr;
    auto inserted = lookupRows->insert(row);

    if (inserted.second) {
        rows->push_back(row);
        for (int index = 0; index < valueCount; ++index) {
            CaptureValue(&row[index], executionContext->PermanentBuffer->GetUnalignedPool());
        }
        *rowPtr = TRow::Allocate(executionContext->PermanentBuffer->GetAlignedPool(), valueCount);
    }
}

void SaveJoinRow(
    TExecutionContext* executionContext,
    std::vector<TRow>* rows,
    TRow row)
{
    CHECK_STACK();

    rows->push_back(executionContext->PermanentBuffer->Capture(row));
}

void JoinOpHelper(
    TExecutionContext* executionContext,
    ui64 (*groupHasher)(TRow),
    char (*groupComparer)(TRow, TRow),
    void** collectRowsClosure,
    void (*collectRows)(void** closure, std::vector<TRow>* rows, TLookupRows* lookupRows, std::vector<TRow>* allRows),
    void** consumeRowsClosure,
    void (*consumeRows)(void** closure, std::vector<TRow>* rows, char* stopFlag))
{
    std::vector<TRow> keys;
    
    TLookupRows keysLookup(
        InitialGroupOpHashtableCapacity,
        groupHasher,
        groupComparer);

    std::vector<TRow> allRows;

    keysLookup.set_empty_key(TRow());

    // Collect join ids.
    collectRows(collectRowsClosure, &keys, &keysLookup, &allRows);

    std::vector<TRow> joinedRows;
    executionContext->EvaluateJoin(executionContext, groupHasher, groupComparer, keys, allRows, &joinedRows);

    // Consume joined rows.
    executionContext->StopFlag = false;
    consumeRows(consumeRowsClosure, &joinedRows, &executionContext->StopFlag);
}

void GroupOpHelper(
    TExecutionContext* executionContext,
    ui64 (*groupHasher)(TRow),
    char (*groupComparer)(TRow, TRow),
    void** collectRowsClosure,
    void (*collectRows)(void** closure, std::vector<TRow>* groupedRows, TLookupRows* lookupRows),
    void** consumeRowsClosure,
    void (*consumeRows)(void** closure, std::vector<TRow>* groupedRows, char* stopFlag))
{
    std::vector<TRow> groupedRows;
    TLookupRows lookupRows(
        InitialGroupOpHashtableCapacity,
        groupHasher,
        groupComparer);

    lookupRows.set_empty_key(TRow());

    collectRows(collectRowsClosure, &groupedRows, &lookupRows);

    executionContext->StopFlag = false;
    consumeRows(consumeRowsClosure, &groupedRows, &executionContext->StopFlag);
}

const TRow* FindRow(TExecutionContext* executionContext, TLookupRows* rows, TRow row)
{
    CHECK_STACK();

    auto it = rows->find(row);
    return it != rows->end()? &*it : nullptr;
}

void AllocatePermanentRow(TExecutionContext* executionContext, int valueCount, TRow* row)
{
    CHECK_STACK();

    *row = TRow::Allocate(executionContext->PermanentBuffer->GetAlignedPool(), valueCount);
}

const TRow* InsertGroupRow(
    TExecutionContext* executionContext,
    TLookupRows* lookupRows,
    std::vector<TRow>* groupedRows,
    TRow* rowPtr,
    int valueCount)
{
    CHECK_STACK();

    TRow row = *rowPtr;
    auto inserted = lookupRows->insert(row);

    if (inserted.second) {
        if (executionContext->StopFlag = CountRow(&executionContext->GroupRowLimit)) {
            executionContext->Statistics->IncompleteOutput = true;
            return nullptr;
        }

        groupedRows->push_back(row);
        for (int index = 0; index < valueCount; ++index) {
            CaptureValue(&row[index], executionContext->PermanentBuffer->GetUnalignedPool());
        }
        *rowPtr = TRow::Allocate(executionContext->PermanentBuffer->GetAlignedPool(), valueCount);
        return nullptr;
    } else {
        return &*inserted.first;
    }
}

void AllocateRow(TExecutionContext* executionContext, int valueCount, TRow* row)
{
    CHECK_STACK();

    *row = TRow::Allocate(executionContext->IntermediateBuffer->GetAlignedPool(), valueCount);
}

TRow* GetRowsData(std::vector<TRow>* groupedRows)
{
    return groupedRows->data();
}

int GetRowsSize(std::vector<TRow>* groupedRows)
{
    return groupedRows->size();
}

void OrderOpHelper(
    TExecutionContext* executionContext,
    char (*comparer)(TRow, TRow),
    void** collectRowsClosure,
    void (*collectRows)(void** closure, TTopN* topN),
    void** consumeRowsClosure,
    void (*consumeRows)(void** closure, std::vector<TRow>* rows, char* stopFlag))
{
    size_t limit = executionContext->Limit;

    TTopN topN(limit, comparer);

    collectRows(collectRowsClosure, &topN);

    std::vector<TRow> rows;

    std::transform(topN.Rows.begin(), topN.Rows.end(), std::back_inserter(rows), [] (const std::pair<TRow, size_t>& value) {
        return value.first;
    });

    std::sort(rows.begin(), rows.end(), comparer);

    // Consume joined rows.
    executionContext->StopFlag = false;

    consumeRows(consumeRowsClosure, &rows, &executionContext->StopFlag);
}

////////////////////////////////////////////////////////////////////////////////

char IsPrefix(
    const char* lhsData,
    ui32 lhsLength,
    const char* rhsData,
    ui32 rhsLength)
{
    return lhsLength <= rhsLength &&
        std::mismatch(lhsData, lhsData + lhsLength, rhsData).first == lhsData + lhsLength;
}

char IsSubstr(
    const char* patternData,
    ui32 patternLength,
    const char* stringData,
    ui32 stringLength)
{
    return std::search(
        stringData,
        stringData + stringLength,
        patternData,
        patternData + patternLength) != stringData + stringLength;
}

char* ToLower(
    TExecutionContext* executionContext,
    const char* data,
    ui32 length)
{
    char* result = executionContext->IntermediateBuffer->GetUnalignedPool()->AllocateUnaligned(length);

    for (ui32 index = 0; index < length; ++index) {
        result[index] = tolower(data[index]);
    }

    return result;
}

char IsRowInArray(
    TExecutionContext* executionContext,
    char (*comparer)(TRow, TRow),
    TRow row,
    int index)
{
    // TODO(lukyan): check null
    const auto& rows = (*executionContext->LiteralRows)[index];
    return std::binary_search(rows.begin(), rows.end(), row, TRowComparer(comparer));
}

size_t StringHash(
    const char* data,
    ui32 length)
{
    return FarmHash(data, length);
}

// FarmHash and MurmurHash hybrid to hash TRow.
ui64 SimpleHash(TRow row)
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

    ui64 result = row.GetCount();

    for (int index = 0; index < row.GetCount(); ++index) {
        switch(row[index].Type) {
            case EValueType::Int64:
                result = hash64(row[index].Data.Int64, result);
                break;
            case EValueType::Uint64:
                result = hash64(row[index].Data.Uint64, result);
                break;
            case EValueType::Boolean:
                result = hash64(row[index].Data.Boolean, result);
                break;
            case EValueType::String:
                result = hash(
                    row[index].Data.String,
                    row[index].Length,
                    result);
                break;
            case EValueType::Null:
                result = hash64(0, result);
                break;
            default:
                YUNREACHABLE();
        }
    }

    return result;
}

// Combined FarmHash for TRow.
ui64 FarmHash(TRow row)
{
    return GetFarmFingerprint(row);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRoutines

////////////////////////////////////////////////////////////////////////////////

using NCodegen::TRoutineRegistry;

void RegisterQueryRoutinesImpl(TRoutineRegistry* registry)
{
#define REGISTER_ROUTINE(routine) \
    registry->RegisterRoutine(#routine, NRoutines::routine)
    REGISTER_ROUTINE(WriteRow);
    REGISTER_ROUTINE(ScanOpHelper);
    REGISTER_ROUTINE(JoinOpHelper);
    REGISTER_ROUTINE(GroupOpHelper);
    REGISTER_ROUTINE(StringHash);
    REGISTER_ROUTINE(FindRow);
    REGISTER_ROUTINE(InsertGroupRow);
    REGISTER_ROUTINE(InsertJoinRow);
    REGISTER_ROUTINE(SaveJoinRow);
    REGISTER_ROUTINE(AllocatePermanentRow);
    REGISTER_ROUTINE(AllocateRow);
    REGISTER_ROUTINE(GetRowsData);
    REGISTER_ROUTINE(GetRowsSize);
    REGISTER_ROUTINE(IsPrefix);
    REGISTER_ROUTINE(IsSubstr);
    REGISTER_ROUTINE(ToLower);
    REGISTER_ROUTINE(IsRowInArray);
    REGISTER_ROUTINE(SimpleHash);
    REGISTER_ROUTINE(FarmHash);

    registry->RegisterRoutine("TTopN::AddRow", TTopN::AddRow);
    REGISTER_ROUTINE(OrderOpHelper);
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

