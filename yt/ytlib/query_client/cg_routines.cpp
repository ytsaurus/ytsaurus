#include "stdafx.h"
#include "cg_routines.h"
#include "cg_types.h"

#include "helpers.h"
#include "callbacks.h"
#include "query_statistics.h"
#include "evaluation_helpers.h"

#include <ytlib/table_client/unversioned_row.h>
#include <ytlib/table_client/schemaful_reader.h>
#include <ytlib/table_client/schemaful_writer.h>
#include <ytlib/table_client/unordered_schemaful_reader.h>
#include <ytlib/table_client/row_buffer.h>

#include <ytlib/chunk_client/chunk_spec.h>

#include <core/concurrency/scheduler.h>

#include <core/profiling/scoped_timer.h>

#include <core/misc/farm_hash.h>

#include <mutex>

namespace NYT {
namespace NQueryClient {
namespace NRoutines {

using namespace NConcurrency;
using namespace NTableClient;

static const auto& Logger = QueryClientLogger;

////////////////////////////////////////////////////////////////////////////////

#ifndef NDEBUG
#define CHECK_STACK() \
    { \
        int dummy; \
        size_t currentStackSize = context->StackSizeGuardHelper - reinterpret_cast<intptr_t>(&dummy); \
        YCHECK(currentStackSize < 10000); \
    }
#else
#define CHECK_STACK() (void) 0;
#endif

////////////////////////////////////////////////////////////////////////////////

void WriteRow(TRow row, TExecutionContext* context)
{
    CHECK_STACK();

    if (!UpdateAndCheckRowLimit(&context->Limit, &context->StopFlag)) {
        return;
    }

    if (!UpdateAndCheckRowLimit(&context->OutputRowLimit, &context->StopFlag)) {
        context->Statistics->IncompleteOutput = true;
        return;
    }
    
    ++context->Statistics->RowsWritten;

    auto* batch = context->OutputRowsBatch;
    
    const auto& rowBuffer = context->OutputBuffer;

    YASSERT(batch->size() < batch->capacity());
    batch->push_back(rowBuffer->Capture(row));

    if (batch->size() == batch->capacity()) {
        auto& writer = context->Writer;
        bool shouldNotWait;
        {
            NProfiling::TAggregatingTimingGuard timingGuard(&context->Statistics->WriteTime);
            shouldNotWait = writer->Write(*batch);
        }

        if (!shouldNotWait) {
            NProfiling::TAggregatingTimingGuard timingGuard(&context->Statistics->AsyncTime);
            WaitFor(writer->GetReadyEvent())
                .ThrowOnError();
        }
        batch->clear();
        rowBuffer->Clear();
    }
}

void ScanOpHelper(
    TExecutionContext* context,
    void** consumeRowsClosure,
    void (*consumeRows)(void** closure, TRow* rows, int size, char* stopFlag))
{
    auto& reader = context->Reader;

    std::vector<TRow> rows;
    rows.reserve(MaxRowsPerRead);

    context->StopFlag = false;

    while (true) {
        context->IntermediateBuffer->Clear();

        bool hasMoreData;
        {
            NProfiling::TAggregatingTimingGuard timingGuard(&context->Statistics->ReadTime);
            hasMoreData = reader->Read(&rows);
        }

        bool shouldWait = rows.empty();

        // Remove null rows.
        rows.erase(
            std::remove_if(rows.begin(), rows.end(), [] (TRow row) {
                return !row;
            }),
            rows.end());

        if (context->InputRowLimit < rows.size()) {
            rows.resize(context->InputRowLimit);
            context->Statistics->IncompleteInput = true;
            hasMoreData = false;
        }
        context->InputRowLimit -= rows.size();
        context->Statistics->RowsRead += rows.size();

        consumeRows(consumeRowsClosure, rows.data(), rows.size(), &context->StopFlag);
        rows.clear();

        if (!hasMoreData || context->StopFlag) {
            break;
        }

        if (shouldWait) {
            LOG_DEBUG("Started waiting for more rows");
            NProfiling::TAggregatingTimingGuard timingGuard(&context->Statistics->AsyncTime);
            WaitFor(reader->GetReadyEvent())
                .ThrowOnError();
            LOG_DEBUG("Finished waiting for more rows");
        }
    }
}

void InsertJoinRow(
    TExecutionContext* context,
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
            context->PermanentBuffer->Capture(&row[index]);
        }
        *rowPtr = TRow::Allocate(context->PermanentBuffer->GetPool(), valueCount);
    }
}

void SaveJoinRow(
    TExecutionContext* context,
    std::vector<TRow>* rows,
    TRow row)
{
    CHECK_STACK();

    rows->push_back(context->PermanentBuffer->Capture(row));
}

void JoinOpHelper(
    TExecutionContext* context,
    int index,
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

    std::sort(keys.begin(), keys.end());

    LOG_DEBUG("Collected %v join keys from %v rows",
        keys.size(),
        allRows.size());

    std::vector<TRow> joinedRows;
    context->JoinEvaluators[index](
        context,
        groupHasher,
        groupComparer,
        keys,
        allRows,
        &joinedRows);

    LOG_DEBUG("Joined into %v rows",
        joinedRows.size());

    // Consume joined rows.
    context->StopFlag = false;
    consumeRows(consumeRowsClosure, &joinedRows, &context->StopFlag);
}

void GroupOpHelper(
    TExecutionContext* context,
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

    context->StopFlag = false;
    consumeRows(consumeRowsClosure, &groupedRows, &context->StopFlag);
}

const TRow* FindRow(TExpressionContext* context, TLookupRows* rows, TRow row)
{
    CHECK_STACK();

    auto it = rows->find(row);
    return it != rows->end()? &*it : nullptr;
}

void AllocatePermanentRow(TExecutionContext* context, int valueCount, TRow* row)
{
    CHECK_STACK();

    *row = TRow::Allocate(context->PermanentBuffer->GetPool(), valueCount);
}

const TRow* InsertGroupRow(
    TExecutionContext* context,
    TLookupRows* lookupRows,
    std::vector<TRow>* groupedRows,
    TRow row,
    int keySize)
{
    CHECK_STACK();

    auto inserted = lookupRows->insert(row);

    if (inserted.second) {
        if (!UpdateAndCheckRowLimit(&context->GroupRowLimit, &context->StopFlag)) {
            context->Statistics->IncompleteOutput = true;
            return nullptr;
        }

        groupedRows->push_back(row);
        for (int index = 0; index < keySize; ++index) {
            context->PermanentBuffer->Capture(&row[index]);
        }
    }

    return &*inserted.first;
}

void AllocateRow(TExpressionContext* context, int valueCount, TRow* row)
{
    CHECK_STACK();

    *row = TRow::Allocate(context->IntermediateBuffer->GetPool(), valueCount);
}

TRow* GetRowsData(std::vector<TRow>* groupedRows)
{
    return groupedRows->data();
}

int GetRowsSize(std::vector<TRow>* groupedRows)
{
    return groupedRows->size();
}

void AddRow(TTopCollector* topN, TRow row)
{
    topN->AddRow(row);
}

void OrderOpHelper(
    TExecutionContext* context,
    char (*comparer)(TRow, TRow),
    void** collectRowsClosure,
    void (*collectRows)(void** closure, TTopCollector* topN),
    void** consumeRowsClosure,
    void (*consumeRows)(void** closure, std::vector<TRow>* rows, char* stopFlag))
{
    auto limit = context->Limit;

    TTopCollector topN(limit, comparer);
    collectRows(collectRowsClosure, &topN);
    auto rows = topN.GetRows();

    // Consume joined rows.
    context->StopFlag = false;
    consumeRows(consumeRowsClosure, &rows, &context->StopFlag);
}

char* AllocateBytes(TExecutionContext* context, size_t byteCount)
{
    CHECK_STACK();

    return context
        ->IntermediateBuffer
        ->GetPool()
        ->AllocateUnaligned(byteCount);
}

char* AllocatePermanentBytes(TExecutionContext* context, size_t byteCount)
{
    CHECK_STACK();

    return context
        ->PermanentBuffer
        ->GetPool()
        ->AllocateUnaligned(byteCount);
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
    TExpressionContext* context,
    const char* data,
    ui32 length)
{
    char* result = context->IntermediateBuffer->GetPool()->AllocateUnaligned(length);

    for (ui32 index = 0; index < length; ++index) {
        result[index] = tolower(data[index]);
    }

    return result;
}

char IsRowInArray(
    TExpressionContext* context,
    char (*comparer)(TRow, TRow),
    TRow row,
    int index)
{
    // TODO(lukyan): check null
    const auto& rows = (*context->LiteralRows)[index];
    return std::binary_search(rows.Begin(), rows.End(), row, comparer);
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
                YUNREACHABLE();
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
    REGISTER_ROUTINE(AllocatePermanentBytes);
    REGISTER_ROUTINE(AllocateBytes);
    REGISTER_ROUTINE(GetRowsData);
    REGISTER_ROUTINE(GetRowsSize);
    REGISTER_ROUTINE(IsPrefix);
    REGISTER_ROUTINE(IsSubstr);
    REGISTER_ROUTINE(IsRowInArray);
    REGISTER_ROUTINE(SimpleHash);
    REGISTER_ROUTINE(FarmHashUint64);
    REGISTER_ROUTINE(AddRow);
    REGISTER_ROUTINE(OrderOpHelper);
    REGISTER_ROUTINE(ThrowException);
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

