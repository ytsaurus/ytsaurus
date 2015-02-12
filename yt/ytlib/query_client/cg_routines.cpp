#include "stdafx.h"
#include "cg_routines.h"
#include "cg_types.h"

#include "helpers.h"
#include "callbacks.h"
#include "query_statistics.h"

#include <ytlib/new_table_client/unversioned_row.h>
#include <ytlib/new_table_client/schemaful_reader.h>
#include <ytlib/new_table_client/schemaful_writer.h>
#include <ytlib/new_table_client/unordered_schemaful_reader.h>
#include <ytlib/new_table_client/row_buffer.h>

#include <ytlib/chunk_client/chunk_spec.h>

#include <core/concurrency/scheduler.h>

#include <core/profiling/scoped_timer.h>

#include <mutex>

namespace NYT {
namespace NQueryClient {
namespace NRoutines {

using namespace NConcurrency;

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



bool CountRow(i64* limit)
{
    if (*limit > 0) {
        --*limit;
        return false;
    } else {
        return true;
    }
}

void WriteRow(TRow row, TExecutionContext* executionContext)
{
    CHECK_STACK();

    if (CountRow(&executionContext->Limit)) {
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
        NProfiling::TAggregatingTimingGuard timingGuard(&executionContext->Statistics->AsyncTime);
        WaitFor(reader->Open(executionContext->Schema))
            .ThrowOnError();
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
            NProfiling::TAggregatingTimingGuard timingGuard(&executionContext->Statistics->AsyncTime);
            auto error = WaitFor(reader->GetReadyEvent());
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
    void (*consumeRows)(void** closure, std::vector<TRow>* rows, char* stopFlag)
    )
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

char Equal(
    const char* lhsData,
    ui32 lhsLength,
    const char* rhsData,
    ui32 rhsLength)
{
    return lhsLength == rhsLength && std::equal(lhsData, lhsData + lhsLength, rhsData);
}

char NotEqual(
    const char* lhsData,
    ui32 lhsLength,
    const char* rhsData,
    ui32 rhsLength)
{
    return !Equal(lhsData, lhsLength, rhsData, rhsLength);
}

char LexicographicalCompare(
    const char* lhsData,
    ui32 lhsLength,
    const char* rhsData,
    ui32 rhsLength)
{
    return std::lexicographical_compare(lhsData, lhsData + lhsLength, rhsData, rhsData + rhsLength);
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

char IsRowInArray(
    TExecutionContext* executionContext,
    char (*comparer)(TRow, TRow),
    TRow row,
    int index)
{
    // TODO(lukyan): check null
    auto rows = executionContext->LiteralRows->at(index);
    return std::binary_search(rows.begin(), rows.end(), row, TRowComparer(comparer));
}

size_t StringHash(
    const char* data,
    ui32 length)
{
    return TStringBuf(data, length).hash();
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
    REGISTER_ROUTINE(Equal);
    REGISTER_ROUTINE(NotEqual);
    REGISTER_ROUTINE(LexicographicalCompare);
    REGISTER_ROUTINE(ToLower);
    REGISTER_ROUTINE(IsRowInArray);
#undef REGISTER_ROUTINE
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

