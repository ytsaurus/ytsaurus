#include "stdafx.h"
#include "cg_routines.h"
#include "cg_routine_registry.h"

#include "helpers.h"
#include "callbacks.h"

#include <ytlib/new_table_client/unversioned_row.h>
#include <ytlib/new_table_client/schemaful_reader.h>
#include <ytlib/new_table_client/schemaful_writer.h>
#include <ytlib/new_table_client/row_buffer.h>

#include <ytlib/chunk_client/chunk_spec.h>

#include <core/concurrency/fiber.h>

#include <mutex>

namespace NYT {
namespace NQueryClient {
namespace NRoutines {

////////////////////////////////////////////////////////////////////////////////

using namespace NConcurrency;

void WriteRow(TRow row, TPassedFragmentParams* P)
{
#ifdef DEBUG
    int dummy;
    size_t currentStackSize = P->StackSizeGuardHelper - reinterpret_cast<size_t>(&dummy);
    YCHECK(currentStackSize < 10000);
#endif
    
    auto batch = P->Batch;
    auto writer = P->Writer;
    auto rowBuffer = P->RowBuffer;

    YASSERT(batch->size() < batch->capacity());

    batch->push_back(rowBuffer->Capture(row));

    if (batch->size() == batch->capacity()) {
        if (!writer->Write(*batch)) {
            auto error = WaitFor(writer->GetReadyEvent());
            THROW_ERROR_EXCEPTION_IF_FAILED(error);
        }
        batch->clear();
        rowBuffer->Clear();
    }
}

void ScanOpHelper(
    TPassedFragmentParams* P,
    int dataSplitsIndex,
    void** consumeRowsClosure,
    void (*consumeRows)(void** closure, TRow* rows, int size))
{
    auto callbacks = P->Callbacks;
    auto context = P->Context;
    auto dataSplits = (*P->DataSplitsArray)[dataSplitsIndex];

    for (const auto& dataSplit : dataSplits) {
        auto reader = callbacks->GetReader(dataSplit, context);
        auto schema = GetTableSchemaFromDataSplit(dataSplit);

        {
            auto error = WaitFor(reader->Open(schema));
            THROW_ERROR_EXCEPTION_IF_FAILED(error);
        }

        std::vector<TRow> rows;
        rows.reserve(MaxRowsPerRead);

        while (true) {
            P->ScratchSpace->Clear();

            bool hasMoreData = reader->Read(&rows);
            bool shouldWait = rows.empty();
            consumeRows(consumeRowsClosure, rows.data(), rows.size());
            rows.clear();

            if (!hasMoreData) {
                break;
            }

            if (shouldWait) {
                auto error = WaitFor(reader->GetReadyEvent());
                THROW_ERROR_EXCEPTION_IF_FAILED(error);
            }
        }
    }
}

void GroupOpHelper(
    int keySize,
    int aggregateItemCount,
    void** consumeRowsClosure,
    void (*consumeRows)(
        void** closure,
        std::vector<TRow>* groupedRows,
        TLookupRows* rows))
{
    std::vector<TRow> groupedRows;
    TLookupRows lookupRows(
        256,
        NDetail::TGroupHasher(keySize),
        NDetail::TGroupComparer(keySize));

    consumeRows(consumeRowsClosure, &groupedRows, &lookupRows);
}

const TRow* FindRow(TPassedFragmentParams* P, TLookupRows* rows, TRow row)
{
#ifdef DEBUG
    int dummy;
    size_t currentStackSize = P->StackSizeGuardHelper - reinterpret_cast<size_t>(&dummy);
    YCHECK(currentStackSize < 10000);
#endif

    auto it = rows->find(row);
    return it != rows->end()? &*it : nullptr;
}

void AddRow(
    TPassedFragmentParams* P,
    TLookupRows* lookupRows,
    std::vector<TRow>* groupedRows,
    TRow* newRow,
    int rowSize)
{
#ifdef DEBUG
    int dummy;
    size_t currentStackSize = P->StackSizeGuardHelper - reinterpret_cast<size_t>(&dummy);
    YCHECK(currentStackSize < 10000);
#endif

    groupedRows->push_back(P->RowBuffer->Capture(*newRow));
    lookupRows->insert(groupedRows->back());
    *newRow = TRow::Allocate(P->ScratchSpace, rowSize);
}

void AllocateRow(TPassedFragmentParams* P, int rowSize, TRow* rowPtr)
{
#ifdef DEBUG
    int dummy;
    size_t currentStackSize = P->StackSizeGuardHelper - reinterpret_cast<size_t>(&dummy);
    YCHECK(currentStackSize < 10000);
#endif

    *rowPtr = TRow::Allocate(P->ScratchSpace, rowSize);
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

} // namespace NRoutines

////////////////////////////////////////////////////////////////////////////////

void RegisterCGRoutinesImpl()
{
#define REGISTER_ROUTINE(routine) \
    TRoutineRegistry::RegisterRoutine(#routine, NRoutines::routine)
    REGISTER_ROUTINE(WriteRow);
    REGISTER_ROUTINE(ScanOpHelper);
    REGISTER_ROUTINE(GroupOpHelper);
    REGISTER_ROUTINE(FindRow);
    REGISTER_ROUTINE(AddRow);
    REGISTER_ROUTINE(AllocateRow);
    REGISTER_ROUTINE(GetRowsData);
    REGISTER_ROUTINE(GetRowsSize);
#undef REGISTER_ROUTINE
}

void RegisterCGRoutines()
{
    static std::once_flag onceFlag;
    std::call_once(onceFlag, &RegisterCGRoutinesImpl);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NQueryClient
} // namespace NYT

