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

void WriteRow(
    TRow row,
    std::vector<TRow>* batch,
    ISchemafulWriter* writer)
{
    YASSERT(batch->size() < batch->capacity());

    batch->push_back(row);

    if (batch->size() == batch->capacity()) {
        if (!writer->Write(*batch)) {
            auto error = WaitFor(writer->GetReadyEvent());
            THROW_ERROR_EXCEPTION_IF_FAILED(error);
        }
        batch->clear();
    }
}

void ScanOpHelper(
    TPassedFragmentParams* passedFragmentParams,
    int dataSplitsIndex,
    void** consumeRowsClosure,
    void (*consumeRows)(void** closure, TRow* rows, int size))
{
    auto callbacks = passedFragmentParams->Callbacks;
    auto context = passedFragmentParams->Context;
    auto dataSplits = (*passedFragmentParams->DataSplitsArray)[dataSplitsIndex];

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

const TRow* FindRow(TLookupRows* rows, TRow row)
{
    auto it = rows->find(row);
    return it != rows->end()? &*it : nullptr;
}

void AddRow(
    TRowBuffer* rowBuffer,
    TLookupRows* lookupRows, // lookup table
    std::vector<TRow>* groupedRows,
    TRow* newRow,
    int rowSize)
{
    groupedRows->push_back(*newRow);
    lookupRows->insert(groupedRows->back());
    *newRow = TRow::Allocate(rowBuffer->GetAlignedPool(), rowSize);
}

void AllocateRow(TRowBuffer* rowBuffer, int rowSize, TRow* rowPtr)
{
    *rowPtr = TRow::Allocate(rowBuffer->GetAlignedPool(), rowSize);
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

