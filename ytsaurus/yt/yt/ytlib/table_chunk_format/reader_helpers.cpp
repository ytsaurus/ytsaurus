#include "reader_helpers.h"

namespace NYT::NTableChunkFormat {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

void TrimRow(
    TMutableVersionedRow row,
    TVersionedValue* endValues,
    TChunkedMemoryPool* memoryPool)
{
    if (endValues != row.EndValues()) {
        auto* memoryTo = const_cast<char*>(row.EndMemory());
        row.SetValueCount(endValues - row.BeginValues());
        auto* memoryFrom = const_cast<char*>(row.EndMemory());
        memoryPool->Free(memoryFrom, memoryTo);
    }
}

TMutableVersionedRow IncreaseRowValueCount(
    TVersionedRow row,
    int newValueCount,
    TChunkedMemoryPool* memoryPool)
{
    YT_VERIFY(newValueCount >= row.GetValueCount());
    auto newRow = TMutableVersionedRow::Allocate(
        memoryPool,
        row.GetKeyCount(),
        newValueCount,
        row.GetWriteTimestampCount(),
        row.GetDeleteTimestampCount());
    std::copy(row.BeginKeys(), row.EndKeys(), newRow.BeginKeys());
    std::copy(row.BeginValues(), row.EndValues(), newRow.BeginValues());
    return newRow;
}

void FreeRow(
    TMutableVersionedRow row,
    TChunkedMemoryPool* memoryPool)
{
    memoryPool->Free(const_cast<char*>(row.BeginMemory()), const_cast<char*>(row.EndMemory()));
}

void SortRowValues(TMutableVersionedRow row)
{
    std::sort(
        row.BeginValues(),
        row.EndValues(),
        [] (const auto& lhs, const auto& rhs) {
            if (lhs.Id < rhs.Id) {
                return true;
            }
            if (lhs.Id > rhs.Id) {
                return false;
            }
            return lhs.Timestamp > rhs.Timestamp;
        });
}

void FilterSingleRowVersion(
    TMutableVersionedRow row,
    const bool* aggregateFlags,
    TChunkedMemoryPool* memoryPool)
{
    const auto* beginValues = row.BeginValues();;
    const auto* endValues = row.EndValues();;
    auto* output = row.BeginValues();
    for (const auto* input = beginValues; input != endValues; ++input) {
        if (input == beginValues || input->Id != (input - 1)->Id || aggregateFlags[input->Id]) {
            *output++ = *input;
        }
    }
    TrimRow(row, output, memoryPool);
}

int GetReaderSchemaColumnCount(TRange<TColumnIdMapping> schemaIdMapping)
{
    int result = 0;
    for (const auto& idMapping : schemaIdMapping) {
        result = std::max(result, idMapping.ReaderSchemaIndex + 1);
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat
