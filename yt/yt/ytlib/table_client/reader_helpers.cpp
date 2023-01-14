#include "reader_helpers.h"

namespace NYT::NTableClient::NDetail {

////////////////////////////////////////////////////////////////////////////////

void TrimRow(
    TMutableVersionedRow row,
    TVersionedValue* endValues,
    TChunkedMemoryPool* memoryPool)
{
    if (endValues != row.EndValues()) {
        auto* memoryTo = const_cast<char*>(row.BeginMemory());
        row.SetValueCount(endValues - row.BeginValues());
        auto* memoryFrom = const_cast<char*>(row.EndMemory());
        memoryPool->Free(memoryFrom, memoryTo);
    }
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

} // namespace NYT::NTableClient::NDetail
