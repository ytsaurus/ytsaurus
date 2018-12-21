#include "schemaless_row_reorderer.h"

#include <yt/client/table_client/name_table.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

TSchemalessRowReorderer::TSchemalessRowReorderer(
    TNameTablePtr nameTable,
    const TKeyColumns& keyColumns)
    : KeyColumns_(keyColumns)
    , NameTable_(nameTable)
{
    EmptyKey_.resize(KeyColumns_.size(), MakeUnversionedSentinelValue(EValueType::Null));
    for (int i = 0; i < KeyColumns_.size(); ++i) {
        auto id = NameTable_->GetIdOrRegisterName(KeyColumns_[i]);
        EmptyKey_[i].Id = id;
        if (id >= IdMapping_.size()) {
            IdMapping_.resize(id + 1, -1);
        }
        IdMapping_[id] = i;
    }
}

TMutableUnversionedRow TSchemalessRowReorderer::ReorderRow(TUnversionedRow row, TChunkedMemoryPool* memoryPool)
{
    int valueCount = KeyColumns_.size() + row.GetCount();
    auto result = TMutableUnversionedRow::Allocate(memoryPool, valueCount);

    // Initialize with empty key.
    ::memcpy(result.Begin(), EmptyKey_.data(), KeyColumns_.size() * sizeof(TUnversionedValue));

    int nextValueIndex = KeyColumns_.size();
    for (auto it = row.Begin(); it != row.End(); ++it) {
        const auto& value = *it;
        if (value.Id < IdMapping_.size()) {
            int keyIndex = IdMapping_[value.Id];
            if (keyIndex >= 0) {
                result.Begin()[keyIndex] = value;
                --valueCount;
                continue;
            }
        }
        result.Begin()[nextValueIndex] = value;
        ++nextValueIndex;
    }

    result.SetCount(valueCount);
    return result;
}

TMutableUnversionedRow TSchemalessRowReorderer::ReorderKey(TUnversionedRow row, TChunkedMemoryPool* memoryPool)
{
    auto result = TMutableUnversionedRow::Allocate(memoryPool, KeyColumns_.size());
    // Initialize with empty key.
    ::memcpy(result.Begin(), EmptyKey_.data(), KeyColumns_.size() * sizeof(TUnversionedValue));

    for (auto it = row.Begin(); it != row.End(); ++it) {
        const auto& value = *it;
        if (value.Id < IdMapping_.size()) {
            int keyIndex = IdMapping_[value.Id];
            if (keyIndex >= 0) {
                result.Begin()[keyIndex] = value;
            }
        }
    }
    return result;
}

TUnversionedOwningRow TSchemalessRowReorderer::ReorderRow(TUnversionedRow row)
{
    auto result = EmptyKey_;

    for (auto it = row.Begin(); it != row.End(); ++it) {
        const auto& value = *it;
        if (value.Id < IdMapping_.size()) {
            int keyIndex = IdMapping_[value.Id];
            if (keyIndex >= 0) {
                result[keyIndex] = value;
                continue;
            }
        }
        result.push_back(value);
    }

    return TUnversionedOwningRow(result.data(), result.data() + result.size());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
