#include "schemaless_row_reorderer.h"

#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/row_buffer.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

TSchemalessRowReorderer::TSchemalessRowReorderer(
    TNameTablePtr nameTable,
    TRowBufferPtr rowBuffer,
    bool deepCapture,
    const TKeyColumns& keyColumns)
    : KeyColumns_(keyColumns)
    , RowBuffer_(std::move(rowBuffer))
    , DeepCapture_(deepCapture)
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

TMutableUnversionedRow TSchemalessRowReorderer::ReorderRow(TUnversionedRow row)
{
    int valueCount = KeyColumns_.size() + row.GetCount();
    auto result = RowBuffer_->AllocateUnversioned(valueCount);

    // Initialize with empty key.
    ::memcpy(result.Begin(), EmptyKey_.data(), KeyColumns_.size() * sizeof(TUnversionedValue));

    int nextValueIndex = KeyColumns_.size();
    int idMappingSize = static_cast<int>(IdMapping_.size());
    for (auto value : row) {
        if (DeepCapture_) {
            RowBuffer_->Capture(&value);
        }
        if (value.Id < idMappingSize) {
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

TMutableUnversionedRow TSchemalessRowReorderer::ReorderKey(TUnversionedRow row)
{
    auto result = RowBuffer_->AllocateUnversioned(KeyColumns_.size());

    // Initialize with empty key.
    ::memcpy(result.Begin(), EmptyKey_.data(), KeyColumns_.size() * sizeof(TUnversionedValue));

    int idMappingSize = static_cast<int>(IdMapping_.size());
    for (auto value : row) {
        if (DeepCapture_) {
            RowBuffer_->Capture(&value);
        }
        if (value.Id < idMappingSize) {
            int keyIndex = IdMapping_[value.Id];
            if (keyIndex >= 0) {
                result.Begin()[keyIndex] = value;
            }
        }
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
