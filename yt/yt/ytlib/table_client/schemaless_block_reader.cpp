#include "schemaless_block_reader.h"
#include "private.h"
#include "helpers.h"

#include <yt/client/table_client/schema.h>

#include <yt/core/misc/algorithm_helpers.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

THorizontalBlockReader::THorizontalBlockReader(
    const TSharedRef& block,
    const NProto::TBlockMeta& meta,
    const TTableSchema& schema,
    const std::vector<TColumnIdMapping>& idMapping,
    int chunkKeyColumnCount,
    int keyColumnCount,
    int extraColumnCount)
    : Block_(block)
    , Meta_(meta)
    , IdMapping_(idMapping)
    , ChunkKeyColumnCount_(chunkKeyColumnCount)
    , KeyColumnCount_(keyColumnCount)
    , ExtraColumnCount_(extraColumnCount)
{
    YT_VERIFY(Meta_.row_count() > 0);

    keyColumnCount = std::max(KeyColumnCount_, ChunkKeyColumnCount_);
    auto keyDataSize = GetUnversionedRowByteSize(KeyColumnCount_);
    KeyBuffer_.reserve(keyDataSize);
    Key_ = TMutableKey::Create(KeyBuffer_.data(), KeyColumnCount_);

    for (int index = 0; index < KeyColumnCount_; ++index) {
        Key_[index] = MakeUnversionedSentinelValue(EValueType::Null, index);
    }

    i64 offsetsLength = sizeof(ui32) * Meta_.row_count();

    Offsets_ = TRef(Block_.Begin(), Block_.Begin() + offsetsLength);
    Data_ = TRef(Offsets_.End(), Block_.End());

    JumpToRowIndex(0);

    const auto& schemaColumns = schema.Columns();
    IsCompositeColumn_.assign(schemaColumns.size(), false);
    for (int i = 0; i < schemaColumns.size(); ++i) {
        auto isSimpleType = schemaColumns[i].SimplifiedLogicalType().has_value();
        if (!isSimpleType) {
            IsCompositeColumn_[i] = true;
        }
    }
}

bool THorizontalBlockReader::NextRow()
{
    return JumpToRowIndex(RowIndex_ + 1);
}

bool THorizontalBlockReader::SkipToRowIndex(i64 rowIndex)
{
    YT_VERIFY(rowIndex >= RowIndex_);
    return JumpToRowIndex(rowIndex);
}

bool THorizontalBlockReader::SkipToKey(const TKey key)
{
    if (GetKey() >= key) {
        // We are already further than pivot key.
        return true;
    }

    auto index = BinarySearch(
        RowIndex_,
        Meta_.row_count(),
        [&] (i64 index) {
            YT_VERIFY(JumpToRowIndex(index));
            return GetKey() < key;
        });

    return JumpToRowIndex(index);
}

TKey THorizontalBlockReader::GetKey() const
{
    return Key_;
}

TMutableUnversionedRow THorizontalBlockReader::GetRow(TChunkedMemoryPool* memoryPool)
{
    auto row = TMutableUnversionedRow::Allocate(memoryPool, ValueCount_ + ExtraColumnCount_);
    int valueCount = 0;

    for (int i = 0; i < ValueCount_; ++i) {
        TUnversionedValue value;
        CurrentPointer_ += ReadValue(CurrentPointer_, &value);

        const auto remappedId = IdMapping_[value.Id].ReaderSchemaIndex;
        if (remappedId >= 0) {
            if (value.Type == EValueType::Any) {
                auto data = TStringBuf(value.Data.String, value.Length);
                // Try to unpack any value.
                if (value.Id < IsCompositeColumn_.size() && IsCompositeColumn_[value.Id]) {
                    value.Type = EValueType::Composite;
                } else {
                    value = MakeUnversionedValue(data, value.Id, Lexer_);
                }
            }
            value.Id = remappedId;
            row[valueCount] = value;
            ++valueCount;
        }
    }
    row.SetCount(valueCount);
    return row;
}

TMutableVersionedRow THorizontalBlockReader::GetVersionedRow(
    TChunkedMemoryPool* memoryPool,
    TTimestamp timestamp)
{
    int valueCount = 0;
    auto currentPointer = CurrentPointer_;
    for (int i = 0; i < ValueCount_; ++i) {
        TUnversionedValue value;
        currentPointer += ReadValue(currentPointer, &value);

        if (IdMapping_[value.Id].ReaderSchemaIndex >= KeyColumnCount_) {
            ++valueCount;
        }
    }

    auto versionedRow = TMutableVersionedRow::Allocate(
        memoryPool,
        KeyColumnCount_,
        valueCount,
        1,
        0);

    for (int index = 0; index < KeyColumnCount_; ++index) {
        versionedRow.BeginKeys()[index] = MakeUnversionedSentinelValue(EValueType::Null, index);
    }

    TVersionedValue* currentValue = versionedRow.BeginValues();
    for (int i = 0; i < ValueCount_; ++i) {
        TUnversionedValue value;
        CurrentPointer_ += ReadValue(CurrentPointer_, &value);

        int id = IdMapping_[value.Id].ReaderSchemaIndex;
        if (id >= KeyColumnCount_) {
            value.Id = id;
            if (value.Type == EValueType::Any) {
                auto data = TStringBuf(value.Data.String, value.Length);
                // Try to unpack any value.
                if (value.Id < IsCompositeColumn_.size() && IsCompositeColumn_[value.Id]) {
                    value.Type = EValueType::Composite;
                } else {
                    value = MakeUnversionedValue(data, value.Id, Lexer_);
                }
            }
            *currentValue = MakeVersionedValue(value, timestamp);
            ++currentValue;
        } else if (id >= 0) {
            versionedRow.BeginKeys()[id] = value;
        }
    }

    versionedRow.BeginWriteTimestamps()[0] = timestamp;

    return versionedRow;
}


i64 THorizontalBlockReader::GetRowIndex() const
{
    return RowIndex_;
}

bool THorizontalBlockReader::JumpToRowIndex(i64 rowIndex)
{
    if (rowIndex >= Meta_.row_count()) {
        return false;
    }

    RowIndex_ = rowIndex;

    ui32 offset = *reinterpret_cast<const ui32*>(Offsets_.Begin() + rowIndex * sizeof(ui32));
    CurrentPointer_ = Data_.Begin() + offset;

    CurrentPointer_ += ReadVarUint32(CurrentPointer_, &ValueCount_);
    YT_VERIFY(ValueCount_ >= ChunkKeyColumnCount_);

    const char* ptr = CurrentPointer_;
    for (int i = 0; i < ChunkKeyColumnCount_; ++i) {
        ptr += ReadValue(ptr, Key_.Begin() + i);
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
