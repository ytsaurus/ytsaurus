#include "schemaless_block_reader.h"
#include "private.h"
#include "helpers.h"

#include <yt/yt/client/table_client/key_bound.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/misc/algorithm_helpers.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

THorizontalBlockReader::THorizontalBlockReader(
    const TSharedRef& block,
    const NProto::TBlockMeta& meta,
    const TTableSchemaPtr& schema,
    const std::vector<TColumnIdMapping>& idMapping,
    const TComparator& chunkComparator,
    const TComparator& comparator,
    int extraColumnCount)
    : Block_(block)
    , Meta_(meta)
    , IdMapping_(idMapping)
    , ChunkComparator_(chunkComparator)
    , Comparator_(comparator)
    , ExtraColumnCount_(extraColumnCount)
{
    YT_VERIFY(Comparator_.GetLength() >= ChunkComparator_.GetLength());
    YT_VERIFY(Meta_.row_count() > 0);

    auto keyDataSize = GetUnversionedRowByteSize(Comparator_.GetLength());
    KeyBuffer_.reserve(keyDataSize);
    Key_ = TMutableUnversionedRow::Create(KeyBuffer_.data(), Comparator_.GetLength());

    // NB: First key of the block will be initialized below during JumpToRowIndex(0) call.
    // Nulls here are used to widen chunk's key to comparator length.
    for (int index = 0; index < Comparator_.GetLength(); ++index) {
        Key_[index] = MakeUnversionedSentinelValue(EValueType::Null, index);
    }

    i64 offsetsLength = sizeof(ui32) * Meta_.row_count();

    Offsets_ = TRef(Block_.Begin(), Block_.Begin() + offsetsLength);
    Data_ = TRef(Offsets_.End(), Block_.End());

    const auto& schemaColumns = schema->Columns();
    IsCompositeColumn_.assign(schemaColumns.size(), false);
    for (int i = 0; i < std::ssize(schemaColumns); ++i) {
        IsCompositeColumn_[i] = IsV3Composite(schemaColumns[i].LogicalType());
    }

    JumpToRowIndex(0);
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

bool THorizontalBlockReader::SkipToKeyBound(const TKeyBound& lowerBound)
{
    YT_VERIFY(lowerBound);
    YT_VERIFY(!lowerBound.IsUpper);

    if (Comparator_.TestKey(GetKey(), lowerBound)) {
        return true;
    }

    // BinarySearch returns first element such that !pred(it).
    // We are looking for the first row such that Comparator_.TestKey(row, lowerBound);
    auto index = BinarySearch(
        RowIndex_,
        Meta_.row_count(),
        [&] (i64 index) {
            YT_VERIFY(JumpToRowIndex(index));
            return !Comparator_.TestKey(GetKey(), lowerBound);
        });

    return JumpToRowIndex(index);
}

bool THorizontalBlockReader::SkipToKey(const TLegacyKey key)
{
    auto keyBound = KeyBoundFromLegacyRow(key, /* isUpper */ false, Comparator_.GetLength());
    return SkipToKeyBound(keyBound);
}

TLegacyKey THorizontalBlockReader::GetLegacyKey() const
{
    return Key_;
}

TKey THorizontalBlockReader::GetKey() const
{
    return TKey::FromRowUnchecked(Key_, Comparator_.GetLength());
}

TMutableUnversionedRow THorizontalBlockReader::GetRow(TChunkedMemoryPool* memoryPool)
{
    auto row = TMutableUnversionedRow::Allocate(memoryPool, ValueCount_ + ExtraColumnCount_);
    int valueCount = 0;

    for (int i = 0; i < static_cast<int>(ValueCount_); ++i) {
        TUnversionedValue value;
        CurrentPointer_ += ReadRowValue(CurrentPointer_, &value);

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
    for (int i = 0; i < static_cast<int>(ValueCount_); ++i) {
        TUnversionedValue value;
        currentPointer += ReadRowValue(currentPointer, &value);

        if (IdMapping_[value.Id].ReaderSchemaIndex >= Comparator_.GetLength()) {
            ++valueCount;
        }
    }

    auto versionedRow = TMutableVersionedRow::Allocate(
        memoryPool,
        Comparator_.GetLength(),
        valueCount,
        1,
        0);

    for (int index = 0; index < Comparator_.GetLength(); ++index) {
        versionedRow.BeginKeys()[index] = MakeUnversionedSentinelValue(EValueType::Null, index);
    }

    TVersionedValue* currentValue = versionedRow.BeginValues();
    for (int i = 0; i < static_cast<int>(ValueCount_); ++i) {
        TUnversionedValue value;
        CurrentPointer_ += ReadRowValue(CurrentPointer_, &value);

        int id = IdMapping_[value.Id].ReaderSchemaIndex;
        if (id >= Comparator_.GetLength()) {
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
    YT_VERIFY(static_cast<int>(ValueCount_) >= ChunkComparator_.GetLength());

    const char* ptr = CurrentPointer_;
    for (int i = 0; i < ChunkComparator_.GetLength(); ++i) {
        auto* currentKeyValue = Key_.Begin() + i;
        ptr += ReadRowValue(ptr, currentKeyValue);
        if (currentKeyValue->Type == EValueType::Any
            && currentKeyValue->Id < IsCompositeColumn_.size()
            && IsCompositeColumn_[currentKeyValue->Id])
        {
            currentKeyValue->Type = EValueType::Composite;
        }
    }

    return true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
