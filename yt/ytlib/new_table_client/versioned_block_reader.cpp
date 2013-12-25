#include "stdafx.h"
#include "versioned_block_reader.h"
#include "versioned_block_writer.h"

#include <ytlib/transaction_client/public.h>

#include <core/misc/serialize.h>

namespace NYT {
namespace NVersionedTableClient {

using namespace NTransactionClient;
using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

int TSimpleVersionedBlockReader::FormatVersion = ETableChunkFormat::SimpleVersioned;

TSimpleVersionedBlockReader::TSimpleVersionedBlockReader(
    const TSharedRef& data,
    const TBlockMeta& meta,
    const TTableSchema& chunkSchema,
    const TKeyColumns& keyColumns,
    const std::vector<int>& schemaIdMapping,
    TTimestamp timestamp)
    : Timestamp_(timestamp)
    , KeyColumnCount_(keyColumns.size())
    , SchemaIdMapping_(schemaIdMapping)
    , Schema_(chunkSchema)
    , Meta_(meta)
{
    std::vector<TUnversionedValue> key(
        KeyColumnCount_,
        MakeUnversionedSentinelValue(EValueType::Null, 0));
    Key_ = TOwningKey(key.data(), key.data() + KeyColumnCount_);

    VersionedMeta_ = Meta_.GetExtension(TSimpleVersionedBlockMeta::block_meta_ext);

    KeyData_ = TRef(data.Begin(), TSimpleVersionedBlockWriter::GetPaddedKeySize(
        KeyColumnCount_,
        Schema_.Columns().size()) * Meta_.row_count());

    ValueData_ = TRef(
        KeyData_.End(),
        TSimpleVersionedBlockWriter::ValueSize * VersionedMeta_.value_count());

    TimestampsData_ = TRef(ValueData_.End(),
        TSimpleVersionedBlockWriter::TimestampSize * VersionedMeta_.timestamp_count());

    char* ptr = TimestampsData_.End();
    KeyNullFlags_.Reset(ptr, KeyColumnCount_ * Meta_.row_count());
    ptr += KeyNullFlags_.GetByteSize();

    ValueNullFlags_.Reset(ptr, VersionedMeta_.value_count());
    ptr += ValueNullFlags_.GetByteSize();

    StringData_ = TRef(ptr, data.End());

    JumpToRowIndex(0);
}

bool TSimpleVersionedBlockReader::NextRow()
{
    return JumpToRowIndex(RowIndex_ + 1);
}

void TSimpleVersionedBlockReader::SkipToRowIndex(int rowIndex)
{
    YCHECK(rowIndex >= RowIndex_);
    JumpToRowIndex(rowIndex);
}

void TSimpleVersionedBlockReader::SkipToKey(const TOwningKey& key)
{
    // Caller must ensure that given key doesn't exceed the last key of the block.

    if (GetKey() >= key) {
        // We are already further than pivot key.
        return;
    }

    int lowerIndex = RowIndex_;
    int upperIndex = Meta_.row_count();
    while (upperIndex - lowerIndex > 1) {
        auto middle = (upperIndex + lowerIndex) / 2;
        JumpToRowIndex(middle);
        if (GetKey() < key) {
            lowerIndex = middle;
        } else {
            upperIndex = middle;
        }
    }

    YCHECK(upperIndex < Meta_.row_count());
    JumpToRowIndex(upperIndex);
}

bool TSimpleVersionedBlockReader::JumpToRowIndex(int index)
{
    if (index >= Meta_.row_count()) {
        return false;
    }

    RowIndex_ = index;
    KeyDataPtr_ = KeyData_.Begin() + TSimpleVersionedBlockWriter::GetPaddedKeySize(
        KeyColumnCount_,
        Schema_.Columns().size()) * RowIndex_;

    for (int id = 0; id < KeyColumnCount_; ++id) {
        Key_[id] = ReadKeyValue(id);
    }

    TimestampOffset_ = *reinterpret_cast<i64*>(KeyDataPtr_);
    KeyDataPtr_ += sizeof(i64);

    ValueOffset_ = *reinterpret_cast<i64*>(KeyDataPtr_);
    KeyDataPtr_ += sizeof(i64);

    TimestampCount_ = *reinterpret_cast<ui32*>(KeyDataPtr_);
    KeyDataPtr_ += sizeof(ui32);

    return true;
}

TVersionedRow TSimpleVersionedBlockReader::GetRow(TChunkedMemoryPool *memoryPool)
{
    if (Timestamp_ == AllCommittedTimestamp) {
        return ReadAllValues(memoryPool);
    } else {
        return ReadValuesByTimestamp(memoryPool);
    }
}

ui32 TSimpleVersionedBlockReader::GetColumnValueCount(int schemaColumnId) const
{
    YASSERT(schemaColumnId >= KeyColumnCount_);
    return *(reinterpret_cast<ui32*>(KeyDataPtr_) + schemaColumnId - KeyColumnCount_);
}

TVersionedRow TSimpleVersionedBlockReader::ReadAllValues(TChunkedMemoryPool *memoryPool)
{
    TVersionedRow row = TVersionedRow::Allocate(
        memoryPool,
        KeyColumnCount_,
        GetColumnValueCount(Schema_.Columns().size() - 1),
        TimestampCount_);

    ::memcpy(row.BeginKeys(), Key_.Begin(), sizeof(TUnversionedValue) * KeyColumnCount_);

    for (int i = 0; i < TimestampCount_; ++i) {
        row.BeginTimestamps()[i] = ReadTimestamp(TimestampOffset_ + i);
    }

    int valueCount = 0;
    for (int valueId = 0; valueId < SchemaIdMapping_.size(); ++valueId) {
        int chunkSchemaId = SchemaIdMapping_[valueId];

        int lowerValueIndex = chunkSchemaId == KeyColumnCount_ ? 0 : GetColumnValueCount(chunkSchemaId - 1);
        int upperValueIndex = GetColumnValueCount(chunkSchemaId);

        for (int valueIndex = lowerValueIndex; valueIndex < upperValueIndex; ++valueIndex) {
            row.BeginValues()[valueCount] = ReadValue(ValueOffset_ + valueIndex, KeyColumnCount_ + valueId);
            ++valueCount;
        }
    }
    row.GetHeader()->ValueCount = valueCount;

    return row;
}

TVersionedRow TSimpleVersionedBlockReader::ReadValuesByTimestamp(TChunkedMemoryPool *memoryPool)
{
    int lowerTimestampIndex = 0; // larger timestamp
    int upperTimestampIndex = TimestampCount_; // smaller timestamp

    while (upperTimestampIndex - lowerTimestampIndex > 0) {
        int middle = (lowerTimestampIndex + upperTimestampIndex) / 2;
        auto ts = ReadTimestamp(TimestampOffset_ + middle);
        if (ts & TimestampValueMask > Timestamp_) {
            lowerTimestampIndex = middle;
        } else {
            upperTimestampIndex = middle;
        }
    }

    if (upperTimestampIndex == TimestampCount_) {
        // Row didn't exist at given timestamp.
        return TVersionedRow();
    }

    auto row = TVersionedRow::Allocate(
        memoryPool,
        KeyColumnCount_,
        SchemaIdMapping_.size(),
        1);

    ::memcpy(row.BeginKeys(), Key_.Begin(), sizeof(TUnversionedValue) * KeyColumnCount_);
    auto ts = ReadTimestamp(TimestampOffset_ + upperTimestampIndex);
    *row.BeginTimestamps() = ts;

    if (TombstoneTimestampMask & ts) {
        row.GetHeader()->ValueCount = 0;
        return row;
    }

    if (upperTimestampIndex == TimestampCount_ - 1) {
        row.BeginTimestamps()[0] |= IncrementalTimestampMask;
    }

    int valueCount = 0;
    for (int valueId = 0; valueId < SchemaIdMapping_.size(); ++valueId) {
        int chunkSchemaId = SchemaIdMapping_[valueId];

        int lowerValueIndex = chunkSchemaId == KeyColumnCount_ ? 0 : GetColumnValueCount(chunkSchemaId - 1);
        int upperValueIndex = GetColumnValueCount(chunkSchemaId);

        while (upperTimestampIndex - lowerTimestampIndex > 0) {
            int middle = (lowerValueIndex + upperValueIndex) / 2;
            auto value = ReadValue(TimestampOffset_ + middle, KeyColumnCount_ + valueId);
            if (value.Timestamp  > Timestamp_) {
                lowerTimestampIndex = middle;
            } else {
                upperTimestampIndex = middle;
            }
        }

        if (upperTimestampIndex < GetColumnValueCount(chunkSchemaId)) {
            auto value = ReadValue(TimestampOffset_ + upperTimestampIndex, KeyColumnCount_ + valueId);
            if (value.Timestamp >= TimestampValueMask & ts) {
                // Check that value didn't come from the previous incarnation of this row.
                row.BeginValues()[valueCount] = value;
                ++valueCount;
            }
        } else {
            // No value in the current column satisfy timestamp filtering.
        }
    }

    row.GetHeader()->ValueCount = valueCount;
    return row;
}

TUnversionedValue TSimpleVersionedBlockReader::ReadKeyValue(int id)
{
    char* valuePtr = KeyDataPtr_;
    KeyDataPtr_ += sizeof(i64);

    bool isNull = KeyNullFlags_[RowIndex_ * KeyColumnCount_ + id];
    if (isNull) {
        return MakeUnversionedSentinelValue(EValueType::Null, id);
    } else {
        switch (Schema_.Columns()[id].Type) {
            case EValueType::Integer:
                return MakeUnversionedIntegerValue(*reinterpret_cast<i64*>(valuePtr), id);

            case EValueType::Double:
                return MakeUnversionedDoubleValue(*reinterpret_cast<double*>(valuePtr), id);

            case EValueType::String:
                return MakeUnversionedStringValue(ReadString(valuePtr), id);

            case EValueType::Any:
                return MakeUnversionedAnyValue(ReadString(valuePtr), id);

            default:
                YUNREACHABLE();
        }
    }
}

TStringBuf TSimpleVersionedBlockReader::ReadString(char* ptr)
{
    ui32 offset = *reinterpret_cast<ui32*>(ptr);
    ptr += sizeof(ui32);

    ui32 length = *reinterpret_cast<ui32*>(ptr);

    return TStringBuf(StringData_.Begin() + offset, length);
}

TVersionedValue TSimpleVersionedBlockReader::ReadValue(int valueIndex, int id)
{
    YCHECK(id >= KeyColumnCount_);

}


////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
