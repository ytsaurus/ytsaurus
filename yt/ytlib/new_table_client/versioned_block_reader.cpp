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

int LowerBound(int lowerIndex, int upperIndex, std::function<bool(int)> less)
{
    while (upperIndex - lowerIndex > 0) {
        auto middle = (upperIndex + lowerIndex) / 2;
        if (less(middle)) {
            lowerIndex = middle + 1;
        } else {
            upperIndex = middle;
        }
    }
    return lowerIndex;
}

int TSimpleVersionedBlockReader::FormatVersion = ETableChunkFormat::SimpleVersioned;

TSimpleVersionedBlockReader::TSimpleVersionedBlockReader(const TSharedRef& data,
    const TBlockMeta& meta,
    const TTableSchema& chunkSchema,
    const TKeyColumns& keyColumns,
    const std::vector<TColumnIdMapping>& schemaIdMapping,
    TTimestamp timestamp)
    : Timestamp_(timestamp)
    , KeyColumnCount_(keyColumns.size())
    , SchemaIdMapping_(schemaIdMapping)
    , Schema_(chunkSchema)
    , Meta_(meta)
    , Closed_(false)
{
    YCHECK(Meta_.row_count() > 0);

    std::vector<TUnversionedValue> key(
        KeyColumnCount_,
        MakeUnversionedSentinelValue(EValueType::Null, 0));
    Key_ = TOwningKey(key.data(), key.data() + KeyColumnCount_);

    VersionedMeta_ = Meta_.GetExtension(TSimpleVersionedBlockMeta::block_meta_ext);

    KeyData_ = TRef(const_cast<char*>(data.Begin()), TSimpleVersionedBlockWriter::GetPaddedKeySize(
        KeyColumnCount_,
        Schema_.Columns().size()) * Meta_.row_count());

    ValueData_ = TRef(
        KeyData_.End(),
        TSimpleVersionedBlockWriter::ValueSize * VersionedMeta_.value_count());

    TimestampsData_ = TRef(ValueData_.End(),
        TSimpleVersionedBlockWriter::TimestampSize * VersionedMeta_.timestamp_count());

    const char* ptr = TimestampsData_.End();
    KeyNullFlags_.Reset(reinterpret_cast<const ui64*>(ptr), KeyColumnCount_ * Meta_.row_count());
    ptr += KeyNullFlags_.GetByteSize();

    ValueNullFlags_.Reset(reinterpret_cast<const ui64*>(ptr), VersionedMeta_.value_count());
    ptr += ValueNullFlags_.GetByteSize();

    StringData_ = TRef(const_cast<char*>(ptr), const_cast<char*>(data.End()));

    JumpToRowIndex(0);
}

bool TSimpleVersionedBlockReader::NextRow()
{
    YCHECK(!Closed_);
    return JumpToRowIndex(RowIndex_ + 1);
}

bool TSimpleVersionedBlockReader::SkipToRowIndex(int rowIndex)
{
    YCHECK(!Closed_);
    YCHECK(rowIndex >= RowIndex_);
    return JumpToRowIndex(rowIndex);
}

bool TSimpleVersionedBlockReader::SkipToKey(const TOwningKey& key)
{
    YCHECK(!Closed_);

    if (GetKey() >= key) {
        // We are already further than pivot key.
        return true;
    }

    auto index = LowerBound(
        RowIndex_,
        Meta_.row_count(),
        [&] (int index) {
            YCHECK(JumpToRowIndex(index));
            return GetKey() < key;
        });

    return JumpToRowIndex(index);
}

bool TSimpleVersionedBlockReader::JumpToRowIndex(int index)
{
    YCHECK(!Closed_);

    if (index >= Meta_.row_count()) {
        Closed_ = true;
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
    YCHECK(!Closed_);
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
    for (const auto& mapping : SchemaIdMapping_) {
        int valueId = mapping.ReaderSchemaIndex;
        int chunkSchemaId = mapping.ChunkSchemaIndex;

        int lowerValueIndex = chunkSchemaId == KeyColumnCount_ ? 0 : GetColumnValueCount(chunkSchemaId - 1);
        int upperValueIndex = GetColumnValueCount(chunkSchemaId);

        for (int valueIndex = lowerValueIndex; valueIndex < upperValueIndex; ++valueIndex) {
            row.BeginValues()[valueCount] = ReadValue(
                ValueOffset_ + valueIndex,
                valueId,
                chunkSchemaId);
            ++valueCount;
        }
    }
    row.GetHeader()->ValueCount = valueCount;

    return row;
}

TVersionedRow TSimpleVersionedBlockReader::ReadValuesByTimestamp(TChunkedMemoryPool *memoryPool)
{
    int timestampIndex = LowerBound(0, TimestampCount_, [&] (int index) {
        auto ts = ReadTimestamp(TimestampOffset_ + index);
        return (ts & TimestampValueMask) > Timestamp_;
    });

    if (timestampIndex == TimestampCount_) {
        // Row didn't exist at given timestamp.
        return TVersionedRow();
    }

    auto row = TVersionedRow::Allocate(
        memoryPool,
        KeyColumnCount_,
        SchemaIdMapping_.size(),
        1);

    ::memcpy(row.BeginKeys(), Key_.Begin(), sizeof(TUnversionedValue) * KeyColumnCount_);
    auto ts = ReadTimestamp(TimestampOffset_ + timestampIndex);
    *row.BeginTimestamps() = ts;

    if (TombstoneTimestampMask & ts) {
        row.GetHeader()->ValueCount = 0;
        return row;
    }

    if (timestampIndex == TimestampCount_ - 1) {
        row.BeginTimestamps()[0] |= IncrementalTimestampMask;
    }

    int valueCount = 0;
    for (const auto& mapping : SchemaIdMapping_) {
        int valueId = mapping.ReaderSchemaIndex;
        int chunkSchemaId = mapping.ChunkSchemaIndex;

        int valueIndex = LowerBound(
            chunkSchemaId == KeyColumnCount_ ? 0 : GetColumnValueCount(chunkSchemaId - 1),
            GetColumnValueCount(chunkSchemaId),
            [&] (int index) {
                auto value = ReadValue(ValueOffset_ + index, valueId, chunkSchemaId);
                return (value.Timestamp  > Timestamp_);
            });

        if (valueIndex < GetColumnValueCount(chunkSchemaId)) {
            auto value = ReadValue(ValueOffset_ + valueIndex, valueId, chunkSchemaId);
            if (value.Timestamp >= (ts & TimestampValueMask)) {
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

TVersionedValue TSimpleVersionedBlockReader::ReadValue(int valueIndex, int id, int chunkSchemaId)
{
    YCHECK(id >= KeyColumnCount_);
    char* valuePtr = ValueData_.Begin() + TSimpleVersionedBlockWriter::ValueSize * valueIndex;
    auto ts = *reinterpret_cast<TTimestamp*>(valuePtr + 8);

    bool isNull = ValueNullFlags_[valueIndex];
    if (isNull) {
        return MakeVersionedSentinelValue(EValueType::Null,  ts, id);
    }

    switch (Schema_.Columns()[chunkSchemaId].Type) {
        case EValueType::Integer:
            return MakeVersionedIntegerValue(*reinterpret_cast<i64*>(valuePtr), ts, id);

        case EValueType::Double:
            return MakeVersionedDoubleValue(*reinterpret_cast<double*>(valuePtr), ts, id);

        case EValueType::String:
            return MakeVersionedStringValue(ReadString(valuePtr), ts, id);

        case EValueType::Any:
            return MakeVersionedAnyValue(ReadString(valuePtr), ts, id);

        default:
            YUNREACHABLE();
    }
}

const TOwningKey& TSimpleVersionedBlockReader::GetKey() const
{
    return Key_;
}

TTimestamp TSimpleVersionedBlockReader::ReadTimestamp(int timestampIndex)
{
    return *reinterpret_cast<TTimestamp*>(
        TimestampsData_.Begin() +
        timestampIndex * TSimpleVersionedBlockWriter::TimestampSize);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NVersionedTableClient
} // namespace NYT
