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

int TSimpleVersionedBlockReader::FormatVersion = ETableChunkFormat::VersionedSimple;

////////////////////////////////////////////////////////////////////////////////

namespace {

template <class TPredicate>
int LowerBound(int lowerIndex, int upperIndex, const TPredicate& less)
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

} // namespace

TSimpleVersionedBlockReader::TSimpleVersionedBlockReader(
    const TSharedRef& data,
    const TBlockMeta& meta,
    const TTableSchema& chunkSchema,
    const TKeyColumns& keyColumns,
    const std::vector<TColumnIdMapping>& schemaIdMapping,
    TTimestamp timestamp)
    : Data_(data)
    , Timestamp_(timestamp)
    , KeyColumnCount_(keyColumns.size())
    , SchemaIdMapping_(schemaIdMapping)
    , Schema_(chunkSchema)
    , Meta_(meta)
{
    YCHECK(Meta_.row_count() > 0);

    for (int index = 0; index < KeyColumnCount_; ++index) {
        KeyBuilder_.AddValue(MakeUnversionedSentinelValue(EValueType::Null, index));
    }
    Key_ = KeyBuilder_.GetRow();

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

bool TSimpleVersionedBlockReader::SkipToKey(TKey key)
{
    YCHECK(!Closed_);

    if (GetKey() >= key) {
        // We are already further than pivot key.
        return true;
    }

    auto index = LowerBound(
        RowIndex_,
        Meta_.row_count(),
        [&] (int index) -> bool {
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
        ReadKeyValue(&Key_[id], id);
    }

    TimestampOffset_ = *reinterpret_cast<i64*>(KeyDataPtr_);
    KeyDataPtr_ += sizeof(i64);

    ValueOffset_ = *reinterpret_cast<i64*>(KeyDataPtr_);
    KeyDataPtr_ += sizeof(i64);

    TimestampCount_ = *reinterpret_cast<ui32*>(KeyDataPtr_);
    KeyDataPtr_ += sizeof(ui32);

    return true;
}

TVersionedRow TSimpleVersionedBlockReader::GetRow(TChunkedMemoryPool* memoryPool)
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

TVersionedRow TSimpleVersionedBlockReader::ReadAllValues(TChunkedMemoryPool* memoryPool)
{
    auto row = TVersionedRow::Allocate(
        memoryPool,
        KeyColumnCount_,
        GetColumnValueCount(Schema_.Columns().size() - 1),
        TimestampCount_);

    ::memcpy(row.BeginKeys(), Key_.Begin(), sizeof(TUnversionedValue) * KeyColumnCount_);

    auto* beginTimestamps = row.BeginTimestamps();
    for (int i = 0; i < TimestampCount_; ++i) {
        beginTimestamps[i] = ReadTimestamp(TimestampOffset_ + i);
    }

    auto* beginValues = row.BeginValues();
    auto* currentValue = beginValues;
    for (const auto& mapping : SchemaIdMapping_) {
        int valueId = mapping.ReaderSchemaIndex;
        int chunkSchemaId = mapping.ChunkSchemaIndex;

        int lowerValueIndex = chunkSchemaId == KeyColumnCount_ ? 0 : GetColumnValueCount(chunkSchemaId - 1);
        int upperValueIndex = GetColumnValueCount(chunkSchemaId);

        for (int valueIndex = lowerValueIndex; valueIndex < upperValueIndex; ++valueIndex) {
            ReadValue(
                currentValue,
                ValueOffset_ + valueIndex,
                valueId,
                chunkSchemaId);
            ++currentValue;
        }
    }
    row.GetHeader()->ValueCount = (currentValue - beginValues);

    return row;
}

TVersionedRow TSimpleVersionedBlockReader::ReadValuesByTimestamp(TChunkedMemoryPool* memoryPool)
{
    int timestampIndex = LowerBound(0, TimestampCount_, [&] (int index) {
        auto timestamp = ReadTimestamp(TimestampOffset_ + index);
        return (timestamp & TimestampValueMask) > Timestamp_;
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
    
    auto timestamp = ReadTimestamp(TimestampOffset_ + timestampIndex);
    auto timestampValue = timestamp & TimestampValueMask;
    
    auto* beginTimestamps = row.BeginTimestamps();
    beginTimestamps[0] = timestamp;

    if (TombstoneTimestampMask & timestamp) {
        row.GetHeader()->ValueCount = 0;
        return row;
    }

    if (timestampIndex == TimestampCount_ - 1) {
        beginTimestamps[0] |= IncrementalTimestampMask;
    }

    auto* beginValues = row.BeginValues();
    auto* currentValue = beginValues;
    for (const auto& mapping : SchemaIdMapping_) {
        int valueId = mapping.ReaderSchemaIndex;
        int chunkSchemaId = mapping.ChunkSchemaIndex;
        int columnValueCount = GetColumnValueCount(chunkSchemaId);

        int valueIndex = LowerBound(
            chunkSchemaId == KeyColumnCount_ ? 0 : GetColumnValueCount(chunkSchemaId - 1),
            columnValueCount,
            [&] (int index) {
                return ReadValueTimestamp(ValueOffset_ + index, valueId) > Timestamp_;
            });

        if (valueIndex < columnValueCount) {
            ReadValue(currentValue, ValueOffset_ + valueIndex, valueId, chunkSchemaId);
            if (currentValue->Timestamp >= timestampValue) {
                // Check that value didn't come from the previous incarnation of this row.
                ++currentValue;
            }
        } else {
            // No value in the current column satisfies timestamp filtering.
        }
    }
    row.GetHeader()->ValueCount = (currentValue - beginValues);

    return row;
}

void TSimpleVersionedBlockReader::ReadKeyValue(TUnversionedValue* value, int id)
{
    value->Id = id;

    char* ptr = KeyDataPtr_;
    KeyDataPtr_ += 8;

    bool isNull = KeyNullFlags_[RowIndex_ * KeyColumnCount_ + id];
    if (isNull) {
        value->Type = EValueType::Null;
        return;
    }

    auto type = Schema_.Columns()[id].Type;
    value->Type = type;

    switch (type) {
        case EValueType::Integer:
            ReadInteger(value, ptr);
            break;

        case EValueType::Double:
            ReadDouble(value, ptr);
            break;

        case EValueType::String:
        case EValueType::Any:
            ReadStringLike(value, ptr);
            break;

        default:
            YUNREACHABLE();
    }
}

void TSimpleVersionedBlockReader::ReadValue(TVersionedValue* value, int valueIndex, int id, int chunkSchemaId)
{
    YASSERT(id >= KeyColumnCount_);
    char* ptr = ValueData_.Begin() + TSimpleVersionedBlockWriter::ValueSize * valueIndex;
    auto timestamp = *reinterpret_cast<TTimestamp*>(ptr + 8);

    value->Id = id;
    value->Timestamp = timestamp;

    bool isNull = ValueNullFlags_[valueIndex];
    if (isNull) {
        value->Type = EValueType::Null;
        return;
    }

    auto type = Schema_.Columns()[chunkSchemaId].Type;
    value->Type = type;

    switch (type) {
        case EValueType::Integer:
            ReadInteger(value, ptr);
            break;

        case EValueType::Double:
            ReadDouble(value, ptr);
            break;

        case EValueType::String:
        case EValueType::Any:
            ReadStringLike(value, ptr);
            break;

        default:
            YUNREACHABLE();
    }
}

void TSimpleVersionedBlockReader::ReadInteger(TUnversionedValue* value, char* ptr)
{
    value->Data.Integer = *reinterpret_cast<i64*>(ptr);
}

void TSimpleVersionedBlockReader::ReadDouble(TUnversionedValue* value, char* ptr)
{
    value->Data.Double = *reinterpret_cast<double*>(ptr);
}

void TSimpleVersionedBlockReader::ReadStringLike(TUnversionedValue* value, char* ptr)
{
    ui32 offset = *reinterpret_cast<ui32*>(ptr);
    ptr += sizeof(ui32);

    ui32 length = *reinterpret_cast<ui32*>(ptr);

    value->Data.String = StringData_.Begin() + offset;
    value->Length = length;
}

TTimestamp TSimpleVersionedBlockReader::ReadValueTimestamp(int valueIndex, int id)
{
    YASSERT(id >= KeyColumnCount_);
    char* ptr = ValueData_.Begin() + TSimpleVersionedBlockWriter::ValueSize * valueIndex;
    return *reinterpret_cast<TTimestamp*>(ptr + 8);
}

TKey TSimpleVersionedBlockReader::GetKey() const
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
