#include "versioned_block_reader.h"
#include "private.h"
#include "versioned_block_writer.h"
#include "schemaless_block_reader.h"

#include <yt/ytlib/transaction_client/public.h>

#include <yt/core/misc/serialize.h>
#include <yt/core/misc/algorithm_helpers.h>

namespace NYT {
namespace NTableClient {

using namespace NTransactionClient;
using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

TSimpleVersionedBlockReader::TSimpleVersionedBlockReader(
    const TSharedRef& block,
    const TBlockMeta& meta,
    const TTableSchema& chunkSchema,
    int chunkKeyColumnCount,
    int keyColumnCount,
    const std::vector<TColumnIdMapping>& schemaIdMapping,
    const TKeyComparer& keyComparer,
    TTimestamp timestamp,
    bool produceAllVersions,
    bool initialize)
    : Block_(block)
    , Timestamp_(timestamp)
    , ProduceAllVersions_(produceAllVersions)
    , ChunkKeyColumnCount_(chunkKeyColumnCount)
    , KeyColumnCount_(keyColumnCount)
    , SchemaIdMapping_(schemaIdMapping)
    , ChunkSchema_(chunkSchema)
    , Meta_(meta)
    , VersionedMeta_(Meta_.GetExtension(TSimpleVersionedBlockMeta::block_meta_ext))
    , KeyComparer_(keyComparer)
{
    YCHECK(Meta_.row_count() > 0);
    YCHECK(KeyColumnCount_ >= ChunkKeyColumnCount_);

    auto keyDataSize = GetUnversionedRowByteSize(KeyColumnCount_);
    KeyBuffer_.reserve(keyDataSize);
    Key_ = TMutableKey::Create(KeyBuffer_.data(), KeyColumnCount_);

    for (int index = 0; index < KeyColumnCount_; ++index) {
        auto& value = Key_[index];
        value.Id = index;
        value.Aggregate = false;
    }

    for (int index = ChunkKeyColumnCount_; index < KeyColumnCount_; ++index) {
        auto& value = Key_[index];
        value.Id = index;
        value.Type = EValueType::Null;
        value.Aggregate = false;
    }

    KeyData_ = TRef(const_cast<char*>(Block_.Begin()), TSimpleVersionedBlockWriter::GetPaddedKeySize(
        ChunkKeyColumnCount_,
        ChunkSchema_.Columns().size()) * Meta_.row_count());

    ValueData_ = TRef(
        KeyData_.End(),
        TSimpleVersionedBlockWriter::ValueSize * VersionedMeta_.value_count());

    TimestampsData_ = TRef(ValueData_.End(),
        TSimpleVersionedBlockWriter::TimestampSize * VersionedMeta_.timestamp_count());

    const char* ptr = TimestampsData_.End();
    KeyNullFlags_.Reset(reinterpret_cast<const ui64*>(ptr), ChunkKeyColumnCount_ * Meta_.row_count());
    ptr += KeyNullFlags_.GetByteSize();

    ValueNullFlags_.Reset(reinterpret_cast<const ui64*>(ptr), VersionedMeta_.value_count());
    ptr += ValueNullFlags_.GetByteSize();

    for (const auto& column : ChunkSchema_.Columns()) {
        if (column.Aggregate()) {
            ValueAggregateFlags_ = TBitmap(reinterpret_cast<const ui64*>(ptr), VersionedMeta_.value_count());
            ptr += ValueAggregateFlags_->GetByteSize();
            break;
        }
    }

    StringData_ = TRef(const_cast<char*>(ptr), const_cast<char*>(Block_.End()));

    if (initialize) {
        JumpToRowIndex(0);
    } else {
        RowIndex_ = -1;
    }
}

bool TSimpleVersionedBlockReader::NextRow()
{
    YCHECK(!Closed_);
    return JumpToRowIndex(RowIndex_ + 1);
}

bool TSimpleVersionedBlockReader::SkipToRowIndex(i64 rowIndex)
{
    YCHECK(!Closed_);
    YCHECK(rowIndex >= RowIndex_);
    return JumpToRowIndex(rowIndex);
}

bool TSimpleVersionedBlockReader::SkipToKey(TKey key)
{
    YCHECK(!Closed_);

    if (KeyComparer_(GetKey(), key) >= 0) {
        // We are already further than pivot key.
        return true;
    }

    auto index = LowerBound(
        RowIndex_,
        Meta_.row_count(),
        [&] (int index) -> bool {
            YCHECK(JumpToRowIndex(index));
            return KeyComparer_(GetKey(), key) < 0;
        });

    return JumpToRowIndex(index);
}

bool TSimpleVersionedBlockReader::JumpToRowIndex(i64 index)
{
    YCHECK(!Closed_);

    if (index >= Meta_.row_count()) {
        Closed_ = true;
        return false;
    }

    RowIndex_ = index;
    KeyDataPtr_ = KeyData_.Begin() + TSimpleVersionedBlockWriter::GetPaddedKeySize(
        ChunkKeyColumnCount_,
        ChunkSchema_.Columns().size()) * RowIndex_;

    for (int id = 0; id < ChunkKeyColumnCount_; ++id) {
        ReadKeyValue(&Key_[id], id);
    }

    TimestampOffset_ = *reinterpret_cast<const i64*>(KeyDataPtr_);
    KeyDataPtr_ += sizeof(i64);

    ValueOffset_ = *reinterpret_cast<const i64*>(KeyDataPtr_);
    KeyDataPtr_ += sizeof(i64);

    WriteTimestampCount_ = *reinterpret_cast<const ui16*>(KeyDataPtr_);
    KeyDataPtr_ += sizeof(ui16);

    DeleteTimestampCount_ = *reinterpret_cast<const ui16*>(KeyDataPtr_);
    KeyDataPtr_ += sizeof(ui16);

    return true;
}

TVersionedRow TSimpleVersionedBlockReader::GetRow(TChunkedMemoryPool* memoryPool)
{
    YCHECK(!Closed_);
    if (ProduceAllVersions_) {
        return ReadAllVersions(memoryPool);
    } else {
        return ReadOneVersion(memoryPool);
    }
}

ui32 TSimpleVersionedBlockReader::GetColumnValueCount(int schemaColumnId) const
{
    Y_ASSERT(schemaColumnId >= ChunkKeyColumnCount_);
    return *(reinterpret_cast<const ui32*>(KeyDataPtr_) + schemaColumnId - ChunkKeyColumnCount_);
}

TVersionedRow TSimpleVersionedBlockReader::ReadAllVersions(TChunkedMemoryPool* memoryPool)
{
    int writeTimestampIndex = 0;
    int deleteTimestampIndex = 0;

    if (Timestamp_ < MaxTimestamp) {
        writeTimestampIndex = LowerBound(0, WriteTimestampCount_, [&] (int index) {
            return ReadTimestamp(TimestampOffset_ + index) > Timestamp_;
        });
        deleteTimestampIndex = LowerBound(0, DeleteTimestampCount_, [&] (int index) {
            return ReadTimestamp(TimestampOffset_ + WriteTimestampCount_ + index) > Timestamp_;
        });
    }

    // Produce output row.
    auto row = TMutableVersionedRow::Allocate(
        memoryPool,
        KeyColumnCount_,
        GetColumnValueCount(ChunkSchema_.Columns().size() - 1), // shrinkable
        WriteTimestampCount_ - writeTimestampIndex,
        DeleteTimestampCount_ - deleteTimestampIndex);

    // Write timestamps.
    auto* beginWriteTimestamps = row.BeginWriteTimestamps();
    auto* currentWriteTimestamp = beginWriteTimestamps;
    for (int i = writeTimestampIndex; i < WriteTimestampCount_; ++i) {
        *currentWriteTimestamp = ReadTimestamp(TimestampOffset_ + i);
        ++currentWriteTimestamp;
    }
    Y_ASSERT(row.GetWriteTimestampCount() == currentWriteTimestamp - beginWriteTimestamps);

    // Delete timestamps.
    auto* beginDeleteTimestamps = row.BeginDeleteTimestamps();
    auto* currentDeleteTimestamp = beginDeleteTimestamps;
    for (int i = deleteTimestampIndex; i < DeleteTimestampCount_; ++i) {
        *currentDeleteTimestamp = ReadTimestamp(TimestampOffset_ + WriteTimestampCount_ + i);
        ++currentDeleteTimestamp;
    }
    Y_ASSERT(row.GetDeleteTimestampCount() == currentDeleteTimestamp - beginDeleteTimestamps);

    // Keys.
    ::memcpy(row.BeginKeys(), Key_.Begin(), sizeof(TUnversionedValue) * KeyColumnCount_);

    // Values.
    auto* beginValues = row.BeginValues();
    auto* currentValue = beginValues;
    for (const auto& mapping : SchemaIdMapping_) {
        int valueId = mapping.ReaderSchemaIndex;
        int chunkSchemaId = mapping.ChunkSchemaIndex;

        int lowerValueIndex = chunkSchemaId == ChunkKeyColumnCount_ ? 0 : GetColumnValueCount(chunkSchemaId - 1);
        int upperValueIndex = GetColumnValueCount(chunkSchemaId);

        for (int index = lowerValueIndex; index < upperValueIndex; ++index) {
            ReadValue(
                currentValue,
                ValueOffset_ + index,
                valueId,
                chunkSchemaId);
            if (currentValue->Timestamp <= Timestamp_) {
                ++currentValue;
            }
        }
    }

    // Shrink memory.
    auto* memoryTo = const_cast<char*>(row.GetMemoryEnd());
    row.SetValueCount(currentValue - beginValues);
    auto* memoryFrom = const_cast<char*>(row.GetMemoryEnd());
    memoryPool->Free(memoryFrom, memoryTo);

    return row;
}

TVersionedRow TSimpleVersionedBlockReader::ReadOneVersion(TChunkedMemoryPool* memoryPool)
{
    int writeTimestampIndex = 0;
    int deleteTimestampIndex = 0;

    if (Timestamp_ < MaxTimestamp) {
        writeTimestampIndex = LowerBound(0, WriteTimestampCount_, [&] (int index) {
            return ReadTimestamp(TimestampOffset_ + index) > Timestamp_;
        });
        deleteTimestampIndex = LowerBound(0, DeleteTimestampCount_, [&] (int index) {
            return ReadTimestamp(TimestampOffset_ + WriteTimestampCount_ + index) > Timestamp_;
        });
    }

    bool hasWriteTimestamp = writeTimestampIndex < WriteTimestampCount_;
    bool hasDeleteTimestamp = deleteTimestampIndex < DeleteTimestampCount_;

    if (!hasWriteTimestamp & !hasDeleteTimestamp) {
        // Row didn't exist at given timestamp.
        return TVersionedRow();
    }

    TTimestamp writeTimestamp = hasWriteTimestamp
        ? ReadTimestamp(TimestampOffset_ + writeTimestampIndex)
        : NullTimestamp;
    TTimestamp deleteTimestamp = hasDeleteTimestamp
        ? ReadTimestamp(TimestampOffset_ + WriteTimestampCount_ + deleteTimestampIndex)
        : NullTimestamp;

    if (deleteTimestamp > writeTimestamp) {
        // Row has been deleted at given timestamp.
        auto row = TMutableVersionedRow::Allocate(
            memoryPool,
            KeyColumnCount_,
            0, // no values
            0, // no write timestamps
            1);
        row.BeginDeleteTimestamps()[0] = deleteTimestamp;
        ::memcpy(row.BeginKeys(), Key_.Begin(), sizeof(TUnversionedValue) * KeyColumnCount_);
        return row;
    }

    YCHECK(hasWriteTimestamp);

    // Produce output row.
    auto row = TMutableVersionedRow::Allocate(
        memoryPool,
        KeyColumnCount_,
        GetColumnValueCount(ChunkSchema_.Columns().size() - 1), // shrinkable
        1,
        hasDeleteTimestamp ? 1 : 0);

    // Write, delete timestamps.
    row.BeginWriteTimestamps()[0] = writeTimestamp;
    if (hasDeleteTimestamp) {
        row.BeginDeleteTimestamps()[0] = deleteTimestamp;
    }

    // Keys.
    ::memcpy(row.BeginKeys(), Key_.Begin(), sizeof(TUnversionedValue) * KeyColumnCount_);

    auto isValueAlive = [&] (int index) -> bool {
        return ReadValueTimestamp(ValueOffset_ + index) > deleteTimestamp;
    };

    auto* beginValues = row.BeginValues();
    auto* currentValue = beginValues;
    for (const auto& mapping : SchemaIdMapping_) {
        int valueId = mapping.ReaderSchemaIndex;
        int chunkSchemaId = mapping.ChunkSchemaIndex;

        int lowerValueIndex = chunkSchemaId == ChunkKeyColumnCount_ ? 0 : GetColumnValueCount(chunkSchemaId - 1);
        int upperValueIndex = GetColumnValueCount(chunkSchemaId);

        int valueBeginIndex = LowerBound(
            lowerValueIndex,
            upperValueIndex,
            [&] (int index) {
                return ReadValueTimestamp(ValueOffset_ + index) > Timestamp_;
            });
        int valueEndIndex = valueBeginIndex;

        if (ChunkSchema_.Columns()[chunkSchemaId].Aggregate()) {
            valueEndIndex = LowerBound(lowerValueIndex, upperValueIndex, isValueAlive);
        } else if (valueBeginIndex < upperValueIndex && isValueAlive(valueBeginIndex)) {
            valueEndIndex = valueBeginIndex + 1;
        }

        for (int index = valueBeginIndex; index < valueEndIndex; ++index) {
            ReadValue(currentValue, ValueOffset_ + index, valueId, chunkSchemaId);
            if (currentValue->Timestamp <= Timestamp_) {
                ++currentValue;
            }
        }
    }

    // Shrink memory.
    auto* memoryTo = const_cast<char*>(row.GetMemoryEnd());
    row.SetValueCount(currentValue - beginValues);
    auto* memoryFrom = const_cast<char*>(row.GetMemoryEnd());
    memoryPool->Free(memoryFrom, memoryTo);

    return row;
}

void TSimpleVersionedBlockReader::ReadKeyValue(TUnversionedValue* value, int id)
{
    const char* ptr = KeyDataPtr_;
    KeyDataPtr_ += 8;

    bool isNull = KeyNullFlags_[RowIndex_ * ChunkKeyColumnCount_ + id];
    if (Y_UNLIKELY(isNull)) {
        value->Type = EValueType::Null;
        return;
    }

    auto type = ChunkSchema_.Columns()[id].GetPhysicalType();
    value->Type = type;

    switch (type) {
        case EValueType::Int64:
        case EValueType::Uint64:
        case EValueType::Double:
        case EValueType::Boolean:
            value->Data.Int64 = *reinterpret_cast<const i64*>(ptr);
            break;

        case EValueType::String:
        case EValueType::Any:
            ReadStringLike(value, ptr);
            break;

        default:
            Y_UNREACHABLE();
    }
}

void TSimpleVersionedBlockReader::ReadValue(TVersionedValue* value, int valueIndex, int id, int chunkSchemaId)
{
    Y_ASSERT(id >= ChunkKeyColumnCount_);
    const char* ptr = ValueData_.Begin() + TSimpleVersionedBlockWriter::ValueSize * valueIndex;
    auto timestamp = *reinterpret_cast<const TTimestamp*>(ptr + 8);

    value->Id = id;
    value->Timestamp = timestamp;
    value->Aggregate = ValueAggregateFlags_ ? ValueAggregateFlags_->operator[](valueIndex) : false;

    bool isNull = ValueNullFlags_[valueIndex];
    if (Y_UNLIKELY(isNull)) {
        value->Type = EValueType::Null;
        return;
    }

    auto type = ChunkSchema_.Columns()[chunkSchemaId].GetPhysicalType();
    value->Type = type;

    switch (type) {
        case EValueType::Int64:
        case EValueType::Uint64:
        case EValueType::Double:
        case EValueType::Boolean:
            value->Data.Int64 = *reinterpret_cast<const i64*>(ptr);
            break;

        case EValueType::String:
        case EValueType::Any:
            ReadStringLike(value, ptr);
            break;

        default:
            Y_UNREACHABLE();
    }
}

void TSimpleVersionedBlockReader::ReadStringLike(TUnversionedValue* value, const char* ptr)
{
    ui32 offset = *reinterpret_cast<const ui32*>(ptr);
    ptr += sizeof(ui32);

    ui32 length = *reinterpret_cast<const ui32*>(ptr);

    value->Data.String = StringData_.Begin() + offset;
    value->Length = length;
}

TTimestamp TSimpleVersionedBlockReader::ReadValueTimestamp(int valueIndex)
{
    const char* ptr = ValueData_.Begin() + TSimpleVersionedBlockWriter::ValueSize * valueIndex;
    return *reinterpret_cast<const TTimestamp*>(ptr + 8);
}

TKey TSimpleVersionedBlockReader::GetKey() const
{
    return Key_;
}

TTimestamp TSimpleVersionedBlockReader::ReadTimestamp(int timestampIndex)
{
    return *reinterpret_cast<const TTimestamp*>(
        TimestampsData_.Begin() +
        timestampIndex * TSimpleVersionedBlockWriter::TimestampSize);
}

i64 TSimpleVersionedBlockReader::GetRowIndex() const
{
    return RowIndex_;
}

////////////////////////////////////////////////////////////////////////////////

THorizontalSchemalessVersionedBlockReader::THorizontalSchemalessVersionedBlockReader(
    const TSharedRef& block,
    const NProto::TBlockMeta& meta,
    const std::vector<TColumnIdMapping>& idMapping,
    int chunkKeyColumnCount,
    int keyColumnCount,
    TTimestamp timestamp)
    : UnderlyingReader_(std::make_unique<THorizontalSchemalessBlockReader>(
        block,
        meta,
        idMapping,
        chunkKeyColumnCount,
        keyColumnCount))
    , Timestamp_(timestamp)
{ }

bool THorizontalSchemalessVersionedBlockReader::NextRow()
{
    return UnderlyingReader_->NextRow();
}

bool THorizontalSchemalessVersionedBlockReader::SkipToRowIndex(i64 rowIndex)
{
    return UnderlyingReader_->JumpToRowIndex(rowIndex);
}

bool THorizontalSchemalessVersionedBlockReader::SkipToKey(TKey key)
{
    return UnderlyingReader_->SkipToKey(key);
}

TKey THorizontalSchemalessVersionedBlockReader::GetKey() const
{
    return UnderlyingReader_->GetKey();
}

TVersionedRow THorizontalSchemalessVersionedBlockReader::GetRow(TChunkedMemoryPool* memoryPool)
{
    return UnderlyingReader_->GetVersionedRow(memoryPool, Timestamp_);
}

i64 THorizontalSchemalessVersionedBlockReader::GetRowIndex() const
{
    return UnderlyingReader_->GetRowIndex();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NTableClient
} // namespace NYT
