#include "versioned_block_reader.h"
#include "private.h"
#include "versioned_block_writer.h"
#include "schemaless_block_reader.h"

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/misc/serialize.h>
#include <yt/yt/core/misc/algorithm_helpers.h>

namespace NYT::NTableClient {

using namespace NTransactionClient;
using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

TSimpleVersionedBlockReader::TSimpleVersionedBlockReader(
    TSharedRef block,
    const TBlockMeta& meta,
    TTableSchemaPtr chunkSchema,
    int chunkKeyColumnCount,
    int keyColumnCount,
    const std::vector<TColumnIdMapping>& schemaIdMapping,
    const TKeyComparer& keyComparer,
    TTimestamp timestamp,
    bool produceAllVersions,
    bool initialize)
    : Block_(std::move(block))
    , Timestamp_(timestamp)
    , ProduceAllVersions_(produceAllVersions)
    , ChunkKeyColumnCount_(chunkKeyColumnCount)
    , KeyColumnCount_(keyColumnCount)
    , SchemaIdMapping_(schemaIdMapping)
    , ChunkSchema_(std::move(chunkSchema))
    , Meta_(meta)
    , VersionedMeta_(Meta_.GetExtension(TSimpleVersionedBlockMeta::block_meta_ext))
    , ColumnHunkFlags_(new bool[ChunkSchema_->GetColumnCount()])
    , ColumnTypes_(new EValueType[ChunkSchema_->GetColumnCount()])
    , KeyComparer_(keyComparer)
{
    YT_VERIFY(Meta_.row_count() > 0);
    YT_VERIFY(KeyColumnCount_ >= ChunkKeyColumnCount_);

    for (int id = 0; id < ChunkSchema_->GetColumnCount(); ++id) {
        const auto& columnSchema = ChunkSchema_->Columns()[id];
        ColumnHunkFlags_[id] = columnSchema.MaxInlineHunkSize().operator bool();
        ColumnTypes_[id] = columnSchema.GetPhysicalType();
    }

    auto keyDataSize = GetUnversionedRowByteSize(KeyColumnCount_);
    KeyBuffer_.reserve(keyDataSize);
    Key_ = TLegacyMutableKey::Create(KeyBuffer_.data(), KeyColumnCount_);

    for (int index = 0; index < KeyColumnCount_; ++index) {
        Key_[index] = MakeUnversionedNullValue(index);
    }

    KeyData_ = TRef(const_cast<char*>(Block_.Begin()), TSimpleVersionedBlockWriter::GetPaddedKeySize(
        ChunkKeyColumnCount_,
        ChunkSchema_->GetColumnCount()) * Meta_.row_count());

    ValueData_ = TRef(
        KeyData_.End(),
        TSimpleVersionedBlockWriter::ValueSize * VersionedMeta_.value_count());

    TimestampsData_ = TRef(ValueData_.End(),
        TSimpleVersionedBlockWriter::TimestampSize * VersionedMeta_.timestamp_count());

    const char* ptr = TimestampsData_.End();
    KeyNullFlags_.Reset(ptr, ChunkKeyColumnCount_ * Meta_.row_count());
    ptr += AlignUp(KeyNullFlags_.GetByteSize(), SerializationAlignment);

    ValueNullFlags_.Reset(ptr, VersionedMeta_.value_count());
    ptr += AlignUp(ValueNullFlags_.GetByteSize(), SerializationAlignment);

    for (const auto& columnSchema : ChunkSchema_->Columns()) {
        if (columnSchema.Aggregate()) {
            ValueAggregateFlags_ = TReadOnlyBitmap(ptr, VersionedMeta_.value_count());
            ptr += AlignUp(ValueAggregateFlags_->GetByteSize(), SerializationAlignment);
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
    YT_VERIFY(!Closed_);
    return JumpToRowIndex(RowIndex_ + 1);
}

bool TSimpleVersionedBlockReader::SkipToRowIndex(i64 rowIndex)
{
    YT_VERIFY(!Closed_);
    YT_VERIFY(rowIndex >= RowIndex_);
    return JumpToRowIndex(rowIndex);
}

bool TSimpleVersionedBlockReader::SkipToKey(TLegacyKey key)
{
    YT_VERIFY(!Closed_);

    if (KeyComparer_(GetKey(), key) >= 0) {
        // We are already further than pivot key.
        return true;
    }

    auto index = BinarySearch(
        RowIndex_,
        Meta_.row_count(),
        [&] (int index) -> bool {
            YT_VERIFY(JumpToRowIndex(index));
            return KeyComparer_(GetKey(), key) < 0;
        });

    return JumpToRowIndex(index);
}

bool TSimpleVersionedBlockReader::JumpToRowIndex(i64 index)
{
    YT_VERIFY(!Closed_);

    if (index >= Meta_.row_count()) {
        Closed_ = true;
        return false;
    }

    RowIndex_ = index;
    KeyDataPtr_ = KeyData_.Begin() + TSimpleVersionedBlockWriter::GetPaddedKeySize(
        ChunkKeyColumnCount_,
        ChunkSchema_->GetColumnCount()) * RowIndex_;

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

TMutableVersionedRow TSimpleVersionedBlockReader::GetRow(TChunkedMemoryPool* memoryPool)
{
    YT_VERIFY(!Closed_);
    return ProduceAllVersions_
        ? ReadAllVersions(memoryPool)
        : ReadOneVersion(memoryPool);
}

ui32 TSimpleVersionedBlockReader::GetColumnValueCount(int schemaColumnId) const
{
    YT_ASSERT(schemaColumnId >= ChunkKeyColumnCount_);
    return *(reinterpret_cast<const ui32*>(KeyDataPtr_) + schemaColumnId - ChunkKeyColumnCount_);
}

TMutableVersionedRow TSimpleVersionedBlockReader::ReadAllVersions(TChunkedMemoryPool* memoryPool)
{
    int writeTimestampIndex = 0;
    int deleteTimestampIndex = 0;

    if (Timestamp_ < MaxTimestamp) {
        writeTimestampIndex = BinarySearch(0, WriteTimestampCount_, [&] (int index) {
            return ReadTimestamp(TimestampOffset_ + index) > Timestamp_;
        });
        deleteTimestampIndex = BinarySearch(0, DeleteTimestampCount_, [&] (int index) {
            return ReadTimestamp(TimestampOffset_ + WriteTimestampCount_ + index) > Timestamp_;
        });

        if (writeTimestampIndex == WriteTimestampCount_ && deleteTimestampIndex == DeleteTimestampCount_) {
            return {};
        }
    }

    // Produce output row.
    auto row = TMutableVersionedRow::Allocate(
        memoryPool,
        KeyColumnCount_,
        GetColumnValueCount(ChunkSchema_->GetColumnCount() - 1), // shrinkable
        WriteTimestampCount_ - writeTimestampIndex,
        DeleteTimestampCount_ - deleteTimestampIndex);

    // Write timestamps.
    auto* beginWriteTimestamps = row.BeginWriteTimestamps();
    auto* currentWriteTimestamp = beginWriteTimestamps;
    for (int i = writeTimestampIndex; i < WriteTimestampCount_; ++i) {
        *currentWriteTimestamp = ReadTimestamp(TimestampOffset_ + i);
        ++currentWriteTimestamp;
    }
    YT_ASSERT(row.GetWriteTimestampCount() == currentWriteTimestamp - beginWriteTimestamps);

    // Delete timestamps.
    auto* beginDeleteTimestamps = row.BeginDeleteTimestamps();
    auto* currentDeleteTimestamp = beginDeleteTimestamps;
    for (int i = deleteTimestampIndex; i < DeleteTimestampCount_; ++i) {
        *currentDeleteTimestamp = ReadTimestamp(TimestampOffset_ + WriteTimestampCount_ + i);
        ++currentDeleteTimestamp;
    }
    YT_ASSERT(row.GetDeleteTimestampCount() == currentDeleteTimestamp - beginDeleteTimestamps);

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

TMutableVersionedRow TSimpleVersionedBlockReader::ReadOneVersion(TChunkedMemoryPool* memoryPool)
{
    int writeTimestampIndex = 0;
    int deleteTimestampIndex = 0;

    if (Timestamp_ < MaxTimestamp) {
        writeTimestampIndex = BinarySearch(0, WriteTimestampCount_, [&] (int index) {
            return ReadTimestamp(TimestampOffset_ + index) > Timestamp_;
        });
        deleteTimestampIndex = BinarySearch(0, DeleteTimestampCount_, [&] (int index) {
            return ReadTimestamp(TimestampOffset_ + WriteTimestampCount_ + index) > Timestamp_;
        });
    }

    bool hasWriteTimestamp = writeTimestampIndex < WriteTimestampCount_;
    bool hasDeleteTimestamp = deleteTimestampIndex < DeleteTimestampCount_;

    if (!hasWriteTimestamp & !hasDeleteTimestamp) {
        // Row didn't exist at given timestamp.
        return TMutableVersionedRow();
    }

    auto writeTimestamp = hasWriteTimestamp
        ? ReadTimestamp(TimestampOffset_ + writeTimestampIndex)
        : NullTimestamp;
    auto deleteTimestamp = hasDeleteTimestamp
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

    YT_VERIFY(hasWriteTimestamp);

    // Produce output row.
    auto row = TMutableVersionedRow::Allocate(
        memoryPool,
        KeyColumnCount_,
        GetColumnValueCount(ChunkSchema_->GetColumnCount() - 1), // shrinkable
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

        int valueBeginIndex = BinarySearch(
            lowerValueIndex,
            upperValueIndex,
            [&] (int index) {
                return ReadValueTimestamp(ValueOffset_ + index) > Timestamp_;
            });
        int valueEndIndex = valueBeginIndex;

        if (ChunkSchema_->Columns()[chunkSchemaId].Aggregate()) {
            valueEndIndex = BinarySearch(lowerValueIndex, upperValueIndex, isValueAlive);
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

    auto type = ColumnTypes_[id];
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

        case EValueType::Null:
        case EValueType::Composite:
        case EValueType::Min:
        case EValueType::Max:
        case EValueType::TheBottom:
            YT_ABORT();
    }
}

void TSimpleVersionedBlockReader::ReadValue(TVersionedValue* value, int valueIndex, int id, int chunkSchemaId)
{
    YT_ASSERT(id >= ChunkKeyColumnCount_);

    const char* ptr = ValueData_.Begin() + TSimpleVersionedBlockWriter::ValueSize * valueIndex;
    auto timestamp = *reinterpret_cast<const TTimestamp*>(ptr + 8);

    *value = {};
    value->Id = id;
    value->Timestamp = timestamp;

    if (ValueAggregateFlags_ && (*ValueAggregateFlags_)[valueIndex]) {
        value->Flags |= EValueFlags::Aggregate;
    }

    if (ColumnHunkFlags_[chunkSchemaId]) {
        value->Flags |= EValueFlags::Hunk;
    }

    if (Y_UNLIKELY(ValueNullFlags_[valueIndex])) {
        value->Type = EValueType::Null;
        return;
    }

    auto type = ColumnTypes_[chunkSchemaId];
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

        case EValueType::Null:
        case EValueType::Composite:
        case EValueType::Min:
        case EValueType::Max:
        case EValueType::TheBottom:
            YT_ABORT();
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

TLegacyKey TSimpleVersionedBlockReader::GetKey() const
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
    const TTableSchemaPtr& schema,
    const std::vector<TColumnIdMapping>& idMapping,
    int chunkKeyColumnCount,
    int keyColumnCount,
    TTimestamp timestamp)
    : UnderlyingReader_(std::make_unique<THorizontalBlockReader>(
        block,
        meta,
        schema,
        idMapping,
        TComparator(std::vector<ESortOrder>(chunkKeyColumnCount, ESortOrder::Ascending)),
        TComparator(std::vector<ESortOrder>(keyColumnCount, ESortOrder::Ascending))))
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

bool THorizontalSchemalessVersionedBlockReader::SkipToKey(TLegacyKey key)
{
    return UnderlyingReader_->SkipToKey(key);
}

TLegacyKey THorizontalSchemalessVersionedBlockReader::GetKey() const
{
    return UnderlyingReader_->GetLegacyKey();
}

TMutableVersionedRow THorizontalSchemalessVersionedBlockReader::GetRow(TChunkedMemoryPool* memoryPool)
{
    return UnderlyingReader_->GetVersionedRow(memoryPool, Timestamp_);
}

i64 THorizontalSchemalessVersionedBlockReader::GetRowIndex() const
{
    return UnderlyingReader_->GetRowIndex();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
