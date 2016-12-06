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
    bool initialize)
    : Block_(block)
    , Timestamp_(timestamp)
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
    }

    for (int index = ChunkKeyColumnCount_; index < KeyColumnCount_; ++index) {
        auto& value = Key_[index];
        value.Id = index;
        value.Type = EValueType::Null;
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
        if (column.Aggregate) {
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
    if (Timestamp_ == AllCommittedTimestamp) {
        return ReadAllValues(memoryPool);
    } else {
        return ReadValuesByTimestamp(memoryPool);
    }
}

ui32 TSimpleVersionedBlockReader::GetColumnValueCount(int schemaColumnId) const
{
    Y_ASSERT(schemaColumnId >= ChunkKeyColumnCount_);
    return *(reinterpret_cast<const ui32*>(KeyDataPtr_) + schemaColumnId - ChunkKeyColumnCount_);
}

TVersionedRow TSimpleVersionedBlockReader::ReadAllValues(TChunkedMemoryPool* memoryPool)
{
    auto row = TMutableVersionedRow::Allocate(
        memoryPool,
        KeyColumnCount_,
        GetColumnValueCount(ChunkSchema_.Columns().size() - 1),
        WriteTimestampCount_,
        DeleteTimestampCount_);

    ::memcpy(row.BeginKeys(), Key_.Begin(), sizeof(TUnversionedValue) * KeyColumnCount_);

    auto* beginWriteTimestamps = row.BeginWriteTimestamps();
    for (int i = 0; i < WriteTimestampCount_; ++i) {
        beginWriteTimestamps[i] = ReadTimestamp(TimestampOffset_ + i);
    }

    auto* beginDeleteTimestamps = row.BeginDeleteTimestamps();
    for (int i = 0; i < DeleteTimestampCount_; ++i) {
        beginDeleteTimestamps[i] = ReadTimestamp(TimestampOffset_ + WriteTimestampCount_ + i);
    }

    auto* beginValues = row.BeginValues();
    auto* currentValue = beginValues;
    for (const auto& mapping : SchemaIdMapping_) {
        int valueId = mapping.ReaderSchemaIndex;
        int chunkSchemaId = mapping.ChunkSchemaIndex;

        int lowerValueIndex = chunkSchemaId == ChunkKeyColumnCount_ ? 0 : GetColumnValueCount(chunkSchemaId - 1);
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
    int writeTimestampIndex = LowerBound(0, WriteTimestampCount_, [&] (int index) {
        return ReadTimestamp(TimestampOffset_ + index) > Timestamp_;
    });

    int deleteTimestampIndex = LowerBound(0, DeleteTimestampCount_, [&] (int index) {
        return ReadTimestamp(TimestampOffset_ + WriteTimestampCount_ + index) > Timestamp_;
    });

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
        ::memcpy(row.BeginKeys(), Key_.Begin(), sizeof(TUnversionedValue) * KeyColumnCount_);
        row.BeginDeleteTimestamps()[0] = deleteTimestamp;
        return row;
    }

    YCHECK(hasWriteTimestamp);

    int aggregateCountDelta = 0;

    for (const auto& mapping : SchemaIdMapping_) {
        int valueId = mapping.ReaderSchemaIndex;
        int chunkSchemaId = mapping.ChunkSchemaIndex;
        int columnValueCount = GetColumnValueCount(chunkSchemaId);

        if (ChunkSchema_.Columns()[chunkSchemaId].Aggregate) {
            int valueBeginIndex = LowerBound(
                chunkSchemaId == ChunkKeyColumnCount_ ? 0 : GetColumnValueCount(chunkSchemaId - 1),
                columnValueCount,
                [&] (int index) {
                    return ReadValueTimestamp(ValueOffset_ + index, valueId) > Timestamp_;
                });
            int valueEndIndex = LowerBound(
                chunkSchemaId == ChunkKeyColumnCount_ ? 0 : GetColumnValueCount(chunkSchemaId - 1),
                columnValueCount,
                [&] (int index) {
                    return ReadValueTimestamp(ValueOffset_ + index, valueId) > deleteTimestamp;
                });

            int valueCount = valueEndIndex - valueBeginIndex;

            if (valueCount > 0) {
                aggregateCountDelta += valueCount - 1;
            }
        }
    }

    int reservedValueCount = SchemaIdMapping_.size() + aggregateCountDelta;

    auto row = TMutableVersionedRow::Allocate(
        memoryPool,
        KeyColumnCount_,
        SchemaIdMapping_.size() + aggregateCountDelta,
        1,
        hasDeleteTimestamp ? 1 : 0);

    ::memcpy(row.BeginKeys(), Key_.Begin(), sizeof(TUnversionedValue) * KeyColumnCount_);

    row.BeginWriteTimestamps()[0] = writeTimestamp;
    if (hasDeleteTimestamp) {
        row.BeginDeleteTimestamps()[0] = deleteTimestamp;
    }

    auto* beginValues = row.BeginValues();
    auto* currentValue = beginValues;
    for (const auto& mapping : SchemaIdMapping_) {
        int valueId = mapping.ReaderSchemaIndex;
        int chunkSchemaId = mapping.ChunkSchemaIndex;
        int columnValueCount = GetColumnValueCount(chunkSchemaId);

        int valueIndex = LowerBound(
            chunkSchemaId == ChunkKeyColumnCount_ ? 0 : GetColumnValueCount(chunkSchemaId - 1),
            columnValueCount,
            [&] (int index) {
                return ReadValueTimestamp(ValueOffset_ + index, valueId) > Timestamp_;
            });

        if (ChunkSchema_.Columns()[chunkSchemaId].Aggregate) {
            int valueEndIndex = LowerBound(
                chunkSchemaId == ChunkKeyColumnCount_ ? 0 : GetColumnValueCount(chunkSchemaId - 1),
                columnValueCount,
                [&] (int index) {
                    return ReadValueTimestamp(ValueOffset_ + index, valueId) > deleteTimestamp;
                });
            
            for (int index = valueIndex; index < valueEndIndex; ++index) {
                ReadValue(currentValue, ValueOffset_ + index, valueId, chunkSchemaId);
                ++currentValue;
            }
        } else {
            if (valueIndex < columnValueCount) {
                ReadValue(currentValue, ValueOffset_ + valueIndex, valueId, chunkSchemaId);
                if (currentValue->Timestamp > deleteTimestamp) {
                    // Check that value didn't come from the previous incarnation of this row.
                    ++currentValue;
                }
            } else {
                // No value in the current column satisfies timestamp filtering.
            }
        }
    }

    YCHECK(currentValue - beginValues <= reservedValueCount);
    row.SetValueCount(currentValue - beginValues);

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

    auto type = ChunkSchema_.Columns()[id].Type;
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
    value->Aggregate = ValueAggregateFlags_.HasValue() ? ValueAggregateFlags_->operator[](valueIndex) : false;

    bool isNull = ValueNullFlags_[valueIndex];
    if (Y_UNLIKELY(isNull)) {
        value->Type = EValueType::Null;
        return;
    }

    auto type = ChunkSchema_.Columns()[chunkSchemaId].Type;
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

TTimestamp TSimpleVersionedBlockReader::ReadValueTimestamp(int valueIndex, int id)
{
    Y_ASSERT(id >= ChunkKeyColumnCount_);
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
