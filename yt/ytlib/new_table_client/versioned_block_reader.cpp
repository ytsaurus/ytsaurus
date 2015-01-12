#include "stdafx.h"

#include "private.h"
#include "versioned_block_reader.h"
#include "versioned_block_writer.h"

#include <ytlib/transaction_client/public.h>

#include <core/misc/serialize.h>

namespace NYT {
namespace NVersionedTableClient {

using namespace NTransactionClient;
using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

TSimpleVersionedBlockReader::TSimpleVersionedBlockReader(
    const TSharedRef& block,
    const TBlockMeta& meta,
    const TTableSchema& chunkSchema,
    const TKeyColumns& keyColumns,
    const std::vector<TColumnIdMapping>& schemaIdMapping,
    TTimestamp timestamp)
    : Block_(block)
    , Timestamp_(timestamp)
    , KeyColumnCount_(keyColumns.size())
    , SchemaIdMapping_(schemaIdMapping)
    , ChunkSchema_(chunkSchema)
    , Meta_(meta)
    , VersionedMeta_(Meta_.GetExtension(TSimpleVersionedBlockMeta::block_meta_ext))
{
    YCHECK(Meta_.row_count() > 0);

    for (int index = 0; index < KeyColumnCount_; ++index) {
        KeyBuilder_.AddValue(MakeUnversionedSentinelValue(EValueType::Null, index));
    }
    Key_ = KeyBuilder_.GetRow();

    KeyData_ = TRef(const_cast<char*>(Block_.Begin()), TSimpleVersionedBlockWriter::GetPaddedKeySize(
        KeyColumnCount_,
        ChunkSchema_.Columns().size()) * Meta_.row_count());

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

    StringData_ = TRef(const_cast<char*>(ptr), const_cast<char*>(Block_.End()));

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
        ChunkSchema_.Columns().size()) * RowIndex_;

    for (int id = 0; id < KeyColumnCount_; ++id) {
        ReadKeyValue(&Key_[id], id);
    }

    TimestampOffset_ = *reinterpret_cast<i64*>(KeyDataPtr_);
    KeyDataPtr_ += sizeof(i64);

    ValueOffset_ = *reinterpret_cast<i64*>(KeyDataPtr_);
    KeyDataPtr_ += sizeof(i64);

    WriteTimestampCount_ = *reinterpret_cast<ui16*>(KeyDataPtr_);
    KeyDataPtr_ += sizeof(ui16);

    DeleteTimestampCount_ = *reinterpret_cast<ui16*>(KeyDataPtr_);
    KeyDataPtr_ += sizeof(ui16);

    return true;
}

TVersionedRow TSimpleVersionedBlockReader::GetRow(TChunkedMemoryPool* memoryPool)
{
    YCHECK(!Closed_);
    if (Timestamp_ == AsyncAllCommittedTimestamp) {
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
        auto row = TVersionedRow::Allocate(
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

    auto row = TVersionedRow::Allocate(
        memoryPool,
        KeyColumnCount_,
        SchemaIdMapping_.size(),
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
            chunkSchemaId == KeyColumnCount_ ? 0 : GetColumnValueCount(chunkSchemaId - 1),
            columnValueCount,
            [&] (int index) {
                return ReadValueTimestamp(ValueOffset_ + index, valueId) > Timestamp_;
            });

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

    auto type = ChunkSchema_.Columns()[id].Type;
    value->Type = type;

    switch (type) {
        case EValueType::Int64:
            ReadInt64(value, ptr);
            break;

        case EValueType::Uint64:
            ReadUint64(value, ptr);
            break;

        case EValueType::Double:
            ReadDouble(value, ptr);
            break;

        case EValueType::Boolean:
            ReadBoolean(value, ptr);
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

    auto type = ChunkSchema_.Columns()[chunkSchemaId].Type;
    value->Type = type;

    switch (type) {
        case EValueType::Int64:
            ReadInt64(value, ptr);
            break;

        case EValueType::Uint64:
            ReadUint64(value, ptr);
            break;

        case EValueType::Double:
            ReadDouble(value, ptr);
            break;

        case EValueType::Boolean:
            ReadBoolean(value, ptr);
            break;

        case EValueType::String:
        case EValueType::Any:
            ReadStringLike(value, ptr);
            break;

        default:
            YUNREACHABLE();
    }
}

void TSimpleVersionedBlockReader::ReadInt64(TUnversionedValue* value, char* ptr)
{
    value->Data.Int64 = *reinterpret_cast<i64*>(ptr);
}

void TSimpleVersionedBlockReader::ReadUint64(TUnversionedValue* value, char* ptr)
{
    value->Data.Uint64 = *reinterpret_cast<ui64*>(ptr);
}

void TSimpleVersionedBlockReader::ReadDouble(TUnversionedValue* value, char* ptr)
{
    value->Data.Double = *reinterpret_cast<double*>(ptr);
}

void TSimpleVersionedBlockReader::ReadBoolean(TUnversionedValue* value, char* ptr)
{
    value->Data.Boolean = (*ptr) == 1;
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
