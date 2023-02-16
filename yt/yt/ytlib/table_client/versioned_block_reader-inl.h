#ifndef VERSIONED_BLOCK_READER_INL_H_
#error "Direct inclusion of this file is not allowed, include versioned_block_reader.h"
// For the sake of sane code completion.
#include "versioned_block_reader.h"
#endif

#include <yt/yt/ytlib/table_chunk_format/reader_helpers.h>

#include <yt/yt/library/numeric/algorithm_helpers.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

template <typename TRowParser>
template <class... TParserArgs>
TVersionedRowReader<TRowParser>::TVersionedRowReader(
    int keyColumnCount,
    const std::vector<TColumnIdMapping>& schemaIdMapping,
    TTimestamp timestamp,
    bool produceAllVersions,
    TParserArgs&&... parserArgs)
    : Parser_(std::forward<TParserArgs>(parserArgs)...)
    , Timestamp_(timestamp)
    , ProduceAllVersions_(produceAllVersions)
    , KeyColumnCount_(keyColumnCount)
    , SchemaIdMapping_(schemaIdMapping)
{
    RowMetadata_.KeyBuffer.reserve(GetUnversionedRowByteSize(KeyColumnCount_));
    RowMetadata_.Key = TLegacyMutableKey::Create(RowMetadata_.KeyBuffer.data(), KeyColumnCount_);

    for (int index = 0; index < KeyColumnCount_; ++index) {
        RowMetadata_.Key[index] = MakeUnversionedNullValue(index);
    }
}

template <typename TRowParser>
TLegacyKey TVersionedRowReader<TRowParser>::GetKey() const
{
    YT_VERIFY(Parser_.IsValid());
    return RowMetadata_.Key;
}

template <typename TRowParser>
TMutableVersionedRow TVersionedRowReader<TRowParser>::ProcessAndGetRow(
    const TCompactVector<TSharedRef, IndexedRowTypicalGroupCount>& /*owningRowData*/,
    const int* /*groupOffsets*/,
    const int* /*groupIndexes*/,
    TChunkedMemoryPool* /*memoryPool*/)
{
    YT_UNIMPLEMENTED();
}

template <>
TMutableVersionedRow TVersionedRowReader<TIndexedVersionedRowParser>::ProcessAndGetRow(
    const TCompactVector<TSharedRef, IndexedRowTypicalGroupCount>& owningRowData,
    const int* groupOffsets,
    const int* groupIndexes,
    TChunkedMemoryPool* memoryPool);

template <typename TRowParser>
TMutableVersionedRow TVersionedRowReader<TRowParser>::GetRow(TChunkedMemoryPool* memoryPool)
{
    return ProduceAllVersions_
        ? ReadRowAllVersions(memoryPool)
        : ReadRowSingleVersion(memoryPool);
}

template <typename TRowParser>
TMutableVersionedRow TVersionedRowReader<TRowParser>::ReadRowAllVersions(TChunkedMemoryPool* memoryPool)
{
    const auto& writeTimestamps = RowMetadata_.WriteTimestamps;
    const auto& deleteTimestamps = RowMetadata_.DeleteTimestamps;

    int writeTimestampIndex = 0;
    int deleteTimestampIndex = 0;
    if (Timestamp_ <= MaxTimestamp) {
        writeTimestampIndex = BinarySearch(0, writeTimestamps.Size(), [&] (int index) {
            return writeTimestamps[index] > Timestamp_;
        });
        deleteTimestampIndex = BinarySearch(0, deleteTimestamps.Size(), [&] (int index) {
            return deleteTimestamps[index] > Timestamp_;
        });

        if (writeTimestampIndex == std::ssize(writeTimestamps) &&
            deleteTimestampIndex == std::ssize(deleteTimestamps))
        {
            return {};
        }
    }

    // Produce output row.
    auto row = TMutableVersionedRow::Allocate(
        memoryPool,
        KeyColumnCount_,
        RowMetadata_.ValueCount, // shrinkable
        std::ssize(writeTimestamps) - writeTimestampIndex,
        std::ssize(deleteTimestamps) - deleteTimestampIndex);

    // Write timestamps.
    std::copy(writeTimestamps.Begin() + writeTimestampIndex, writeTimestamps.End(), row.BeginWriteTimestamps());

    // Delete timestamps.
    std::copy(deleteTimestamps.Begin() + deleteTimestampIndex, deleteTimestamps.End(), row.BeginDeleteTimestamps());

    // Keys.
    std::copy(RowMetadata_.Key.Begin(), RowMetadata_.Key.End(), row.BeginKeys());

    // Values.
    auto* currentValue = row.BeginValues();
    for (const auto& columnIdMapping : SchemaIdMapping_) {
        auto columnDescriptor = Parser_.GetColumnDescriptor(columnIdMapping);

        for (int index = columnDescriptor.LowerValueIndex; index < columnDescriptor.UpperValueIndex; ++index) {
            Parser_.ReadValue(currentValue, columnDescriptor, index);
            if (currentValue->Timestamp <= Timestamp_) {
                ++currentValue;
            }
        }
    }

    NTableChunkFormat::TrimRow(row, currentValue, memoryPool);

    return row;
}

template <typename TRowParser>
TMutableVersionedRow TVersionedRowReader<TRowParser>::ReadRowSingleVersion(TChunkedMemoryPool* memoryPool)
{
    int writeTimestampIndex = 0;
    int deleteTimestampIndex = 0;

    const auto& writeTimestamps = RowMetadata_.WriteTimestamps;
    const auto& deleteTimestamps = RowMetadata_.DeleteTimestamps;

    if (Timestamp_ <= MaxTimestamp) {
        writeTimestampIndex = BinarySearch(0, writeTimestamps.Size(), [&] (int index) {
            return writeTimestamps[index] > Timestamp_;
        });
        deleteTimestampIndex = BinarySearch(0, deleteTimestamps.Size(), [&] (int index) {
            return deleteTimestamps[index] > Timestamp_;
        });
    }

    bool hasWriteTimestamp = writeTimestampIndex < std::ssize(writeTimestamps);
    bool hasDeleteTimestamp = deleteTimestampIndex < std::ssize(deleteTimestamps);

    if (!hasWriteTimestamp && !hasDeleteTimestamp) {
        // Row didn't exist at given timestamp.
        return {};
    }

    auto writeTimestamp = hasWriteTimestamp
        ? writeTimestamps[writeTimestampIndex]
        : NullTimestamp;
    auto deleteTimestamp = hasDeleteTimestamp
        ? deleteTimestamps[deleteTimestampIndex]
        : NullTimestamp;

    if (deleteTimestamp > writeTimestamp) {
        // Row has been deleted at given timestamp.
        auto row = TMutableVersionedRow::Allocate(
            memoryPool,
            KeyColumnCount_,
            /*valueCount*/ 0,
            /*writeTimestampCount*/ 0,
            /*deleteTimestampCount*/ 1);
        row.BeginDeleteTimestamps()[0] = deleteTimestamp;
        std::copy(RowMetadata_.Key.Begin(), RowMetadata_.Key.End(), row.BeginKeys());
        return row;
    }

    YT_VERIFY(hasWriteTimestamp);

    // Produce output row.
    auto row = TMutableVersionedRow::Allocate(
        memoryPool,
        KeyColumnCount_,
        RowMetadata_.ValueCount, // shrinkable
        /*writeTimestampCount*/ 1,
        /*deleteTimestampCount*/ hasDeleteTimestamp ? 1 : 0);

    // Write, delete timestamps.
    row.BeginWriteTimestamps()[0] = writeTimestamp;
    if (hasDeleteTimestamp) {
        row.BeginDeleteTimestamps()[0] = deleteTimestamp;
    }

    // Keys.
    std::copy(RowMetadata_.Key.Begin(), RowMetadata_.Key.End(), row.BeginKeys());

    auto* currentValue = row.BeginValues();
    for (const auto& columnIdMapping : SchemaIdMapping_) {
        auto columnDescriptor = Parser_.GetColumnDescriptor(columnIdMapping);

        auto isValueAlive = [&] (int index) {
            return Parser_.ReadValueTimestamp(columnDescriptor, index) > deleteTimestamp;
        };

        int valueBeginIndex = BinarySearch(
            columnDescriptor.LowerValueIndex,
            columnDescriptor.UpperValueIndex,
            [&] (int index) {
                return Parser_.ReadValueTimestamp(columnDescriptor, index) > Timestamp_;
            });

        int valueEndIndex = valueBeginIndex;
        if (columnDescriptor.Aggregate) {
            valueEndIndex = BinarySearch(
                columnDescriptor.LowerValueIndex,
                columnDescriptor.UpperValueIndex,
                isValueAlive);
        } else if (valueBeginIndex < columnDescriptor.UpperValueIndex && isValueAlive(valueBeginIndex)) {
            valueEndIndex = valueBeginIndex + 1;
        }

        for (int index = valueBeginIndex; index < valueEndIndex; ++index) {
            Parser_.ReadValue(currentValue, columnDescriptor, index);
            if (currentValue->Timestamp <= Timestamp_) {
                ++currentValue;
            }
        }
    }

    NTableChunkFormat::TrimRow(row, currentValue, memoryPool);

    return row;
}

////////////////////////////////////////////////////////////////////////////////

template <typename TBlockParser>
TVersionedBlockReader<TBlockParser>::TVersionedBlockReader(
    TSharedRef block,
    const NProto::TDataBlockMeta& blockMeta,
    int blockFormatVersion,
    const TTableSchemaPtr& chunkSchema,
    int keyColumnCount,
    const std::vector<TColumnIdMapping>& schemaIdMapping,
    const TKeyComparer& keyComparer,
    TTimestamp timestamp,
    bool produceAllVersions)
    : TVersionedRowReader<TBlockParser>(
        keyColumnCount,
        schemaIdMapping,
        timestamp,
        produceAllVersions,
        std::move(block),
        blockMeta,
        blockFormatVersion,
        chunkSchema)
    , KeyComparer_(keyComparer)
{ }

template <typename TBlockParser>
bool TVersionedBlockReader<TBlockParser>::NextRow()
{
    return JumpToRowIndex(RowIndex_ + 1);
}

template <typename TBlockParser>
bool TVersionedBlockReader<TBlockParser>::SkipToRowIndex(int rowIndex)
{
    YT_VERIFY(rowIndex >= RowIndex_ && rowIndex >= 0);
    return JumpToRowIndex(rowIndex);
}

template <typename TBlockParser>
bool TVersionedBlockReader<TBlockParser>::JumpToRowIndex(int rowIndex)
{
    if (Parser_.JumpToRowIndex(rowIndex, &RowMetadata_)) {
        RowIndex_ = rowIndex;
        return true;
    }
    return false;
}

template <typename TBlockParser>
bool TVersionedBlockReader<TBlockParser>::SkipToKey(TLegacyKey key)
{
    if (RowIndex_ < 0) {
        YT_VERIFY(JumpToRowIndex(0));
    }

    auto inBound = [&] (TUnversionedRow pivot) {
        // Key is already widened here.
        return CompareKeys(pivot, key, KeyComparer_.Get()) >= 0;
    };

    if (inBound(GetKey())) {
        // We are already further than pivot key.
        return true;
    }

    auto rowIndex = BinarySearch(
        RowIndex_,
        Parser_.GetRowCount(),
        [&] (int rowIndex) {
            // TODO(akozhikhov): Optimize?
            YT_VERIFY(JumpToRowIndex(rowIndex));
            return !inBound(GetKey());
        });

    return JumpToRowIndex(rowIndex);
}

template <typename TBlockParser>
TMutableVersionedRow TVersionedBlockReader<TBlockParser>::GetRow(TChunkedMemoryPool* memoryPool)
{
    YT_VERIFY(Parser_.IsValid());
    return TVersionedRowReader<TBlockParser>::GetRow(memoryPool);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
