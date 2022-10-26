#ifndef VERSIONED_BLOCK_READER_INL_H_
#error "Direct inclusion of this file is not allowed, include versioned_block_reader.h"
// For the sake of sane code completion.
#include "versioned_block_reader.h"
#endif

#include <yt/yt/library/numeric/algorithm_helpers.h>

namespace NYT::NTableClient {

////////////////////////////////////////////////////////////////////////////////

template <typename TRowParser>
TVersionedRowReader<TRowParser>::TVersionedRowReader(
    int keyColumnCount,
    const std::vector<TColumnIdMapping>& schemaIdMapping,
    TTimestamp timestamp,
    bool produceAllVersions,
    TRowParser rowParser)
    : Parser_(std::move(rowParser))
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
    return RowMetadata_.Key;
}

template <typename TRowParser>
TMutableVersionedRow TVersionedRowReader<TRowParser>::GetRow(TChunkedMemoryPool* memoryPool)
{
    YT_VERIFY(!Parser_.Closed());
    return ProduceAllVersions_
        ? ReadAllVersions(memoryPool)
        : ReadOneVersion(memoryPool);
}

template <typename TRowParser>
TMutableVersionedRow TVersionedRowReader<TRowParser>::ReadAllVersions(TChunkedMemoryPool* memoryPool)
{
    int writeTimestampIndex = 0;
    int deleteTimestampIndex = 0;

    const auto& writeTimestamps = RowMetadata_.WriteTimestamps;
    const auto& deleteTimestamps = RowMetadata_.DeleteTimestamps;

    if (Timestamp_ < MaxTimestamp) {
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
    ::memcpy(row.BeginKeys(), RowMetadata_.Key.Begin(), sizeof(TUnversionedValue) * KeyColumnCount_);

    // Values.
    auto* beginValues = row.BeginValues();
    auto* currentValue = beginValues;
    for (const auto& columnIdMapping : SchemaIdMapping_) {
        auto columnDescriptor = Parser_.GetColumnDescriptor(columnIdMapping);

        for (int index = columnDescriptor.LowerValueIndex; index < columnDescriptor.UpperValueIndex; ++index) {
            Parser_.ReadValue(currentValue, columnDescriptor, index);
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

template <typename TRowParser>
TMutableVersionedRow TVersionedRowReader<TRowParser>::ReadOneVersion(TChunkedMemoryPool* memoryPool)
{
    int writeTimestampIndex = 0;
    int deleteTimestampIndex = 0;

    const auto& writeTimestamps = RowMetadata_.WriteTimestamps;
    const auto& deleteTimestamps = RowMetadata_.DeleteTimestamps;

    if (Timestamp_ < MaxTimestamp) {
        writeTimestampIndex = BinarySearch(0, writeTimestamps.Size(), [&] (int index) {
            return writeTimestamps[index] > Timestamp_;
        });
        deleteTimestampIndex = BinarySearch(0, deleteTimestamps.Size(), [&] (int index) {
            return deleteTimestamps[index] > Timestamp_;
        });
    }

    bool hasWriteTimestamp = writeTimestampIndex < std::ssize(writeTimestamps);
    bool hasDeleteTimestamp = deleteTimestampIndex < std::ssize(deleteTimestamps);

    if (!hasWriteTimestamp & !hasDeleteTimestamp) {
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
            0, // no values
            0, // no write timestamps
            1);
        row.BeginDeleteTimestamps()[0] = deleteTimestamp;
        ::memcpy(row.BeginKeys(), RowMetadata_.Key.Begin(), sizeof(TUnversionedValue) * KeyColumnCount_);
        return row;
    }

    YT_VERIFY(hasWriteTimestamp);

    // Produce output row.
    auto row = TMutableVersionedRow::Allocate(
        memoryPool,
        KeyColumnCount_,
        RowMetadata_.ValueCount, // shrinkable
        1,
        hasDeleteTimestamp ? 1 : 0);

    // Write, delete timestamps.
    row.BeginWriteTimestamps()[0] = writeTimestamp;
    if (hasDeleteTimestamp) {
        row.BeginDeleteTimestamps()[0] = deleteTimestamp;
    }

    // Keys.
    ::memcpy(row.BeginKeys(), RowMetadata_.Key.Begin(), sizeof(TUnversionedValue) * KeyColumnCount_);

    auto* beginValues = row.BeginValues();
    auto* currentValue = beginValues;
    for (const auto& columnIdMapping : SchemaIdMapping_) {
        auto columnDescriptor = Parser_.GetColumnDescriptor(columnIdMapping);

        auto isValueAlive = [&] (int index) -> bool {
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

    // Shrink memory.
    auto* memoryTo = const_cast<char*>(row.GetMemoryEnd());
    row.SetValueCount(currentValue - beginValues);
    auto* memoryFrom = const_cast<char*>(row.GetMemoryEnd());
    memoryPool->Free(memoryFrom, memoryTo);

    return row;
}

////////////////////////////////////////////////////////////////////////////////

template <typename TBlockParser>
TVersionedBlockReader<TBlockParser>::TVersionedBlockReader(
    TSharedRef block,
    const NProto::TDataBlockMeta& blockMeta,
    const TTableSchemaPtr& chunkSchema,
    int keyColumnCount,
    const std::vector<TColumnIdMapping>& schemaIdMapping,
    const TKeyComparer& keyComparer,
    TTimestamp timestamp,
    bool produceAllVersions,
    bool initialize)
    : TVersionedRowReader<TBlockParser>(
        keyColumnCount,
        schemaIdMapping,
        timestamp,
        produceAllVersions,
        TBlockParser(
            std::move(block),
            blockMeta,
            chunkSchema))
    , KeyComparer_(keyComparer)
{
    if (initialize) {
        JumpToRowIndex(0);
    }
}

template <typename TBlockParser>
bool TVersionedBlockReader<TBlockParser>::NextRow()
{
    return JumpToRowIndex(RowIndex_ + 1);
}

template <typename TBlockParser>
bool TVersionedBlockReader<TBlockParser>::SkipToRowIndex(i64 rowIndex)
{
    YT_VERIFY(rowIndex >= RowIndex_);
    return JumpToRowIndex(rowIndex);
}

template <typename TBlockParser>
bool TVersionedBlockReader<TBlockParser>::JumpToRowIndex(i64 rowIndex)
{
    if (Parser_.JumpToRowIndex(rowIndex, &RowMetadata_)) {
        RowIndex_ = rowIndex;
        return true;
    }
    return false;
}

template <typename TBlockParser>
bool TVersionedBlockReader<TBlockParser>::SkipToKey(TLegacyKey bound)
{
    YT_VERIFY(!Parser_.Closed());

    auto inBound = [&] (TUnversionedRow key) -> bool {
        // Key is already widened here.
        return CompareKeys(key, bound, KeyComparer_.Get()) >= 0;
    };

    if (inBound(GetKey())) {
        // We are already further than pivot key.
        return true;
    }

    auto rowIndex = BinarySearch(
        RowIndex_,
        Parser_.RowCount(),
        [&] (int rowIndex) -> bool {
            // TODO(akozhikhov): Optimize?
            YT_VERIFY(JumpToRowIndex(rowIndex));
            return !inBound(GetKey());
        });

    return JumpToRowIndex(rowIndex);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
