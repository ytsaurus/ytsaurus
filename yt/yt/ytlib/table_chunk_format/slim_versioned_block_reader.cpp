#include "slim_versioned_block_reader.h"

#include "private.h"

#include <yt/yt_proto/yt/client/table_chunk_format/proto/chunk_meta.pb.h>

#include <yt/yt/client/table_client/schema.h>

namespace NYT::NTableChunkFormat {

using namespace NTableClient;
using namespace NTableClient::NProto;

////////////////////////////////////////////////////////////////////////////////

TSlimVersionedBlockReader::TSlimVersionedBlockReader(
    TSharedRef block,
    const TDataBlockMeta& blockMeta,
    int /*blockFormatVersion*/,
    const TTableSchemaPtr& chunkSchema,
    int keyColumnCount,
    const std::vector<TColumnIdMapping>& schemaIdMapping,
    const TKeyComparer& keyComparer,
    TTimestamp timestamp,
    bool produceAllVersions)
    : TVersionedRowParserBase(chunkSchema)
    , Block_(std::move(block))
    , Timestamp_(timestamp)
    , ProduceAllVersions_(produceAllVersions)
    , KeyColumnCount_(keyColumnCount)
    , ChunkKeyColumnCount_(chunkSchema->GetKeyColumnCount())
    , KeyComparer_(keyComparer)
    , RowCount_(blockMeta.row_count())
    , ReaderSchemaColumnCount_(std::max(GetReaderSchemaColumnCount(schemaIdMapping), KeyColumnCount_))
    , ReaderIdToAggregateFlagStorage_(static_cast<size_t>(ReaderSchemaColumnCount_), false)
    , ReaderIdToAggregateFlag_(ReaderIdToAggregateFlagStorage_.data())
    , ChunkToReaderIdMappingStorage_(static_cast<size_t>(chunkSchema->GetColumnCount()), -1)
    , ChunkToReaderIdMapping_(ChunkToReaderIdMappingStorage_.data())
    , ReaderToChunkIdMappingStorage_(static_cast<size_t>(ReaderSchemaColumnCount_), -1)
    , ReaderToChunkIdMapping_(ReaderToChunkIdMappingStorage_.data())
{
    for (const auto& idMapping : schemaIdMapping) {
        ChunkToReaderIdMapping_[idMapping.ChunkSchemaIndex] = idMapping.ReaderSchemaIndex;
        ReaderToChunkIdMapping_[idMapping.ReaderSchemaIndex] = idMapping.ChunkSchemaIndex;
        if (chunkSchema->Columns()[idMapping.ChunkSchemaIndex].Aggregate()) {
            ReaderIdToAggregateFlag_[idMapping.ReaderSchemaIndex] = true;
        }
    }

    for (int index = 0; index < ChunkKeyColumnCount_; ++index) {
        ChunkToReaderIdMapping_[index] = index;
        ReaderToChunkIdMapping_[index] = index;
    }

    const char* ptr = Block_.begin();

    auto readPadding = [&] {
        ReadPadding(ptr, ptr - Block_.begin());
    };

    TSlimVersionedBlockHeader header;
    ReadPod(ptr, header);
    readPadding();

    auto initializeStream = [&] (const auto*& stream, int byteSize) {
        stream = reinterpret_cast<std::remove_reference_t<decltype(stream)>>(ptr);
        ptr += byteSize;
        readPadding();
    };
    initializeStream(RowOffsets_, header.RowOffsetsSize);
    initializeStream(Timestamps_, header.TimestampDataSize);
    initializeStream(KeyDictionary_.Offsets, header.KeyDictionaryOffsetsSize);
    initializeStream(KeyDictionary_.Data, header.KeyDictionaryDataSize);
    initializeStream(ValueDictionary_.Offsets, header.ValueDictionaryOffsetsSize);
    initializeStream(ValueDictionary_.Data, header.ValueDictionaryDataSize);
    initializeStream(Rows_, header.RowDataSize);

    ValueCountPerRowEstimate_ = header.ValueCountPerRowEstimate;

    KeyScratch_.reserve(GetUnversionedRowByteSize(KeyColumnCount_));
    Key_ = TLegacyMutableKey::Create(KeyScratch_.data(), KeyColumnCount_);
    for (int index = 0; index < KeyColumnCount_; ++index) {
        Key_[index] = MakeUnversionedNullValue(index);
    }
}

TLegacyKey TSlimVersionedBlockReader::GetKey() const
{
    return Key_;
}

TMutableVersionedRow TSlimVersionedBlockReader::GetRow(TChunkedMemoryPool* memoryPool)
{
    YT_VERIFY(Valid_);
    return ProduceAllVersions_
        ? ReadRowAllVersions(memoryPool)
        : ReadRowSingleVersion(memoryPool);
}

TMutableVersionedRow TSlimVersionedBlockReader::ReadRowAllVersions(TChunkedMemoryPool* memoryPool)
{
    WriteTimestampScratch_.clear();
    DeleteTimestampScratch_.clear();
    ValueScratch_.clear();

    const char* ptr = RowPtr_;

    // Counters
    ui32 deleteTimestampCount;
    ptr += ReadVarUint32(ptr, &deleteTimestampCount);

    for (ui32 index = 0; index < deleteTimestampCount; ++index) {
        ui32 timestampIndex;
        ptr += ReadVarUint32(ptr, &timestampIndex);
        auto timestamp = Timestamps_[timestampIndex];
        if (timestamp <= Timestamp_) {
            DeleteTimestampScratch_.push_back(timestamp);
        }
    }

    // Values
    const auto* endPtr = Rows_ + RowOffsets_[RowIndex_ + 1];
    while (ptr != endPtr) {
        ui32 timestampIndex;
        ptr += ReadVarUint32(ptr, &timestampIndex);
        auto timestamp = Timestamps_[timestampIndex];

        if (timestamp <= Timestamp_) {
            WriteTimestampScratch_.push_back(timestamp);
            ReadValues(
                ValueDictionary_,
                ptr,
                [&] (int /*chunkSchemaId*/) {
                    auto* value = &ValueScratch_.emplace_back();
                    value->Timestamp = timestamp;
                    return value;
                });
        } else {
            SkipValues(
                ValueDictionary_,
                ptr);
        }
    }

    SortUnique(WriteTimestampScratch_, std::greater<>());
    Sort(DeleteTimestampScratch_, std::greater<>());

    auto row = TMutableVersionedRow::Allocate(
        memoryPool,
        KeyColumnCount_,
        std::ssize(ValueScratch_),
        std::ssize(WriteTimestampScratch_),
        std::ssize(DeleteTimestampScratch_));

    FillKey(row);
    std::copy(WriteTimestampScratch_.begin(), WriteTimestampScratch_.end(), row.BeginWriteTimestamps());
    std::copy(DeleteTimestampScratch_.begin(), DeleteTimestampScratch_.end(), row.BeginDeleteTimestamps());
    std::copy(ValueScratch_.begin(), ValueScratch_.end(), row.BeginValues());

    SortRowValues(row);
    return row;
}

TMutableVersionedRow TSlimVersionedBlockReader::ReadRowSingleVersion(TChunkedMemoryPool* memoryPool)
{
    const char* ptr = RowPtr_;

    // Counters
    ui32 deleteTimestampCount;
    ptr += ReadVarUint32(ptr, &deleteTimestampCount);

    auto deleteTimestamp = NullTimestamp;
    for (ui32 index = 0; index < deleteTimestampCount; ++index) {
        ui32 timestampIndex;
        ptr += ReadVarUint32(ptr, &timestampIndex);
        auto timestamp = Timestamps_[timestampIndex];
        if (deleteTimestamp == NullTimestamp && timestamp <= Timestamp_) {
            deleteTimestamp = timestamp;
        }
        YT_ASSERT(deleteTimestamp == NullTimestamp || timestamp <= deleteTimestamp);
    }

    auto row = TMutableVersionedRow::Allocate(
        memoryPool,
        KeyColumnCount_,
        ValueCountPerRowEstimate_, // shrinkable
        /*writeTimestampCount*/ 1,
        /*deleteTimestampCount*/ deleteTimestamp == NullTimestamp ? 0 : 1);

    // Values
    const auto* endPtr = Rows_ + RowOffsets_[RowIndex_ + 1];
    auto* value = row.BeginValues();
    const auto* endValue = row.EndValues();
    auto writeTimestamp = NullTimestamp;

    while (ptr != endPtr) {
        ui32 timestampIndex;
        ptr += ReadVarUint32(ptr, &timestampIndex);
        auto timestamp = Timestamps_[timestampIndex];

        if (timestamp > deleteTimestamp && timestamp <= Timestamp_) {
            writeTimestamp = std::max(writeTimestamp, timestamp);
            ReadValues(
                ValueDictionary_,
                ptr,
                [&] (int /*chunkSchemaId*/) {
                    if (Y_UNLIKELY(value == endValue)) {
                        int oldValueCount = row.GetValueCount();
                        int newValueCount = oldValueCount * 2;
                        row = IncreaseRowValueCount(row, newValueCount, memoryPool);
                        value = row.BeginValues() + oldValueCount;
                        endValue = row.EndValues();
                    }
                    value->Timestamp = timestamp;
                    return value++;
                });
            YT_ASSERT(value <= row.EndValues());
        } else {
            SkipValues(
                ValueDictionary_,
                ptr);
        }
    }

    if (writeTimestamp == NullTimestamp && deleteTimestamp == NullTimestamp) {
        // Row didn't exist at given timestamp.
        FreeRow(row, memoryPool);
        return {};
    }

    if (deleteTimestamp > writeTimestamp) {
        // Row has been deleted at given timestamp.
        row.SetWriteTimestampCount(0);
        row.BeginDeleteTimestamps()[0] = deleteTimestamp;
        TrimRow(row, row.BeginValues(), memoryPool);
        FillKey(row);
        return row;
    }

    row.BeginWriteTimestamps()[0] = writeTimestamp;
    if (deleteTimestamp != NullTimestamp) {
        row.BeginDeleteTimestamps()[0] = deleteTimestamp;
    }

    TrimRow(row, value, memoryPool);
    SortRowValues(row);
    FilterSingleRowVersion(row, ReaderIdToAggregateFlag_, memoryPool);
    FillKey(row);

    return row;
}

bool TSlimVersionedBlockReader::NextRow()
{
    return JumpToRowIndex(RowIndex_ + 1);
}

bool TSlimVersionedBlockReader::SkipToRowIndex(int rowIndex)
{
    return JumpToRowIndex(rowIndex);
}

bool TSlimVersionedBlockReader::SkipToKey(TLegacyKey key)
{
    if (RowIndex_ < 0) {
        JumpToRowIndex(0);
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
        RowCount_,
        [&] (int rowIndex) {
            // TODO(akozhikhov): Optimize?
            YT_VERIFY(JumpToRowIndex(rowIndex));
            return !inBound(GetKey());
        });

    return JumpToRowIndex(rowIndex);
}

bool TSlimVersionedBlockReader::JumpToRowIndex(int rowIndex)
{
    if (rowIndex < 0 || rowIndex >= RowCount_) {
        Valid_ = false;
        return false;
    }

    const char* ptr = Rows_ + RowOffsets_[rowIndex];
    int count = 0;
    ReadValues(
        KeyDictionary_,
        ptr,
        [&] (int chunkSchemaId) {
            ++count;
            return &Key_[chunkSchemaId];
        });
    YT_ASSERT(count == ChunkKeyColumnCount_);

    RowIndex_ = rowIndex;
    RowPtr_ = ptr;
    Valid_ = true;
    return true;
}

void TSlimVersionedBlockReader::FillKey(TMutableVersionedRow row)
{
    std::copy(Key_.Begin(), Key_.End(), row.BeginKeys());
}

template <class TValueParser>
Y_FORCE_INLINE void TSlimVersionedBlockReader::ParseValues(
    TDictionary dictionary,
    const char*& ptr,
    TValueParser valueParser)
{
    bool eos = false;
    while (!eos) {
        ui32 dictionaryTag;
        ptr += ReadVarUint32(ptr, &dictionaryTag);

        auto dictionaryIndex = dictionaryTag >> SlimVersionedDictionaryTagIndexShift;
        eos = (dictionaryTag & SlimVersionedDictionaryTagEos) != 0;

        const char* dictionaryPtr = dictionary.Data + dictionary.Offsets[dictionaryIndex];
        const char* endDictionaryPtr = dictionary.Data + dictionary.Offsets[dictionaryIndex + 1];
        while (dictionaryPtr != endDictionaryPtr) {
            ui32 valueTag;
            dictionaryPtr += ReadVarUint32(dictionaryPtr, &valueTag);
            valueParser(dictionaryPtr, valueTag);
        }
    }
}

template <class TValueCtor>
Y_FORCE_INLINE void TSlimVersionedBlockReader::ReadValues(
    TDictionary dictionary,
    const char*& ptr,
    TValueCtor valueCtor)
{
    ParseValues(
        dictionary,
        ptr,
        [&] (const char*& dictionaryPtr, ui32 valueTag) {
            auto chunkSchemaId = valueTag >> SlimVersionedIdValueTagShift;
            auto readerSchemaId = ChunkToReaderIdMapping_[chunkSchemaId];
            if (readerSchemaId >= 0) {
                auto* value = valueCtor(chunkSchemaId);
                value->Id = readerSchemaId;
                value->Flags = EValueFlags::None;
                if ((valueTag & SlimVersionedValueTagAggregate) != 0) {
                    value->Flags |= EValueFlags::Aggregate;
                }
                if ((valueTag & SlimVersionedValueTagNull) != 0) {
                    value->Type = EValueType::Null;
                } else {
                    if (ColumnHunkFlags_[chunkSchemaId]) {
                        value->Flags |= EValueFlags::Hunk;
                    }
                    value->Type = PhysicalColumnTypes_[chunkSchemaId];
                    if ((valueTag & SlimVersionedValueTagDictionaryPayload) != 0) {
                        ReadNonNullValue(dictionaryPtr, chunkSchemaId, value);
                    } else {
                        ReadNonNullValue(ptr, chunkSchemaId, value);
                    }
                }
            } else if ((valueTag & SlimVersionedValueTagNull) == 0) {
                if ((valueTag & SlimVersionedValueTagDictionaryPayload) != 0) {
                    SkipNonNullValue(dictionaryPtr, chunkSchemaId);
                } else {
                    SkipNonNullValue(ptr, chunkSchemaId);
                }
            }
        });
}

Y_FORCE_INLINE void TSlimVersionedBlockReader::SkipValues(
    TDictionary dictionary,
    const char*& ptr)
{
    ParseValues(
        dictionary,
        ptr,
        [&] (const char*& dictionaryPtr, ui32 valueTag) {
            auto chunkSchemaId = valueTag >> SlimVersionedIdValueTagShift;
            if ((valueTag & SlimVersionedValueTagNull) == 0) {
                if ((valueTag & SlimVersionedValueTagDictionaryPayload) != 0) {
                    SkipNonNullValue(dictionaryPtr, chunkSchemaId);
                } else {
                    SkipNonNullValue(ptr, chunkSchemaId);
                }
            }
        });
}

Y_FORCE_INLINE void TSlimVersionedBlockReader::ReadNonNullValue(
    const char*& ptr,
    int chunkSchemaId,
    TUnversionedValue* value)
{
    auto type = LogicalColumnTypes_[chunkSchemaId];
    switch (type) {
        case ESimpleLogicalValueType::Null:
        case ESimpleLogicalValueType::Void:
            break;

        case ESimpleLogicalValueType::Int64:
        case ESimpleLogicalValueType::Interval:
            YT_ASSERT(value->Type == EValueType::Int64);
            ReadPod(ptr, value->Data.Int64);
            break;

        case ESimpleLogicalValueType::Uint64:
        case ESimpleLogicalValueType::Date:
        case ESimpleLogicalValueType::Datetime:
        case ESimpleLogicalValueType::Timestamp:
            YT_ASSERT(value->Type == EValueType::Uint64);
            ReadPod(ptr, value->Data.Uint64);
            break;

        case ESimpleLogicalValueType::Double:
            YT_ASSERT(value->Type == EValueType::Double);
            ReadPod(ptr, value->Data.Double);
            break;

        case ESimpleLogicalValueType::Float: {
            YT_ASSERT(value->Type == EValueType::Double);
            float data;
            ReadPod(ptr, data);
            value->Data.Double = data;
            break;
        }

        case ESimpleLogicalValueType::Boolean:
            YT_ASSERT(value->Type == EValueType::Boolean);
            ReadPod(ptr, value->Data.Boolean);
            break;

        case ESimpleLogicalValueType::String:
        case ESimpleLogicalValueType::Any:
        case ESimpleLogicalValueType::Json:
        case ESimpleLogicalValueType::Utf8:
        case ESimpleLogicalValueType::Uuid:
            YT_ASSERT(IsStringLikeType(value->Type));
            ptr += ReadVarUint32(ptr, &value->Length);
            value->Data.String = ptr;
            ptr += value->Length;
            break;

        case ESimpleLogicalValueType::Int8: {
            YT_ASSERT(value->Type == EValueType::Int64);
            i8 data;
            ReadPod(ptr, data);
            value->Data.Int64 = data;
            break;
        }

        case ESimpleLogicalValueType::Uint8: {
            YT_ASSERT(value->Type == EValueType::Uint64);
            ui8 data;
            ReadPod(ptr, data);
            value->Data.Uint64 = data;
            break;
        }

        case ESimpleLogicalValueType::Int16: {
            YT_ASSERT(value->Type == EValueType::Int64);
            i16 data;
            ReadPod(ptr, data);
            value->Data.Int64 = data;
            break;
        }

        case ESimpleLogicalValueType::Uint16: {
            YT_ASSERT(value->Type == EValueType::Uint64);
            ui16 data;
            ReadPod(ptr, data);
            value->Data.Uint64 = data;
            break;
        }

        case ESimpleLogicalValueType::Int32: {
            YT_ASSERT(value->Type == EValueType::Int64);
            i32 data;
            ReadPod(ptr, data);
            value->Data.Int64 = data;
            break;
        }

        case ESimpleLogicalValueType::Uint32: {
            YT_ASSERT(value->Type == EValueType::Uint64);
            ui32 data;
            ReadPod(ptr, data);
            value->Data.Uint64 = data;
            break;
        }

        default:
            THROW_ERROR_EXCEPTION("Unsupported simple logical type %Qlv",
                type);
    }
}

Y_FORCE_INLINE void TSlimVersionedBlockReader::SkipNonNullValue(
    const char*& ptr,
    int chunkSchemaId)
{
    auto type = LogicalColumnTypes_[chunkSchemaId];
    switch (type) {
        case ESimpleLogicalValueType::Null:
        case ESimpleLogicalValueType::Void:
            break;

        case ESimpleLogicalValueType::Int64:
        case ESimpleLogicalValueType::Interval:
            ptr += sizeof(i64);
            break;

        case ESimpleLogicalValueType::Uint64:
        case ESimpleLogicalValueType::Date:
        case ESimpleLogicalValueType::Datetime:
        case ESimpleLogicalValueType::Timestamp:
            ptr += sizeof(ui64);
            break;

        case ESimpleLogicalValueType::Double:
            ptr += sizeof(double);
            break;

        case ESimpleLogicalValueType::Float:
            ptr += sizeof(float);
            break;

        case ESimpleLogicalValueType::Boolean:
            ptr += sizeof(bool);
            break;

        case ESimpleLogicalValueType::String:
        case ESimpleLogicalValueType::Any:
        case ESimpleLogicalValueType::Json:
        case ESimpleLogicalValueType::Utf8:
        case ESimpleLogicalValueType::Uuid: {
            ui32 length;
            ptr += ReadVarUint32(ptr, &length);
            ptr += length;
            break;
        }

        case ESimpleLogicalValueType::Int8:
            ptr += sizeof(i8);
            break;

        case ESimpleLogicalValueType::Uint8:
            ptr += sizeof(ui8);
            break;

        case ESimpleLogicalValueType::Int16:
            ptr += sizeof(i16);
            break;

        case ESimpleLogicalValueType::Uint16:
            ptr += sizeof(ui16);
            break;

        case ESimpleLogicalValueType::Int32:
            ptr += sizeof(i32);
            break;

        case ESimpleLogicalValueType::Uint32:
            ptr += sizeof(ui32);
            break;

        default:
            THROW_ERROR_EXCEPTION("Unsupported simple logical type %Qlv",
                type);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableChunkFormat
