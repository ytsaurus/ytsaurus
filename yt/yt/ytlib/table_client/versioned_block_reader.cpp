#include "versioned_block_reader.h"
#include "private.h"
#include "versioned_block_writer.h"
#include "schemaless_block_reader.h"

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/misc/checksum.h>
#include <yt/yt/core/misc/serialize.h>
#include <yt/yt/core/misc/algorithm_helpers.h>

namespace NYT::NTableClient {

using namespace NTransactionClient;
using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

TVersionedRowParserBase::TVersionedRowParserBase(
    const TTableSchemaPtr& chunkSchema)
    : ChunkKeyColumnCount_(chunkSchema->GetKeyColumnCount())
    , ChunkColumnCount_(chunkSchema->GetColumnCount())
    , ColumnHunkFlags_(new bool[ChunkColumnCount_])
    , ColumnAggregateFlags_(new bool[ChunkColumnCount_])
    , ColumnTypes_(new EValueType[ChunkColumnCount_])
{
    for (int id = 0; id < chunkSchema->GetColumnCount(); ++id) {
        const auto& columnSchema = chunkSchema->Columns()[id];
        ColumnHunkFlags_[id] = columnSchema.MaxInlineHunkSize().has_value();
        ColumnAggregateFlags_[id] = columnSchema.Aggregate().has_value();
        ColumnTypes_[id] = columnSchema.GetWireType();
    }
}

////////////////////////////////////////////////////////////////////////////////

TSimpleVersionedBlockParser::TSimpleVersionedBlockParser(
    TSharedRef block,
    const NProto::TDataBlockMeta& blockMeta,
    const TTableSchemaPtr& chunkSchema)
    : TVersionedRowParserBase(chunkSchema)
    , RowCount_(blockMeta.row_count())
    , Block_(std::move(block))
{
    YT_VERIFY(RowCount_ > 0);

   const auto& simpleVersionedBlockMetaExt = blockMeta.GetExtension(TSimpleVersionedBlockMeta::block_meta_ext);

    KeyData_ = TRef(
        const_cast<char*>(Block_.Begin()),
        GetSimpleVersionedBlockPaddedKeySize(
            ChunkKeyColumnCount_,
            ChunkColumnCount_) * RowCount_);

    ValueData_ = TRef(
        KeyData_.End(),
        VersionedBlockValueSize * simpleVersionedBlockMetaExt.value_count());

    TimestampsData_ = TRef(
        ValueData_.End(),
        sizeof(TTimestamp) * simpleVersionedBlockMetaExt.timestamp_count());

    const char* ptr = TimestampsData_.End();
    KeyNullFlags_.Reset(ptr, ChunkKeyColumnCount_ * RowCount_);
    ptr += AlignUp(KeyNullFlags_.GetByteSize(), SerializationAlignment);

    ValueNullFlags_.Reset(ptr, simpleVersionedBlockMetaExt.value_count());
    ptr += AlignUp(ValueNullFlags_.GetByteSize(), SerializationAlignment);

    for (bool aggregate : MakeRange(ColumnAggregateFlags_.get(), ChunkColumnCount_)) {
        if (aggregate) {
            ValueAggregateFlags_ = TReadOnlyBitmap(ptr, simpleVersionedBlockMetaExt.value_count());
            ptr += AlignUp(ValueAggregateFlags_->GetByteSize(), SerializationAlignment);
            break;
        }
    }

    StringData_ = TRef(const_cast<char*>(ptr), const_cast<char*>(Block_.End()));
}

bool TSimpleVersionedBlockParser::JumpToRowIndex(i64 rowIndex, TRowMetadata* rowMetadata)
{
    YT_VERIFY(!Closed_);

    if (rowIndex >= RowCount_) {
        Closed_ = true;
        return false;
    }

    const char* keyDataPtr = KeyData_.Begin() + GetSimpleVersionedBlockPaddedKeySize(
        ChunkKeyColumnCount_,
        ChunkColumnCount_) * rowIndex;

    for (int id = 0; id < ChunkKeyColumnCount_; ++id) {
        ReadKeyValue(&rowMetadata->Key[id], id, keyDataPtr, rowIndex);
        keyDataPtr += 8;
    }

    TimestampOffset_ = *reinterpret_cast<const i64*>(keyDataPtr);
    keyDataPtr += sizeof(i64);

    ValueOffset_ = *reinterpret_cast<const i64*>(keyDataPtr);
    keyDataPtr += sizeof(i64);

    auto writeTimestampCount = *reinterpret_cast<const ui16*>(keyDataPtr);
    keyDataPtr += sizeof(ui16);

    auto deleteTimestampCount = *reinterpret_cast<const ui16*>(keyDataPtr);
    keyDataPtr += sizeof(ui16);

    rowMetadata->WriteTimestamps = TRange(
        reinterpret_cast<const TTimestamp*>(
            TimestampsData_.Begin() +
            TimestampOffset_ * sizeof(TTimestamp)),
        writeTimestampCount);
    rowMetadata->DeleteTimestamps = TRange(
        reinterpret_cast<const TTimestamp*>(
            TimestampsData_.Begin() +
            (TimestampOffset_ + writeTimestampCount) * sizeof(TTimestamp)),
        deleteTimestampCount);

    ColumnValueCounts_ = keyDataPtr;
    rowMetadata->ValueCount = GetColumnValueCount(ChunkColumnCount_ - 1);

    return true;
}

ui32 TSimpleVersionedBlockParser::GetColumnValueCount(int chunkSchemaId) const
{
    YT_ASSERT(chunkSchemaId >= ChunkKeyColumnCount_);
    return *(reinterpret_cast<const ui32*>(ColumnValueCounts_) + chunkSchemaId - ChunkKeyColumnCount_);
}

TSimpleVersionedBlockParser::TColumnDescriptor
TSimpleVersionedBlockParser::GetColumnDescriptor(
    const TColumnIdMapping& mapping) const
{
    int valueId = mapping.ReaderSchemaIndex;
    int chunkSchemaId = mapping.ChunkSchemaIndex;

    int lowerValueIndex = chunkSchemaId == ChunkKeyColumnCount_ ? 0 : GetColumnValueCount(chunkSchemaId - 1);
    int upperValueIndex = GetColumnValueCount(chunkSchemaId);
    lowerValueIndex += ValueOffset_;
    upperValueIndex += ValueOffset_;

    return TColumnDescriptor{
        .ValueId = valueId,
        .ChunkSchemaId = chunkSchemaId,
        .LowerValueIndex = lowerValueIndex,
        .UpperValueIndex = upperValueIndex,
        .Aggregate = ColumnAggregateFlags_[chunkSchemaId],
    };
}

void TSimpleVersionedBlockParser::ReadKeyValue(
    TUnversionedValue* value,
    int id,
    const char* ptr,
    i64 rowIndex) const
{
    bool isNull = KeyNullFlags_[rowIndex * ChunkKeyColumnCount_ + id];
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

void TSimpleVersionedBlockParser::ReadValue(
    TVersionedValue* value,
    const TColumnDescriptor& columnDescriptor,
    int valueIndex) const
{
    YT_ASSERT(columnDescriptor.ValueId >= ChunkKeyColumnCount_);

    const char* ptr = ValueData_.Begin() + VersionedBlockValueSize * valueIndex;
    auto timestamp = *reinterpret_cast<const TTimestamp*>(ptr + 8);

    *value = {};
    value->Id = columnDescriptor.ValueId;
    value->Timestamp = timestamp;

    if (ValueAggregateFlags_ && (*ValueAggregateFlags_)[valueIndex]) {
        value->Flags |= EValueFlags::Aggregate;
    }

    if (Y_UNLIKELY(ValueNullFlags_[valueIndex])) {
        value->Type = EValueType::Null;
        return;
    }

    if (ColumnHunkFlags_[columnDescriptor.ChunkSchemaId]) {
        value->Flags |= EValueFlags::Hunk;
    }

    auto type = ColumnTypes_[columnDescriptor.ChunkSchemaId];
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
        case EValueType::Composite:
            ReadStringLike(value, ptr);
            break;

        case EValueType::Null:
        case EValueType::Min:
        case EValueType::Max:
        case EValueType::TheBottom:
            YT_ABORT();
    }
}

TTimestamp TSimpleVersionedBlockParser::ReadValueTimestamp(
    const TColumnDescriptor& /*columnDescriptor*/,
    int valueIndex) const
{
    const char* ptr = ValueData_.Begin() + VersionedBlockValueSize * valueIndex;
    return *reinterpret_cast<const TTimestamp*>(ptr + 8);
}

void TSimpleVersionedBlockParser::ReadStringLike(TUnversionedValue* value, const char* ptr) const
{
    ui32 offset = *reinterpret_cast<const ui32*>(ptr);
    ptr += sizeof(ui32);

    ui32 length = *reinterpret_cast<const ui32*>(ptr);

    value->Data.String = StringData_.Begin() + offset;
    value->Length = length;
}

////////////////////////////////////////////////////////////////////////////////

TIndexedVersionedRowParser::TIndexedVersionedRowParser(
    const TTableSchemaPtr& chunkSchema,
    TCompactVector<int, IndexedRowTypicalGroupCount> groupIndexesToRead)
    : TVersionedRowParserBase(chunkSchema)
    , BlockFormatDetail_(chunkSchema)
    , GroupCount_(BlockFormatDetail_.GetGroupCount())
    , HasAggregateColumns_(chunkSchema->HasAggregateColumns())
    , GroupIndexesToRead_(std::move(groupIndexesToRead))
{
    GroupInfos_.resize(GroupCount_);
}

TIndexedVersionedRowParser::TColumnDescriptor
TIndexedVersionedRowParser::GetColumnDescriptor(const TColumnIdMapping& mapping)
{
    int valueId = mapping.ReaderSchemaIndex;
    int chunkSchemaId = mapping.ChunkSchemaIndex;

    auto columnInfo = BlockFormatDetail_.GetValueColumnInfo(chunkSchemaId);

    const auto& groupInfo = GetGroupInfo(columnInfo.GroupIndex, columnInfo.ColumnCountInGroup);

    int lowerValueIndex = groupInfo.ColumnValueCounts[columnInfo.ColumnIndexInGroup];
    int upperValueIndex = columnInfo.ColumnIndexInGroup + 1 == columnInfo.ColumnCountInGroup
        ? groupInfo.ValueCount
        : groupInfo.ColumnValueCounts[columnInfo.ColumnIndexInGroup + 1];

    return TColumnDescriptor{
        .GroupInfo = groupInfo,
        .ValueId = valueId,
        .ChunkSchemaId = chunkSchemaId,
        .LowerValueIndex = lowerValueIndex,
        .UpperValueIndex = upperValueIndex,
        .Aggregate = ColumnAggregateFlags_[chunkSchemaId],
    };
}

void TIndexedVersionedRowParser::ReadValue(
    TVersionedValue* value,
    const TIndexedVersionedRowParser::TColumnDescriptor& columnDescriptor,
    int valueIndex) const
{
    YT_ASSERT(columnDescriptor.ValueId >= ChunkKeyColumnCount_);

    const char* ptr = columnDescriptor.GroupInfo.ValuesBegin + VersionedBlockValueSize * valueIndex;
    auto timestamp = *reinterpret_cast<const TTimestamp*>(ptr + 8);

    *value = {};
    value->Id = columnDescriptor.ValueId;
    value->Timestamp = timestamp;

    const auto& aggregateFlags = columnDescriptor.GroupInfo.AggregateFlags;
    if (aggregateFlags && (*aggregateFlags)[valueIndex]) {
        value->Flags |= EValueFlags::Aggregate;
    }

    if (Y_UNLIKELY(columnDescriptor.GroupInfo.NullFlags[valueIndex])) {
        value->Type = EValueType::Null;
        return;
    }

    if (ColumnHunkFlags_[columnDescriptor.ChunkSchemaId]) {
        value->Flags |= EValueFlags::Hunk;
    }

    auto type = ColumnTypes_[columnDescriptor.ChunkSchemaId];
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
        case EValueType::Composite:
            ReadStringLike(value, ptr);
            break;

        case EValueType::Null:
        case EValueType::Min:
        case EValueType::Max:
        case EValueType::TheBottom:
            YT_ABORT();
    }
}

TTimestamp TIndexedVersionedRowParser::ReadValueTimestamp(
    const TColumnDescriptor& columnDescriptor,
    int valueIndex) const
{
    const char* ptr = columnDescriptor.GroupInfo.ValuesBegin + VersionedBlockValueSize * valueIndex;
    return *reinterpret_cast<const TTimestamp*>(ptr + sizeof(i64));
}

void TIndexedVersionedRowParser::PreprocessRow(
    const TCompactVector<TRef, IndexedRowTypicalGroupCount>& rowData,
    const int* groupOffsets,
    const int* groupIndexes,
    bool validateChecksums,
    TRowMetadata* rowMetadata)
{
    auto validateChecksum = [] (TRef data) {
        auto dataWithoutChecksum = data.Slice(0, data.Size() - sizeof(TChecksum));
        auto expectedChecksum = *reinterpret_cast<const TChecksum*>(dataWithoutChecksum.End());
        auto actualChecksum = GetChecksum(dataWithoutChecksum);
        if (expectedChecksum != actualChecksum) {
            THROW_ERROR_EXCEPTION(
                NChunkClient::EErrorCode::IncorrectChunkFileChecksum,
                "Incorrect checksum detected for indexed row: expected %v, actual %v",
                expectedChecksum,
                actualChecksum);
        }
    };

    if (validateChecksums) {
        validateChecksum(rowData[0]);
    }

    auto* rowDataBegin = rowData[0].Begin();

    auto* rowDataPtr = rowDataBegin;
    auto* keyColumns = rowDataPtr;
    rowDataPtr += ChunkKeyColumnCount_ * sizeof(i64);

    KeyNullFlags_.Reset(rowDataPtr, ChunkKeyColumnCount_);
    rowDataPtr += KeyNullFlags_.GetByteSize();

    for (int i = 0; i < ChunkKeyColumnCount_; ++i) {
        ReadKeyValue(&rowMetadata->Key[i], i, keyColumns, &rowDataPtr);
        keyColumns += sizeof(i64);
    }

    rowDataPtr += AlignUpSpace<i64>(rowDataPtr - rowDataBegin, SerializationAlignment);

    auto writeTimestampCount = *reinterpret_cast<const i32*>(rowDataPtr);
    rowDataPtr += sizeof(i32);
    auto deleteTimestampCount = *reinterpret_cast<const i32*>(rowDataPtr);
    rowDataPtr += sizeof(i32);

    rowMetadata->WriteTimestamps = TRange(reinterpret_cast<const TTimestamp*>(rowDataPtr), writeTimestampCount);
    rowDataPtr += sizeof(TTimestamp) * writeTimestampCount;
    rowMetadata->DeleteTimestamps = TRange(reinterpret_cast<const TTimestamp*>(rowDataPtr), deleteTimestampCount);
    rowDataPtr += sizeof(TTimestamp) * deleteTimestampCount;

    rowMetadata->ValueCount = 0;

    auto processGroup = [&] (const char* groupDataBegin, int groupIndex) {
        GroupInfos_[groupIndex].Initialized = false;
        GroupInfos_[groupIndex].GroupDataBegin = groupDataBegin;

        auto valueCount = *reinterpret_cast<const i32*>(groupDataBegin);
        GroupInfos_[groupIndex].ValueCount = valueCount;
        rowMetadata->ValueCount += valueCount;
    };

    if (rowData.size() == 1) {
        if (GroupCount_ == 1) {
            processGroup(rowDataPtr, 0);
        } else {
            for (auto groupIndex = 0; groupIndex < GroupCount_; ++groupIndex) {
                auto physicalGroupIndex = groupIndexes ? groupIndexes[groupIndex] : groupIndex;
                processGroup(rowData[0].Begin() + groupOffsets[physicalGroupIndex], groupIndex);
            }
        }
    } else {
        YT_VERIFY(rowData.size() == GroupIndexesToRead_.size() + 1);
        YT_VERIFY(validateChecksums);

        for (int refIndex = 1; refIndex < std::ssize(rowData) - 1; ++refIndex) {
            validateChecksum(rowData[refIndex]);
        }
        // Ignore full row checksum.
        validateChecksum(rowData.back().Slice(0, rowData.back().Size() - sizeof(TChecksum)));

        for (int groupIndex = 0; groupIndex < std::ssize(GroupIndexesToRead_); ++groupIndex) {
            processGroup(rowData[groupIndex + 1].Begin(), GroupIndexesToRead_[groupIndex]);
        }
    }
}

void TIndexedVersionedRowParser::ReadKeyValue(
    TUnversionedValue* value,
    int id,
    const char* ptr,
    const char** rowData) const
{
    bool isNull = KeyNullFlags_[id];
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
            *rowData += value->Length;
            break;

        case EValueType::Null:
        case EValueType::Composite:
        case EValueType::Min:
        case EValueType::Max:
        case EValueType::TheBottom:
            YT_ABORT();
    }
}

void TIndexedVersionedRowParser::ReadStringLike(
    TUnversionedValue* value,
    const char* ptr) const
{
    ui32 offset = *reinterpret_cast<const ui32*>(ptr);
    value->Data.String = ptr + offset;
    ptr += sizeof(ui32);

    ui32 length = *reinterpret_cast<const ui32*>(ptr);
    value->Length = length;
}

const TIndexedVersionedRowParser::TGroupInfo& TIndexedVersionedRowParser::GetGroupInfo(
    int groupIndex,
    int columnCountInGroup)
{
    auto& groupInfo = GroupInfos_[groupIndex];
    if (groupInfo.Initialized) {
        return groupInfo;
    }

    groupInfo.Initialized = true;

    auto* groupData = groupInfo.GroupDataBegin;
    groupData += sizeof(i32);
    groupInfo.ColumnValueCounts = reinterpret_cast<const i32*>(groupData);
    groupData += sizeof(i32) * columnCountInGroup;

    groupInfo.NullFlags.Reset(groupData, groupInfo.ValueCount);
    groupData += groupInfo.NullFlags.GetByteSize();
    if (HasAggregateColumns_) {
        groupInfo.AggregateFlags.emplace().Reset(groupData, groupInfo.ValueCount);
        groupData += groupInfo.AggregateFlags->GetByteSize();
    }
    groupData += AlignUpSpace<i64>(groupData - groupInfo.GroupDataBegin, SerializationAlignment);

    groupInfo.ValuesBegin = groupData;

    return groupInfo;
}

////////////////////////////////////////////////////////////////////////////////

TIndexedVersionedBlockParser::TIndexedVersionedBlockParser(
    TSharedRef block,
    const NProto::TDataBlockMeta& blockMeta,
    const TTableSchemaPtr& chunkSchema)
    : TIndexedVersionedRowParser(chunkSchema)
    , RowCount_(blockMeta.row_count())
    , Block_(std::move(block))
{
    const auto& indexedVersionedBlockMetaExt = blockMeta.GetExtension(TIndexedVersionedBlockMeta::block_meta_ext);
    if (indexedVersionedBlockMetaExt.format_version() != 0) {
        THROW_ERROR_EXCEPTION("Unsupported indexed block format version %v",
            indexedVersionedBlockMetaExt.format_version());
    }

    GroupReorderingEnabled_ = indexedVersionedBlockMetaExt.group_reordering_enabled();
    YT_VERIFY(!GroupReorderingEnabled_);

    auto* blockEnd = Block_.End();

    if (GroupCount_ > 1) {
        if (GroupReorderingEnabled_) {
            blockEnd -= sizeof(i32) * RowCount_ * GroupCount_;
            GroupIndexes_ = reinterpret_cast<const i32*>(blockEnd);
        }

        blockEnd -= sizeof(i32) * RowCount_ * GroupCount_;
        GroupOffsets_ = reinterpret_cast<const i32*>(blockEnd);
    }

    blockEnd -= sizeof(i64) * RowCount_;
    RowOffsets_ = reinterpret_cast<const i64*>(blockEnd);
}

bool TIndexedVersionedBlockParser::JumpToRowIndex(i64 rowIndex, TRowMetadata* rowMetadata)
{
    YT_VERIFY(!Closed_);

    if (rowIndex >= RowCount_) {
        Closed_ = true;
        return false;
    }

    auto rowBegin = RowOffsets_[rowIndex];
    auto rowEnd = rowIndex + 1 < RowCount_
        ? RowOffsets_[rowIndex + 1]
        : Block_.Size();

    const int* groupOffsets = nullptr;
    const int* groupIndexes = nullptr;
    if (GroupCount_ > 1) {
        if (GroupReorderingEnabled_) {
            groupIndexes = GroupIndexes_ + rowIndex * GroupCount_;
        }
        groupOffsets = GroupOffsets_ + rowIndex * GroupCount_;
    }

    PreprocessRow(
        {TRef(Block_).Slice(rowBegin, rowEnd)},
        groupOffsets,
        groupIndexes,
        /*validateChecksums*/ false,
        rowMetadata);

    return true;
}

////////////////////////////////////////////////////////////////////////////////

THorizontalSchemalessVersionedBlockReader::THorizontalSchemalessVersionedBlockReader(
    const TSharedRef& block,
    const NProto::TDataBlockMeta& blockMeta,
    const std::vector<bool>& compositeColumnFlags,
    const std::vector<int>& chunkToReaderIdMapping,
    TRange<ESortOrder> sortOrders,
    int commonKeyPrefix,
    TTimestamp timestamp)
    : THorizontalBlockReader(
        block,
        blockMeta,
        compositeColumnFlags,
        chunkToReaderIdMapping,
        sortOrders,
        commonKeyPrefix,
        /*keyWideningOptions*/ {})
    , Timestamp_(timestamp)
{ }

TLegacyKey THorizontalSchemalessVersionedBlockReader::GetKey() const
{
    return THorizontalBlockReader::GetLegacyKey();
}

TMutableVersionedRow THorizontalSchemalessVersionedBlockReader::GetRow(TChunkedMemoryPool* memoryPool)
{
    return THorizontalBlockReader::GetVersionedRow(memoryPool, Timestamp_);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
