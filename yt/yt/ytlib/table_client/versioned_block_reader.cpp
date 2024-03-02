#include "versioned_block_reader.h"
#include "private.h"
#include "versioned_block_writer.h"
#include "schemaless_block_reader.h"
#include "hunks.h"
#include "helpers.h"

#include <yt/yt/ytlib/table_chunk_format/reader_helpers.h>

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/helpers.h>

#include <yt/yt/library/numeric/algorithm_helpers.h>

#include <yt/yt/core/misc/checksum.h>
#include <yt/yt/core/misc/serialize.h>

namespace NYT::NTableClient {

using namespace NTransactionClient;
using namespace NTableChunkFormat;
using namespace NTableClient::NProto;

////////////////////////////////////////////////////////////////////////////////

TVersionedRowParserBase::TVersionedRowParserBase(const TTableSchemaPtr& chunkSchema)
    : ChunkKeyColumnCount_(chunkSchema->GetKeyColumnCount())
    , ChunkColumnCount_(chunkSchema->GetColumnCount())
    , ColumnHunkFlagsStorage_(static_cast<size_t>(ChunkColumnCount_))
    , ColumnHunkFlags_(ColumnHunkFlagsStorage_.data())
    , ColumnAggregateFlagsStorage_(static_cast<size_t>(ChunkColumnCount_))
    , ColumnAggregateFlags_(ColumnAggregateFlagsStorage_.data())
    , PhysicalColumnTypesStorage_(static_cast<size_t>(ChunkColumnCount_), EValueType::Null)
    , PhysicalColumnTypes_(PhysicalColumnTypesStorage_.data())
    , LogicalColumnTypesStorage_(static_cast<size_t>(ChunkColumnCount_), ESimpleLogicalValueType::Null)
    , LogicalColumnTypes_(LogicalColumnTypesStorage_.data())
{
    for (int id = 0; id < chunkSchema->GetColumnCount(); ++id) {
        const auto& columnSchema = chunkSchema->Columns()[id];
        ColumnHunkFlags_[id] = columnSchema.MaxInlineHunkSize().has_value();
        ColumnAggregateFlags_[id] = columnSchema.Aggregate().has_value();
        PhysicalColumnTypes_[id] = columnSchema.GetWireType();
        LogicalColumnTypes_[id] = columnSchema.CastToV1Type();
    }
}

////////////////////////////////////////////////////////////////////////////////

TSimpleVersionedBlockParser::TSimpleVersionedBlockParser(
    TSharedRef block,
    const NProto::TDataBlockMeta& blockMeta,
    int /*blockFormatVersion*/,
    const TTableSchemaPtr& chunkSchema)
    : TVersionedRowParserBase(chunkSchema)
    , Block_(std::move(block))
    , RowCount_(blockMeta.row_count())
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

    for (const auto& columnSchema : chunkSchema->Columns()) {
        if (columnSchema.Aggregate()) {
            ValueAggregateFlags_ = TReadOnlyBitmap(ptr, simpleVersionedBlockMetaExt.value_count());
            ptr += AlignUp(ValueAggregateFlags_->GetByteSize(), SerializationAlignment);
            break;
        }
    }

    StringData_ = TRef(const_cast<char*>(ptr), const_cast<char*>(Block_.End()));
}

int TSimpleVersionedBlockParser::GetRowCount() const
{
    return RowCount_;
}

bool TSimpleVersionedBlockParser::IsValid() const
{
    return Valid_;
}

bool TSimpleVersionedBlockParser::JumpToRowIndex(int rowIndex, TVersionedRowMetadata* rowMetadata)
{
    if (rowIndex < 0 || rowIndex >= RowCount_) {
        Valid_ = false;
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

    Valid_ = true;
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
    int readerSchemaId = mapping.ReaderSchemaIndex;
    int chunkSchemaId = mapping.ChunkSchemaIndex;

    int lowerValueIndex = chunkSchemaId == ChunkKeyColumnCount_ ? 0 : GetColumnValueCount(chunkSchemaId - 1);
    int upperValueIndex = GetColumnValueCount(chunkSchemaId);
    lowerValueIndex += ValueOffset_;
    upperValueIndex += ValueOffset_;

    return TColumnDescriptor{
        .ReaderSchemaId = readerSchemaId,
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
    int rowIndex) const
{
    bool isNull = KeyNullFlags_[rowIndex * ChunkKeyColumnCount_ + id];
    if (Y_UNLIKELY(isNull)) {
        value->Type = EValueType::Null;
        return;
    }

    auto type = PhysicalColumnTypes_[id];
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
    YT_ASSERT(columnDescriptor.ReaderSchemaId >= ChunkKeyColumnCount_);

    const char* ptr = ValueData_.Begin() + VersionedBlockValueSize * valueIndex;
    auto timestamp = *reinterpret_cast<const TTimestamp*>(ptr + 8);
    auto type = PhysicalColumnTypes_[columnDescriptor.ChunkSchemaId];

    *value = {};
    value->Id = columnDescriptor.ReaderSchemaId;
    value->Timestamp = timestamp;
    value->Type = type;

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
    std::vector<int> groupIndexesToRead)
    : TVersionedRowParserBase(chunkSchema)
    , BlockFormatDetail_(chunkSchema)
    , HasAggregateColumns_(chunkSchema->HasAggregateColumns())
    , GroupIndexesToRead_(std::move(groupIndexesToRead))
{
    GroupInfos_.resize(BlockFormatDetail_.GetGroupCount());
}

TIndexedVersionedRowParser::TColumnDescriptor
TIndexedVersionedRowParser::GetColumnDescriptor(const TColumnIdMapping& mapping)
{
    int readerSchemaId = mapping.ReaderSchemaIndex;
    int chunkSchemaId = mapping.ChunkSchemaIndex;

    auto columnInfo = BlockFormatDetail_.GetValueColumnInfo(chunkSchemaId);

    const auto& groupInfo = GetGroupInfo(columnInfo.GroupIndex, columnInfo.ColumnCountInGroup);

    int lowerValueIndex = groupInfo.ColumnValueCounts[columnInfo.ColumnIndexInGroup];
    int upperValueIndex = columnInfo.ColumnIndexInGroup + 1 == columnInfo.ColumnCountInGroup
        ? groupInfo.ValueCount
        : groupInfo.ColumnValueCounts[columnInfo.ColumnIndexInGroup + 1];

    return TColumnDescriptor{
        .GroupInfo = groupInfo,
        .ReaderSchemaId = readerSchemaId,
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
    YT_ASSERT(columnDescriptor.ReaderSchemaId >= ChunkKeyColumnCount_);

    const char* ptr = columnDescriptor.GroupInfo.ValuesBegin + VersionedBlockValueSize * valueIndex;
    auto timestamp = *reinterpret_cast<const TTimestamp*>(ptr + 8);

    *value = {};
    value->Id = columnDescriptor.ReaderSchemaId;
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

    auto type = PhysicalColumnTypes_[columnDescriptor.ChunkSchemaId];
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

void TIndexedVersionedRowParser::ValidateRowDataChecksum(
    const TCompactVector<TRef, IndexedRowTypicalGroupCount>& rowData)
{
    auto validateChecksum = [] (TRef data) {
        auto dataWithoutChecksum = data.Slice(0, data.Size() - sizeof(TChecksum));
        TChecksum expectedChecksum;
        memcpy(&expectedChecksum, dataWithoutChecksum.End(), sizeof(TChecksum));
        auto actualChecksum = GetChecksum(dataWithoutChecksum);
        if (expectedChecksum != actualChecksum) {
            THROW_ERROR_EXCEPTION(
                NChunkClient::EErrorCode::IncorrectChunkFileChecksum,
                "Incorrect checksum detected for indexed row: expected %v, actual %v",
                expectedChecksum,
                actualChecksum);
        }
    };

    validateChecksum(rowData[0]);

    for (int partIndex = 1; partIndex < std::ssize(rowData); ++partIndex) {
        validateChecksum(rowData[partIndex]);
    }
}

void TIndexedVersionedRowParser::ProcessRow(
    const TCompactVector<TRef, IndexedRowTypicalGroupCount>& rowData,
    const int* groupOffsets,
    const int* groupIndexes,
    TVersionedRowMetadata* rowMetadata)
{
    YT_VERIFY(rowData.size() == GroupIndexesToRead_.size() + 1);

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
        if (BlockFormatDetail_.GetGroupCount() == 1) {
            processGroup(rowDataPtr, 0);
        } else {
            for (auto groupIndex = 0; groupIndex < BlockFormatDetail_.GetGroupCount(); ++groupIndex) {
                auto physicalGroupIndex = groupIndexes ? groupIndexes[groupIndex] : groupIndex;
                processGroup(rowData[0].Begin() + groupOffsets[physicalGroupIndex], groupIndex);
            }
        }
    } else {
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

    auto type = PhysicalColumnTypes_[id];
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
    YT_VERIFY(groupInfo.GroupDataBegin);

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
    int blockFormatVersion,
    const TTableSchemaPtr& chunkSchema)
    : TIndexedVersionedRowParser(
        chunkSchema,
        /*groupIndexesToRead*/ {})
    , Block_(std::move(block))
    , RowCount_(blockMeta.row_count())
{
    YT_VERIFY(!GroupReorderingEnabled_);

    if (blockFormatVersion != 1) {
        THROW_ERROR_EXCEPTION("Unsupported indexed block format version %v, expected %v",
            blockFormatVersion,
            1);
    }

    auto* blockEnd = Block_.End();

    auto groupCount = BlockFormatDetail_.GetGroupCount();
    if (groupCount > 1) {
        if (GroupReorderingEnabled_) {
            blockEnd -= sizeof(i32) * RowCount_ * groupCount;
            GroupIndexes_ = reinterpret_cast<const i32*>(blockEnd);
        }

        blockEnd -= sizeof(i32) * RowCount_ * groupCount;
        GroupOffsets_ = reinterpret_cast<const i32*>(blockEnd);
    }

    blockEnd -= sizeof(i64) * RowCount_;
    RowOffsets_ = reinterpret_cast<const i64*>(blockEnd);
}

int TIndexedVersionedBlockParser::GetRowCount() const
{
    return RowCount_;
}

bool TIndexedVersionedBlockParser::IsValid() const
{
    return Valid_;
}

bool TIndexedVersionedBlockParser::JumpToRowIndex(int rowIndex, TVersionedRowMetadata* rowMetadata)
{
    if (rowIndex < 0 || rowIndex >= RowCount_) {
        Valid_ = false;
        return false;
    }

    auto rowBegin = RowOffsets_[rowIndex];
    auto rowEnd = rowIndex + 1 < RowCount_
        ? RowOffsets_[rowIndex + 1]
        : Block_.Size();

    const int* groupOffsets = nullptr;
    const int* groupIndexes = nullptr;
    auto groupCount = BlockFormatDetail_.GetGroupCount();
    if (groupCount > 1) {
        if (GroupReorderingEnabled_) {
            groupIndexes = GroupIndexes_ + rowIndex * groupCount;
        }
        groupOffsets = GroupOffsets_ + rowIndex * groupCount;
    }

    ProcessRow(
        {TRef(Block_).Slice(rowBegin, rowEnd)},
        groupOffsets,
        groupIndexes,
        rowMetadata);

    Valid_ = true;
    return true;
}

////////////////////////////////////////////////////////////////////////////////

template <>
TMutableVersionedRow TVersionedRowReader<TIndexedVersionedRowParser>::ProcessAndGetRow(
    const TCompactVector<TSharedRef, IndexedRowTypicalGroupCount>& owningRowData,
    const int* groupOffsets,
    const int* groupIndexes,
    TChunkedMemoryPool* memoryPool)
{
    TCompactVector<TRef, IndexedRowTypicalGroupCount> rowData;
    rowData.reserve(owningRowData.size());
    for (const auto& dataPart : owningRowData) {
        rowData.emplace_back(dataPart);
    }

    Parser_.ValidateRowDataChecksum(rowData);
    Parser_.ProcessRow(
        rowData,
        groupOffsets,
        groupIndexes,
        &RowMetadata_);

    return GetRow(memoryPool);
}

////////////////////////////////////////////////////////////////////////////////

THorizontalSchemalessVersionedBlockReader::THorizontalSchemalessVersionedBlockReader(
    const TSharedRef& block,
    const NProto::TDataBlockMeta& blockMeta,
    const std::vector<bool>& compositeColumnFlags,
    const std::vector<bool>& hunkColumnFlags,
    const std::vector<NTableClient::THunkChunkMeta>& hunkChunkMetas,
    const std::vector<NTableClient::THunkChunkRef>& hunkChunkRefs,
    const std::vector<int>& chunkToReaderIdMapping,
    TRange<ESortOrder> sortOrders,
    int commonKeyPrefix,
    TTableSchemaPtr readerSchema,
    TTimestamp timestamp)
    : THorizontalBlockReader(
        block,
        blockMeta,
        compositeColumnFlags,
        hunkColumnFlags,
        hunkChunkRefs,
        hunkChunkMetas,
        chunkToReaderIdMapping,
        sortOrders,
        commonKeyPrefix,
        /*keyWideningOptions*/ {})
    , ReaderSchema_(std::move(readerSchema))
    , Timestamp_(timestamp)
{ }

TLegacyKey THorizontalSchemalessVersionedBlockReader::GetKey() const
{
    return THorizontalBlockReader::GetLegacyKey();
}

TMutableVersionedRow THorizontalSchemalessVersionedBlockReader::GetRow(TChunkedMemoryPool* memoryPool)
{
    auto row = THorizontalBlockReader::GetVersionedRow(memoryPool, Timestamp_);

    for (auto& value : row.Values()) {
        EnsureAnyValueEncoded(
            &value,
            *ReaderSchema_,
            memoryPool,
            /*ignoreRequired*/ true);
    }

    return row;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
