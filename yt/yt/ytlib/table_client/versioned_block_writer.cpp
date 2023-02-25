#include "versioned_block_writer.h"

#include "chunk_index_builder.h"

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/ytlib/table_client/hunks.h>

#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/misc/serialize.h>

namespace NYT::NTableClient {

using namespace NTableClient::NProto;
using namespace NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

static constexpr i64 NullValue = 0;

////////////////////////////////////////////////////////////////////////////////

//! Key layout is as follows:
/*!
 * 8 bytes for each key column + timestamp offset + value offset
 * 4 bytes for value count for each non-key column
 * 2 bytes for write timestamp count and delete timestamp count
 */
int GetSimpleVersionedBlockKeySize(int keyColumnCount, int schemaColumnCount)
{
    return 8 * (keyColumnCount + 2) + 4 * (schemaColumnCount - keyColumnCount) + 2 * 2;
}

int GetSimpleVersionedBlockPaddedKeySize(int keyColumnCount, int schemaColumnCount)
{
    return AlignUp<int>(
        GetSimpleVersionedBlockKeySize(keyColumnCount, schemaColumnCount),
        SerializationAlignment);
}

////////////////////////////////////////////////////////////////////////////////

TVersionedBlockWriterBase::TVersionedBlockWriterBase(
    TTableSchemaPtr schema,
    TMemoryUsageTrackerGuard guard)
    : Schema_(std::move(schema))
    , SchemaColumnCount_(Schema_->GetColumnCount())
    , KeyColumnCount_(Schema_->GetKeyColumnCount())
    , MemoryGuard_(std::move(guard))
    , ColumnHunkFlags_(new bool[SchemaColumnCount_])
{
    for (int id = 0; id < SchemaColumnCount_; ++id) {
        const auto& columnSchema = Schema_->Columns()[id];
        ColumnHunkFlags_[id] = columnSchema.MaxInlineHunkSize().has_value();
    }
}

void TVersionedBlockWriterBase::UpdateMinMaxTimestamp(TTimestamp timestamp)
{
    MaxTimestamp_ = std::max(MaxTimestamp_, timestamp);
    MinTimestamp_ = std::min(MinTimestamp_, timestamp);
}

TBlock TVersionedBlockWriterBase::FlushBlock(
    std::vector<TSharedRef> blockParts,
    TDataBlockMeta meta)
{
    auto size = GetByteSize(blockParts);

    meta.set_row_count(RowCount_);
    meta.set_uncompressed_size(size);

    TBlock block;
    block.Data.swap(blockParts);
    block.Meta.Swap(&meta);

    if (MemoryGuard_) {
        MemoryGuard_.SetSize(0);
    }

    return block;
}

bool TVersionedBlockWriterBase::IsInlineHunkValue(const TUnversionedValue& value) const
{
    return ColumnHunkFlags_[value.Id] && None(value.Flags & EValueFlags::Hunk);
}

////////////////////////////////////////////////////////////////////////////////

TSimpleVersionedBlockWriter::TSimpleVersionedBlockWriter(
    TTableSchemaPtr schema,
    TMemoryUsageTrackerGuard guard)
    : TVersionedBlockWriterBase(
        std::move(schema),
        std::move(guard))
{
    if (Schema_->HasAggregateColumns()) {
        ValueAggregateFlags_.emplace();
    }
}

void TSimpleVersionedBlockWriter::WriteRow(TVersionedRow row)
{
    ++RowCount_;

    std::optional<TBitmapOutput> nullAggregateFlags;
    int keyOffset = KeyStream_.GetSize();
    for (const auto* it = row.BeginKeys(); it != row.EndKeys(); ++it) {
        const auto& value = *it;
        YT_ASSERT(value.Type == EValueType::Null || value.Type == Schema_->Columns()[value.Id].GetWireType());
        WriteValue(KeyStream_, KeyNullFlags_, nullAggregateFlags, value);
    }

    WritePod(KeyStream_, TimestampCount_);
    WritePod(KeyStream_, ValueCount_);
    WritePod(KeyStream_, static_cast<ui16>(row.GetWriteTimestampCount()));
    WritePod(KeyStream_, static_cast<ui16>(row.GetDeleteTimestampCount()));

    TimestampCount_ += row.GetWriteTimestampCount();
    for (const auto* it = row.BeginWriteTimestamps(); it != row.EndWriteTimestamps(); ++it) {
        auto timestamp = *it;
        WritePod(TimestampStream_, timestamp);
        UpdateMinMaxTimestamp(timestamp);
    }

    TimestampCount_ += row.GetDeleteTimestampCount();
    for (const auto* it = row.BeginDeleteTimestamps(); it != row.EndDeleteTimestamps(); ++it) {
        auto timestamp = *it;
        WritePod(TimestampStream_, timestamp);
        UpdateMinMaxTimestamp(timestamp);
    }

    ValueCount_ += row.GetValueCount();

    int lastId = KeyColumnCount_;
    ui32 valueCount = 0;
    while (static_cast<int>(valueCount) < row.GetValueCount()) {
        const auto& value = row.BeginValues()[valueCount];
        YT_ASSERT(value.Type == EValueType::Null || value.Type == Schema_->Columns()[value.Id].GetWireType());
        YT_ASSERT(lastId <= value.Id);
        if (lastId < value.Id) {
            WritePod(KeyStream_, valueCount);
            ++lastId;
        } else {
            WriteValue(ValueStream_, ValueNullFlags_, ValueAggregateFlags_, value);
            WritePod(ValueStream_, value.Timestamp);
            ++valueCount;
        }
    }

    while (lastId < SchemaColumnCount_) {
        WritePod(KeyStream_, valueCount);
        ++lastId;
    }

    YT_ASSERT(static_cast<int>(KeyStream_.GetSize() - keyOffset) ==
        GetSimpleVersionedBlockKeySize(KeyColumnCount_, SchemaColumnCount_));
    WritePadding(KeyStream_, GetSimpleVersionedBlockKeySize(KeyColumnCount_, SchemaColumnCount_));

    if (MemoryGuard_) {
        MemoryGuard_.SetSize(GetBlockSize());
    }
}

TBlock TSimpleVersionedBlockWriter::FlushBlock()
{
    std::vector<TSharedRef> blockParts;
    auto keys = KeyStream_.Finish();
    blockParts.insert(blockParts.end(), keys.begin(), keys.end());

    auto values = ValueStream_.Finish();
    blockParts.insert(blockParts.end(), values.begin(), values.end());

    auto timestamps = TimestampStream_.Finish();
    blockParts.insert(blockParts.end(), timestamps.begin(), timestamps.end());

    blockParts.insert(blockParts.end(), KeyNullFlags_.Flush<TSimpleVersionedBlockWriterTag>());
    blockParts.insert(blockParts.end(), ValueNullFlags_.Flush<TSimpleVersionedBlockWriterTag>());
    if (ValueAggregateFlags_) {
        blockParts.insert(blockParts.end(), ValueAggregateFlags_->Flush<TSimpleVersionedBlockWriterTag>());
    }

    auto strings = StringDataStream_.Finish();
    blockParts.insert(blockParts.end(), strings.begin(), strings.end());

    TDataBlockMeta meta;
    auto* metaExt = meta.MutableExtension(TSimpleVersionedBlockMeta::block_meta_ext);
    metaExt->set_value_count(ValueCount_);
    metaExt->set_timestamp_count(TimestampCount_);

    return TVersionedBlockWriterBase::FlushBlock(std::move(blockParts), std::move(meta));
}

void TSimpleVersionedBlockWriter::WriteValue(
    TChunkedOutputStream& stream,
    TBitmapOutput& nullFlags,
    std::optional<TBitmapOutput>& aggregateFlags,
    const TUnversionedValue& value)
{
    if (aggregateFlags) {
        aggregateFlags->Append(Any(value.Flags & EValueFlags::Aggregate));
    }

    switch (value.Type) {
        case EValueType::Int64:
            WritePod(stream, value.Data.Int64);
            nullFlags.Append(false);
            break;

        case EValueType::Uint64:
            WritePod(stream, value.Data.Uint64);
            nullFlags.Append(false);
            break;

        case EValueType::Double:
            WritePod(stream, value.Data.Double);
            nullFlags.Append(false);
            break;

        case EValueType::Boolean:
            // NB(psushin): all values in simple versioned block must be 64-bits.
            WritePod(stream, static_cast<ui64>(value.Data.Boolean));
            nullFlags.Append(false);
            break;

        case EValueType::String:
        case EValueType::Any:
        case EValueType::Composite:
            WritePod(stream, static_cast<ui32>(StringDataStream_.GetSize()));
            if (IsInlineHunkValue(value)) {
                WritePod(stream, value.Length + 1);
                StringDataStream_.Write(static_cast<char>(EHunkValueTag::Inline));
                StringDataStream_.Write(value.Data.String, value.Length);
            } else {
                WritePod(stream, value.Length);
                StringDataStream_.Write(value.Data.String, value.Length);
            }
            nullFlags.Append(false);
            break;

        case EValueType::Null:
            WritePod(stream, NullValue);
            nullFlags.Append(true);
            break;

        case EValueType::Min:
        case EValueType::Max:
        case EValueType::TheBottom:
            YT_ABORT();
    }
}

i64 TSimpleVersionedBlockWriter::GetBlockSize() const
{
    return
        KeyStream_.GetSize() +
        ValueStream_.GetSize() +
        TimestampStream_.GetSize() +
        StringDataStream_.GetSize() +
        KeyNullFlags_.GetByteSize() +
        ValueNullFlags_.GetByteSize() +
        (ValueAggregateFlags_.operator bool() ? ValueAggregateFlags_->GetByteSize() : 0);
}

////////////////////////////////////////////////////////////////////////////////

TIndexedVersionedBlockWriter::TIndexedVersionedBlockWriter(
    TTableSchemaPtr schema,
    int blockIndex,
    const TIndexedVersionedBlockFormatDetail& blockFormatDetail,
    IChunkIndexBuilderPtr chunkIndexBuilder,
    TMemoryUsageTrackerGuard guard)
    : TVersionedBlockWriterBase(
        std::move(schema),
        std::move(guard))
    , BlockFormatDetail_(blockFormatDetail)
    , ChunkIndexBuilder_(std::move(chunkIndexBuilder))
    , BlockIndex_(blockIndex)
    , GroupCount_(BlockFormatDetail_.GetGroupCount())
    , HasAggregateColumns_(Schema_->HasAggregateColumns())
    , SectorAlignmentSize_(THashTableChunkIndexFormatDetail::SectorSize)
    // TODO(akozhikhov).
    , EnableGroupReordering_(false)
{
    YT_VERIFY(AlignUpSpace<i64>(SectorAlignmentSize_, SerializationAlignment) == 0);
    YT_VERIFY(GroupCount_ > 0);

    RowData_.ValueData.Groups.resize(GroupCount_);
}

void TIndexedVersionedBlockWriter::WriteRow(TVersionedRow row)
{
    ++RowCount_;

    ResetRowData(row);

    auto rowByteSize = GetRowByteSize();
    VerifySerializationAligned(rowByteSize);
    VerifySerializationAligned(Stream_.GetSize());

    char* buffer = Stream_.Preallocate(rowByteSize);
    EncodeRow(buffer);

    auto rowChecksum = GetChecksum(TRef(buffer, rowByteSize));

    Stream_.Advance(rowByteSize);

    WritePod(Stream_, rowChecksum);

    TSharedRange<int> groupOffsets;
    TSharedRange<int> groupIndexes;
    if (GroupCount_ > 1) {
        groupOffsets = MakeSharedRange(std::vector<int>(
            GroupOffsets_.end() - GroupCount_,
            GroupOffsets_.end()));
        if (EnableGroupReordering_) {
            groupIndexes = MakeSharedRange(std::vector<int>(
                GroupIndexes_.end() - GroupCount_,
                GroupIndexes_.end()));
        }
    }

    rowByteSize += sizeof(TChecksum);
    ChunkIndexBuilder_->ProcessRow({
        .Row = RowData_.Row,
        .BlockIndex = BlockIndex_,
        .RowOffset = RowOffsets_.back(),
        .RowLength = rowByteSize,
        .GroupOffsets = std::move(groupOffsets),
        .GroupIndexes = std::move(groupIndexes)
    });

    if (MemoryGuard_) {
        MemoryGuard_.SetSize(GetBlockSize());
    }
}

TBlock TIndexedVersionedBlockWriter::FlushBlock()
{
    VerifySerializationAligned(Stream_.GetSize());

    auto blockSize = GetUnalignedBlockSize();

    // NB: We enforce sector alignment of blocks to simplify reasoning about sector alignment of column groups.
    // Padding is added before row offsets and group offsets so block reader may easily access them.
    WriteZeroes(Stream_, AlignUpSpace<i64>(blockSize, SectorAlignmentSize_));

    for (auto rowOffset : RowOffsets_) {
        WritePod(Stream_, rowOffset);
    }
    for (auto groupOffset : GroupOffsets_) {
        WritePod(Stream_, groupOffset);
    }
    for (auto groupIndex : GroupIndexes_) {
        WritePod(Stream_, groupIndex);
    }

    VerifySerializationAligned(Stream_.GetSize());

    return TVersionedBlockWriterBase::FlushBlock(Stream_.Finish(), TDataBlockMeta());
}

i64 TIndexedVersionedBlockWriter::GetUnalignedBlockSize() const
{
    return
        Stream_.GetSize() +
        RowOffsets_.size() * sizeof(RowOffsets_[0]) +
        GroupOffsets_.size() * sizeof(GroupOffsets_[0]) +
        GroupIndexes_.size() * sizeof(GroupIndexes_[0]);
}

i64 TIndexedVersionedBlockWriter::GetBlockSize() const
{
    return AlignUp<i64>(GetUnalignedBlockSize(), SectorAlignmentSize_);
}

int TIndexedVersionedBlockWriter::GetBlockFormatVersion()
{
    return 1;
}

void TIndexedVersionedBlockWriter::ResetRowData(TVersionedRow row)
{
    RowData_.Row = row;
    RowData_.RowSectorAlignmentSize = 0;

    ResetKeyData();
    ResetValueData();

    RowData_.ValueData.GroupOrder = ComputeGroupAlignmentAndReordering();

    if (RowData_.RowSectorAlignmentSize != 0) {
        WriteZeroes(Stream_, RowData_.RowSectorAlignmentSize);
    }

    RowOffsets_.push_back(Stream_.GetSize());

    if (GroupCount_ == 1) {
        return;
    }

    auto groupIndexesStart = GroupIndexes_.size();
    if (EnableGroupReordering_) {
        GroupIndexes_.resize(GroupIndexes_.size() + GroupCount_);
    }

    auto groupOffset = GetRowMetadataByteSize();
    for (int physicalGroupIndex = 0; physicalGroupIndex < GroupCount_; ++physicalGroupIndex) {
        GroupOffsets_.push_back(groupOffset);

        auto logicalGroupIndex = RowData_.ValueData.GroupOrder[physicalGroupIndex];
        groupOffset += GetValueGroupDataByteSize(
            logicalGroupIndex,
            /*includeSectorAlignment*/ true);
        if (EnableGroupReordering_) {
            GroupIndexes_[groupIndexesStart + logicalGroupIndex] = physicalGroupIndex;
        }
    }
}

void TIndexedVersionedBlockWriter::ResetKeyData()
{
    auto& stringDataSize = RowData_.KeyData.StringDataSize;

    stringDataSize = 0;
    for (const auto* it = RowData_.Row.BeginKeys(); it != RowData_.Row.EndKeys(); ++it) {
        const auto& value = *it;
        YT_ASSERT(value.Type == EValueType::Null || value.Type == Schema_->Columns()[value.Id].GetWireType());

        if (IsStringLikeType(value.Type)) {
            YT_VERIFY(!IsInlineHunkValue(value));
            stringDataSize += value.Length;
        }
    }
}

void TIndexedVersionedBlockWriter::ResetValueData()
{
    auto row = RowData_.Row;
    auto& groups = RowData_.ValueData.Groups;

    for (auto& group : groups) {
        group.ValueCount = 0;
        group.StringDataSize = 0;
        // FIXME(akozhikhov): reserve.
        group.ColumnValueRanges.clear();
        group.SectorAlignmentSize = 0;
    }

    int currentId = KeyColumnCount_;
    int currentIdBeginIndex = 0;
    int groupId = BlockFormatDetail_.GetValueColumnInfo(currentId).GroupIndex;

    auto fillValueRange = [&] (int valueIndex) {
        auto& group = groups[groupId];
        group.ValueCount += valueIndex - currentIdBeginIndex;
        group.ColumnValueRanges.emplace_back(currentIdBeginIndex, valueIndex);

        currentIdBeginIndex = valueIndex;
        ++currentId;
        if (currentId != SchemaColumnCount_) {
            groupId = BlockFormatDetail_.GetValueColumnInfo(currentId).GroupIndex;
        }
    };

    for (int valueIndex = 0; valueIndex < row.GetValueCount(); ++valueIndex) {
        const auto& value = row.BeginValues()[valueIndex];

        YT_ASSERT(value.Type == EValueType::Null || value.Type == Schema_->Columns()[value.Id].GetWireType());
        YT_ASSERT(currentId <= value.Id);

        while (currentId < value.Id) {
            fillValueRange(valueIndex);
        }

        if (IsStringLikeType(value.Type)) {
            groups[groupId].StringDataSize += IsInlineHunkValue(value) ? 1 : 0;
            groups[groupId].StringDataSize += value.Length;
        }
    }

    while (currentId < SchemaColumnCount_) {
        fillValueRange(row.GetValueCount());
    }
}

i64 TIndexedVersionedBlockWriter::GetRowByteSize() const
{
    return
        GetRowMetadataByteSize() +
        GetValueDataByteSize();
}

i64 TIndexedVersionedBlockWriter::GetRowMetadataByteSize() const
{
    return
        GetKeyDataByteSize() +
        GetTimestampDataByteSize() +
        (GroupCount_ > 1 ? sizeof(TChecksum) : 0);
}

i64 TIndexedVersionedBlockWriter::GetKeyDataByteSize() const
{
    auto result =
        sizeof(i64) * KeyColumnCount_ +
        NBitmapDetail::GetByteSize(KeyColumnCount_) +
        RowData_.KeyData.StringDataSize;
    result = AlignUp<i64>(result, SerializationAlignment);

    AssertSerializationAligned(result);

    return result;
}

i64 TIndexedVersionedBlockWriter::GetTimestampDataByteSize() const
{
    int writeTimestampCount = RowData_.Row.GetWriteTimestampCount();
    int deleteTimestampCount = RowData_.Row.GetDeleteTimestampCount();

    auto result =
        sizeof(writeTimestampCount) +
        sizeof(deleteTimestampCount) +
        sizeof(TTimestamp) * (writeTimestampCount + deleteTimestampCount);

    AssertSerializationAligned(result);

    return result;
}

i64 TIndexedVersionedBlockWriter::GetValueDataByteSize() const
{
    i64 result = 0;
    for (int groupIndex = 0; groupIndex < GroupCount_; ++groupIndex) {
        result += GetValueGroupDataByteSize(groupIndex, /*includeSectorAlignment*/ true);
    }

    AssertSerializationAligned(result);

    return result;
}

i64 TIndexedVersionedBlockWriter::GetValueGroupDataByteSize(
    int groupIndex,
    bool includeSectorAlignment) const
{
    const auto& group = RowData_.ValueData.Groups[groupIndex];

    auto result =
        sizeof(group.ValueCount) +
        sizeof(i32) * std::ssize(group.ColumnValueRanges) +
        NBitmapDetail::GetByteSize(group.ValueCount) +
        (HasAggregateColumns_ ? NBitmapDetail::GetByteSize(group.ValueCount) : 0);
    result = AlignUp<i64>(result, SerializationAlignment);

    result += 2 * sizeof(i64) * group.ValueCount;
    result += AlignUp<i64>(group.StringDataSize, SerializationAlignment);

    result += includeSectorAlignment ? group.SectorAlignmentSize : 0;
    result += GroupCount_ > 1 ? sizeof(TChecksum) : 0;

    AssertSerializationAligned(result);

    return result;
}

void TIndexedVersionedBlockWriter::EncodeRow(char* buffer)
{
    buffer = EncodeRowMetadata(buffer);
    EncodeValueData(buffer);
}

char* TIndexedVersionedBlockWriter::EncodeRowMetadata(char* buffer)
{
    auto* bufferStart = buffer;

    buffer = EncodeKeyData(buffer);
    buffer = EncodeTimestampData(buffer);

    if (GroupCount_ > 1) {
        WriteChecksum(buffer, buffer - bufferStart);
    }

    YT_ASSERT(buffer - bufferStart == GetRowMetadataByteSize());

    return buffer;
}

char* TIndexedVersionedBlockWriter::EncodeKeyData(char* buffer) const
{
    auto* bufferStart = buffer;

    auto* valuesBuffer = buffer;
    buffer += sizeof(i64) * KeyColumnCount_;

    TMutableBitmap nullFlagsBitmap(buffer);
    buffer += NBitmapDetail::GetByteSize(KeyColumnCount_);

    auto* stringDataBuffer = buffer;
    std::optional<TMutableBitmap> nullAggregateFlagsBitmap;
    for (int keyColumnIndex = 0; keyColumnIndex < KeyColumnCount_; ++keyColumnIndex) {
        const auto& value = RowData_.Row.BeginKeys()[keyColumnIndex];
        DoWriteValue(
            valuesBuffer,
            stringDataBuffer,
            keyColumnIndex,
            value,
            &nullFlagsBitmap,
            &nullAggregateFlagsBitmap);
    }

    WritePadding(stringDataBuffer, stringDataBuffer - bufferStart);

    YT_ASSERT(stringDataBuffer - bufferStart == GetKeyDataByteSize());

    return stringDataBuffer;
}

char* TIndexedVersionedBlockWriter::EncodeTimestampData(char* buffer)
{
    auto* bufferStart = buffer;

    auto row = RowData_.Row;

    int writeTimestampCount = RowData_.Row.GetWriteTimestampCount();
    int deleteTimestampCount = RowData_.Row.GetDeleteTimestampCount();

    WritePod(buffer, writeTimestampCount);
    WritePod(buffer, deleteTimestampCount);

    auto writeTimestamps = [&] (const TTimestamp* begin, const TTimestamp* end) {
        for (const auto* it = begin; it != end; ++it) {
            TTimestamp timestamp = *it;
            WritePod(buffer, timestamp);
            MaxTimestamp_ = std::max(MaxTimestamp_, timestamp);
            MinTimestamp_ = std::min(MinTimestamp_, timestamp);
        }
    };
    writeTimestamps(row.BeginWriteTimestamps(), row.EndWriteTimestamps());
    writeTimestamps(row.BeginDeleteTimestamps(), row.EndDeleteTimestamps());

    YT_ASSERT(buffer - bufferStart == GetTimestampDataByteSize());

    return buffer;
}

char* TIndexedVersionedBlockWriter::EncodeValueData(char* buffer)
{
    auto* bufferStart = buffer;

    YT_VERIFY(RowData_.ValueData.Groups[RowData_.ValueData.GroupOrder.back()].SectorAlignmentSize == 0);

    for (auto groupIndex : RowData_.ValueData.GroupOrder) {
        auto* groupBufferStart = buffer;
        buffer = EncodeValueGroupData(buffer, groupIndex);

        if (GroupCount_ > 1) {
            WriteZeroes(buffer, RowData_.ValueData.Groups[groupIndex].SectorAlignmentSize);
            WriteChecksum(buffer, buffer - groupBufferStart);
        }

        VerifySerializationAligned(buffer - bufferStart);

        YT_ASSERT(buffer - groupBufferStart == GetValueGroupDataByteSize(groupIndex, true));
    }

    YT_ASSERT(buffer - bufferStart == GetValueDataByteSize());

    return buffer;
}

char* TIndexedVersionedBlockWriter::EncodeValueGroupData(char* buffer, int groupIndex) const
{
    auto* bufferStart = buffer;

    auto row = RowData_.Row;
    const auto& group = RowData_.ValueData.Groups[groupIndex];

    WritePod(buffer, group.ValueCount);

    char* columnOffsetsBuffer = buffer;
    buffer += sizeof(i32) * std::ssize(group.ColumnValueRanges);

    TMutableBitmap nullFlagsBitmap(buffer);
    buffer += NBitmapDetail::GetByteSize(group.ValueCount);

    std::optional<TMutableBitmap> aggregateFlagsBitmap;
    if (HasAggregateColumns_) {
        aggregateFlagsBitmap.emplace(buffer);
        buffer += NBitmapDetail::GetByteSize(group.ValueCount);
    }

    WritePadding(buffer, buffer - bufferStart);

    auto* stringDataBuffer = buffer + 2 * sizeof(i64) * group.ValueCount;

    int valueIndexInGroup = 0;
    for (auto [beginIndex, endIndex] : group.ColumnValueRanges) {
        WritePod(columnOffsetsBuffer, valueIndexInGroup);

        for (int index = beginIndex; index < endIndex; ++index) {
            const auto& value = row.BeginValues()[index];
            DoWriteValue(
                buffer,
                stringDataBuffer,
                valueIndexInGroup,
                value,
                &nullFlagsBitmap,
                &aggregateFlagsBitmap);
            WritePod(buffer, value.Timestamp);

            ++valueIndexInGroup;
        }
    }

    YT_VERIFY(valueIndexInGroup == group.ValueCount);

    WritePadding(stringDataBuffer, group.StringDataSize);

    return stringDataBuffer;
}

TCompactVector<int, IndexedRowTypicalGroupCount>
TIndexedVersionedBlockWriter::ComputeGroupAlignmentAndReordering()
{
    /*
        The purpose of this method is to compute row alignment and group alignment to achieve better disk utilization
        and optionally reorder groups to diminish space amplification.

        We try to align up to sector size, where sector size is determined via chunk index builder
        and corresponds to page size.
        Alignment is performed if it reduces number of sectors to be accessed
        upon reading of the whole row or dedicated groups from disk.

        Having a single group we only perform row alignment and the group goes right after row metadata.

        With multiple groups each group may come with sector alignment.
        Note that row metadata and the first group cannot be separated by alignment
        as we always read row metadata so it makes no sence to align the first group.

        If group reordering is enabled we perform best-fit-decreasing algorithm to pack groups in bins, where
        bin capacity equals to sector alignment and group size is its actual size modulo sector alignment.
        Sector alignment is applied to a single group within each bin
        and finally group ordering is formed putting each bin's groups one by one.
        First group (that is attached to row metadata) is chosen in some greedy fashion.

        With multiple groups chunk index is provided with group offsets to allow accessing them from disk separately.
        If reordering is enabled chunk index is also provided with mapping from logical to physical indexation.
    */

    auto bufferSize = Stream_.GetSize();
    auto rowAlignUpSpace = AlignUpSpace<i64>(bufferSize, SectorAlignmentSize_);
    auto rowMetadataSize = GetRowMetadataByteSize();

    if (GroupCount_ == 1) {
        auto rowSize =
            rowMetadataSize +
            GetValueGroupDataByteSize(0, /*includeSectorAlignment*/ false);
        if (rowSize % SectorAlignmentSize_ > rowAlignUpSpace) {
            RowData_.RowSectorAlignmentSize = rowAlignUpSpace;
        }

        return {0};
    }

    auto shouldAlignRowMetadata = rowMetadataSize % SectorAlignmentSize_ > rowAlignUpSpace;
    if (shouldAlignRowMetadata) {
        RowData_.RowSectorAlignmentSize = rowAlignUpSpace;
        bufferSize += rowAlignUpSpace;
    }

    bufferSize += rowMetadataSize;

    if (!EnableGroupReordering_) {
        bufferSize += GetValueGroupDataByteSize(0, /*includeSectorAlignment*/ false);
        for (int groupIndex = 1; groupIndex < GroupCount_; ++groupIndex) {
            auto groupSize = GetValueGroupDataByteSize(groupIndex, /*includeSectorAlignment*/ false);
            auto alignUpSpace = AlignUpSpace<i64>(bufferSize, SectorAlignmentSize_);
            if (groupSize % SectorAlignmentSize_ > alignUpSpace) {
                RowData_.ValueData.Groups[groupIndex - 1].SectorAlignmentSize = alignUpSpace;
                bufferSize += alignUpSpace;
            }

            bufferSize += groupSize;
        }

        TCompactVector<int, IndexedRowTypicalGroupCount> groupIndexes(GroupCount_);
        std::iota(groupIndexes.begin(), groupIndexes.end(), 0);
        return groupIndexes;
    }

    std::vector<std::pair<i64, int>> groupBinSizes;
    for (int groupIndex = 0; groupIndex < GroupCount_; ++groupIndex) {
        groupBinSizes.emplace_back(
            GetValueGroupDataByteSize(groupIndex, /*includeSectorAlignment*/ false) % SectorAlignmentSize_,
            groupIndex);
    }

    std::sort(groupBinSizes.begin(), groupBinSizes.end(), std::greater());

    // Find the first group to fit right after metadata. Otherwise, pick the largest one.
    auto firstGroupIt = std::lower_bound(
        groupBinSizes.begin(),
        groupBinSizes.end(),
        AlignUpSpace<i64>(bufferSize, SectorAlignmentSize_),
        [] (const std::pair<i64, int>& group, i64 binSize) {
            return group.first > binSize;
        });
    int firstGroupIndex = firstGroupIt == groupBinSizes.end()
        ? 0
        : std::distance(groupBinSizes.begin(), firstGroupIt);

    bufferSize += GetValueGroupDataByteSize(firstGroupIndex, /*includeSectorAlignment*/ false);

    std::multimap<i64, std::vector<int>> binSpaceToGroupIndexes;
    const auto& firstBin = *binSpaceToGroupIndexes.insert({
        AlignUpSpace<i64>(bufferSize, SectorAlignmentSize_),
        {firstGroupIndex}
    });

    for (auto [groupBinSize, groupIndex] : groupBinSizes) {
        if (firstGroupIndex == groupIndex) {
            continue;
        }

        auto it = binSpaceToGroupIndexes.lower_bound(groupBinSize);
        if (it == binSpaceToGroupIndexes.end()) {
            it = binSpaceToGroupIndexes.insert({SectorAlignmentSize_, {}});
        }

        auto node = binSpaceToGroupIndexes.extract(it);
        node.key() -= groupBinSize;
        node.mapped().push_back(groupIndex);
        binSpaceToGroupIndexes.insert(std::move(node));
    }

    TCompactVector<int, IndexedRowTypicalGroupCount> reorderedGroupIndexes;
    reorderedGroupIndexes.reserve(GroupCount_);

    reorderedGroupIndexes.insert(
        reorderedGroupIndexes.end(),
        firstBin.second.begin(),
        firstBin.second.end());

    for (auto it = binSpaceToGroupIndexes.begin(); it != binSpaceToGroupIndexes.end(); ++it) {
        YT_VERIFY(it->first < SectorAlignmentSize_);
        RowData_.ValueData.Groups[it->second.back()].SectorAlignmentSize = it->first;

        if (it->second[0] == firstGroupIndex) {
            continue;
        }

        reorderedGroupIndexes.insert(
            reorderedGroupIndexes.end(),
            it->second.begin(),
            it->second.end());
    }

    RowData_.ValueData.Groups[reorderedGroupIndexes.back()].SectorAlignmentSize = 0;

    return reorderedGroupIndexes;
}

void TIndexedVersionedBlockWriter::DoWriteValue(
    char*& buffer,
    char*& stringBuffer,
    int valueIndex,
    const TUnversionedValue& value,
    TMutableBitmap* nullFlagsBitmap,
    std::optional<TMutableBitmap>* aggregateFlagsBitmap) const
{
    if (aggregateFlagsBitmap->has_value()) {
        (*aggregateFlagsBitmap)->Set(valueIndex, Any(value.Flags & EValueFlags::Aggregate));
    }

    switch (value.Type) {
        case EValueType::Int64:
            WritePod(buffer, value.Data.Int64);
            nullFlagsBitmap->Set(valueIndex, false);
            break;
        case EValueType::Uint64:
            WritePod(buffer, value.Data.Uint64);
            nullFlagsBitmap->Set(valueIndex, false);
            break;
        case EValueType::Double:
            WritePod(buffer, value.Data.Double);
            nullFlagsBitmap->Set(valueIndex, false);
            break;

        case EValueType::Boolean: {
            // NB(psushin): all values in simple versioned block must be 64-bits.
            ui64 castValue = static_cast<ui64>(value.Data.Boolean);
            WritePod(buffer, castValue);
            nullFlagsBitmap->Set(valueIndex, false);
            break;
        }

        case EValueType::String:
        case EValueType::Any:
        case EValueType::Composite: {
            ui32 offset = static_cast<ui32>(stringBuffer - buffer);
            WritePod(buffer, offset);

            if (IsInlineHunkValue(value)) {
                auto hunkTag = static_cast<char>(EHunkValueTag::Inline);
                ui32 lengthWithTag = value.Length + 1;
                WritePod(buffer, lengthWithTag);
                WritePod(stringBuffer, hunkTag);
                WriteRef(stringBuffer, TRef(value.Data.String, value.Length));
            } else {
                WritePod(buffer, value.Length);
                WriteRef(stringBuffer, TRef(value.Data.String, value.Length));
            }

            nullFlagsBitmap->Set(valueIndex, false);

            break;
        }

        case EValueType::Null:
            WritePod(buffer, NullValue);
            nullFlagsBitmap->Set(valueIndex, true);
            break;

        default:
            YT_ABORT();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
