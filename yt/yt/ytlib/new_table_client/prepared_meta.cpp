#include "prepared_meta.h"
#include "dispatch_by_type.h"

#include <yt/yt/ytlib/table_client/columnar_chunk_meta.h>

namespace NYT::NNewTableClient {

////////////////////////////////////////////////////////////////////////////////

using TSegmentMetas = TRange<const NProto::TSegmentMeta*>;

void TMetaBase::Init(const NProto::TSegmentMeta& meta)
{
    Offset = meta.offset();
    RowCount = meta.row_count();
    ChunkRowCount = meta.chunk_row_count();
    Type = meta.type();

    if (!IsDense(Type) && meta.HasExtension(NProto::TDenseVersionedSegmentMeta::dense_versioned_segment_meta)) {
        Type = 3;
    }
}

void TTimestampMeta::Init(const NProto::TSegmentMeta& meta)
{
    TMetaBase::Init(meta);

    const auto& timestampMeta = meta.GetExtension(NProto::TTimestampSegmentMeta::timestamp_segment_meta);
    BaseTimestamp = timestampMeta.min_timestamp();
    ExpectedDeletesPerRow = timestampMeta.expected_deletes_per_row();
    ExpectedWritesPerRow = timestampMeta.expected_writes_per_row();
}

void TIntegerMeta::Init(const NProto::TSegmentMeta& meta)
{
    TMetaBase::Init(meta);

    const auto& integerMeta = meta.GetExtension(NProto::TIntegerSegmentMeta::integer_segment_meta);
    BaseValue = integerMeta.min_value();
}

void TBlobMeta::Init(const NProto::TSegmentMeta& meta)
{
    TMetaBase::Init(meta);

    const auto& stringMeta = meta.GetExtension(NProto::TStringSegmentMeta::string_segment_meta);
    ExpectedLength = stringMeta.expected_length();
}

void TDenseMeta::Init(const NProto::TSegmentMeta& meta)
{
    bool dense = meta.HasExtension(NProto::TDenseVersionedSegmentMeta::dense_versioned_segment_meta);
    if (dense) {
        const auto& denseVersionedMeta = meta.GetExtension(NProto::TDenseVersionedSegmentMeta::dense_versioned_segment_meta);
        ExpectedPerRow = denseVersionedMeta.expected_values_per_row();
    }
}

////////////////////////////////////////////////////////////////////////////////

struct TPrepareResult
{
    std::vector<ui32> BlockIds;
    std::vector<ui32> SegmentPivots;
    TSharedRef Meta;
};

template <class TMeta>
static TPrepareResult DoPrepare(TSegmentMetas metas)
{
    auto preparedMeta = TSharedMutableRef::Allocate(sizeof(TMeta) * metas.size());
    auto* preparedMetas = reinterpret_cast<TMeta*>(preparedMeta.begin());

    std::vector<ui32> blockIds;
    std::vector<ui32> segmentPivots;

    // Prepare metas and group by block indexes.
    int lastBlockIndex = -1;
    for (ui32 index = 0; index < metas.size(); ++index) {
        auto blockIndex = metas[index]->block_index();

        if (blockIndex != lastBlockIndex) {
            blockIds.push_back(blockIndex);
            segmentPivots.push_back(index);
            lastBlockIndex = blockIndex;
        }

        preparedMetas[index].Init(*metas[index]);
    }

    segmentPivots.push_back(metas.size());

    return {blockIds, segmentPivots, preparedMeta};
}

struct TColumnInfo
{
    std::vector<ui32> SegmentPivots;
    TSharedRef Meta;

    template <EValueType Type>
    TRange<TKeyMeta<Type>> GetKeyMetas()
    {
        return reinterpret_cast<const TKeyMeta<Type>*>(Meta.begin());
    }

    template <EValueType Type>
    TRange<TValueMeta<Type>> GetValueMetas()
    {
        return reinterpret_cast<const TValueMeta<Type>*>(Meta.begin());
    }

    template <EValueType Type>
    struct TPrepareMeta
    {
        static TPrepareResult Do(TSegmentMetas metas, bool valueColumn)
        {
            if (valueColumn) {
                return DoPrepare<TValueMeta<Type>>(metas);
            } else {
                return DoPrepare<TKeyMeta<Type>>(metas);
            }
        }
    };

    std::vector<ui32> PrepareTimestampMetas(TSegmentMetas metas)
    {
        auto [blockIds, segmentPivots, preparedMeta] = DoPrepare<TTimestampMeta>(metas);

        SegmentPivots = std::move(segmentPivots);
        Meta = std::move(preparedMeta);
        return blockIds;
    }

    std::vector<ui32> PrepareMetas(TSegmentMetas metas, EValueType type, bool versioned)
    {
        auto [blockIds, segmentPivots, preparedMeta] = DispatchByDataType<TPrepareMeta>(type, metas, versioned);

        SegmentPivots = std::move(segmentPivots);
        Meta = std::move(preparedMeta);
        return blockIds;
    }
};

size_t TPreparedChunkMeta::Prepare(
    const NTableClient::TTableSchemaPtr& chunkSchema,
    const NTableClient::TRefCountedColumnMetaPtr& columnMetas)
{
    const auto& chunkSchemaColumns = chunkSchema->Columns();

    std::vector<ui16> columnIndexToGroupId(chunkSchemaColumns.size() + 1);
    {
        ui16 currentGroupId = 0;
        THashMap<TStringBuf, int> groups;

        for (int index = 0; index < std::ssize(chunkSchemaColumns); ++index) {
            const auto& group = chunkSchemaColumns[index].Group();

            ui16 groupId;
            if (group) {
                auto [it, inserted] = groups.emplace(*group, currentGroupId);
                if (inserted) {
                    ++currentGroupId;
                }
                groupId = it->second;
            } else {
                groupId = currentGroupId++;
            }

            columnIndexToGroupId[index] = groupId;
        }

        // Timestamp column has own group.
        ++currentGroupId;
        ColumnGroups.resize(currentGroupId);
    }
    std::vector<TColumnInfo> Columns;

    Columns.resize(chunkSchemaColumns.size() + 1);
    GroupIdPerColumn.resize(chunkSchemaColumns.size() + 1);
    ColumnIdInGroup.resize(chunkSchemaColumns.size() + 1);

    for (int index = 0; index < std::ssize(chunkSchemaColumns); ++index) {
        const auto& columnMeta = columnMetas->columns(index);

        auto type = chunkSchemaColumns[index].GetPhysicalType();
        bool valueColumn = index >= chunkSchema->GetKeyColumnCount();

        auto blockIds = Columns[index].PrepareMetas(MakeRange(columnMeta.segments()), type, valueColumn);

        ui16 groupId = columnIndexToGroupId[index];
        auto& blockGroup = ColumnGroups[groupId];
        // If BlockIds is empty fill it. Otherwise check that BlockIds and blockIds are equal.
        if (blockGroup.BlockIds.empty()) {
            blockGroup.BlockIds = std::move(blockIds);
            blockGroup.BlockSegmentMetas.resize(blockGroup.BlockIds.size());
        } else {
            YT_VERIFY(blockIds == blockGroup.BlockIds);
        }

        GroupIdPerColumn[index] = groupId;
        ColumnIdInGroup[index] = blockGroup.ColumnIds.size();
        blockGroup.ColumnIds.push_back(index);

        auto& column = Columns[index];
        YT_VERIFY(column.SegmentPivots.size() > 0);
        auto segmentCount = column.SegmentPivots.back();
        auto segmentSize = column.Meta.Size() / segmentCount;

        for (ui32 blockId = 0; blockId < blockGroup.BlockIds.size(); ++blockId) {
            auto offset = column.SegmentPivots[blockId] * segmentSize;
            auto offsetEnd = column.SegmentPivots[blockId + 1] * segmentSize;
            blockGroup.BlockSegmentMetas[blockId].push_back(column.Meta.Slice(offset, offsetEnd));
        }
    }

    {
        int timestampReaderIndex = columnMetas->columns().size() - 1;
        auto& blockGroup = ColumnGroups.back();
        auto& column = Columns[timestampReaderIndex];

        auto blockIds = column.PrepareTimestampMetas(
            MakeRange(columnMetas->columns(timestampReaderIndex).segments()));

        GroupIdPerColumn[timestampReaderIndex] = ColumnGroups.size() - 1;
        ColumnIdInGroup[timestampReaderIndex] = blockGroup.ColumnIds.size();
        YT_VERIFY(ColumnIdInGroup[timestampReaderIndex] == 0);
        blockGroup.BlockIds = blockIds;
        blockGroup.BlockSegmentMetas.resize(blockGroup.BlockIds.size());


        YT_VERIFY(column.SegmentPivots.size() > 0);
        auto segmentCount = column.SegmentPivots.back();
        auto segmentSize = column.Meta.Size() / segmentCount;

        for (ui32 blockId = 0; blockId < blockGroup.BlockIds.size(); ++blockId) {
            auto offset = column.SegmentPivots[blockId] * segmentSize;
            auto offsetEnd = column.SegmentPivots[blockId + 1] * segmentSize;
            blockGroup.BlockSegmentMetas[blockId].push_back(column.Meta.Slice(offset, offsetEnd));
        }
    }

    for (auto& blockGroup : ColumnGroups) {
        for (auto& blockSegmentMeta : blockGroup.BlockSegmentMetas) {
            auto columnCount = blockSegmentMeta.size();

            size_t size = 0;
            for (const auto& metas : blockSegmentMeta) {
                size += metas.size();
            }

            auto mergedMeta = TSharedMutableRef::Allocate(sizeof(ui32) * (columnCount + 1) + size);

            ui32* offsets = reinterpret_cast<ui32*>(mergedMeta.Begin());
            auto offset = sizeof(ui32) * (columnCount + 1);
            auto* metasData = reinterpret_cast<char*>(mergedMeta.Begin() + offset);

            for (const auto& metas : blockSegmentMeta) {
                *offsets++ = offset;
                std::copy(metas.begin(), metas.end(), metasData);
                offset += metas.size();
                metasData += metas.size();
            }
            *offsets++ = offset;
            blockGroup.MergedMetas.push_back(mergedMeta);
        }
    }

    size_t size = 0;
    for (const auto& column : Columns) {
        size += column.Meta.Size();
    }

    return size;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNewTableClient
