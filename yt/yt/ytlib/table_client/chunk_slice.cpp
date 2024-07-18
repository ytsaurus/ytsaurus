#include "chunk_slice.h"

#include "private.h"
#include "chunk_meta_extensions.h"
#include "key_set.h"

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>

#include <yt/yt/client/table_client/comparator.h>
#include <yt/yt/client/table_client/key_bound.h>
#include <yt/yt/client/table_client/schema.h>
#include <yt/yt/client/table_client/name_table.h>

#include <yt/yt/library/erasure/impl/codec.h>

#include <yt/yt/core/concurrency/periodic_yielder.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/misc/numeric_helpers.h>
#include <yt/yt/core/misc/protobuf_helpers.h>

#include <cmath>

namespace NYT::NTableClient {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NTableClient::NProto;

using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto SlicerYieldPeriod = TDuration::MilliSeconds(10);

////////////////////////////////////////////////////////////////////////////////

void FormatValue(TStringBuilderBase* builder, const TChunkSlice& slice, TStringBuf /*spec*/)
{
    builder->AppendFormat("LowerLimit: %v, UpperLimit: %v, RowCount: %v, DataWeight: %v",
        slice.LowerLimit,
        slice.UpperLimit,
        slice.RowCount,
        slice.DataWeight);
}

////////////////////////////////////////////////////////////////////////////////

class TSortedChunkSlicer
    : private TNonCopyable
{
public:
    TSortedChunkSlicer(
        const NChunkClient::NProto::TSliceRequest& sliceReq,
        const NChunkClient::NProto::TChunkMeta& meta)
        : SliceReq_(sliceReq)
        , Meta_(meta)
    {
        auto chunkFormat = CheckedEnumCast<EChunkFormat>(Meta_.format());
        switch (chunkFormat) {
            case EChunkFormat::TableUnversionedSchemalessHorizontal:
            case EChunkFormat::TableUnversionedColumnar:
            case EChunkFormat::TableVersionedSimple:
            case EChunkFormat::TableVersionedSlim:
            case EChunkFormat::TableVersionedIndexed:
            case EChunkFormat::TableVersionedColumnar:
                break;
            default:
                auto chunkId = FromProto<TChunkId>(SliceReq_.chunk_id());
                THROW_ERROR_EXCEPTION("Unsupported format %Qlv for chunk %v",
                    chunkFormat,
                    chunkId);
        }

        TComparator chunkComparator;
        if (auto schemaExt = FindProtoExtension<TTableSchemaExt>(Meta_.extensions())) {
            chunkComparator = FromProto<TTableSchema>(*schemaExt).ToComparator();
        } else {
            // NB(gritukan): Very old chunks do not have schema, but they are always sorted in ascending order.
            auto keyColumnsExt = GetProtoExtension<TKeyColumnsExt>(Meta_.extensions());
            int keyColumnCount = keyColumnsExt.names_size();
            chunkComparator = TComparator(std::vector<ESortOrder>(keyColumnCount, ESortOrder::Ascending));
        }

        int keyColumnCount = std::min<int>(chunkComparator.GetLength(), SliceReq_.key_column_count());
        SliceComparator_ = chunkComparator.Trim(keyColumnCount);

        YT_VERIFY(FindBoundaryKeyBounds(Meta_, &ChunkLowerBound_, &ChunkUpperBound_));
        ChunkLowerBound_ = ShortenKeyBound(ChunkLowerBound_, SliceComparator_.GetLength());
        ChunkUpperBound_ = ShortenKeyBound(ChunkUpperBound_, SliceComparator_.GetLength());

        auto miscExt = GetProtoExtension<NChunkClient::NProto::TMiscExt>(Meta_.extensions());
        i64 chunkDataWeight = miscExt.has_data_weight()
            ? miscExt.data_weight()
            : miscExt.uncompressed_data_size();

        i64 chunkRowCount = miscExt.row_count();
        YT_VERIFY(chunkRowCount > 0);

        DataWeightPerRow_ = std::max((i64)1, chunkDataWeight / chunkRowCount);

        auto blockMetaExt = GetProtoExtension<TDataBlockMetaExt>(Meta_.extensions());
        auto blockCount = blockMetaExt.data_blocks_size();
        BlockDescriptors_.reserve(blockCount);

        TPeriodicYielder yielder(SlicerYieldPeriod);

        for (int blockIndex = 0; blockIndex < blockCount; ++blockIndex) {
            yielder.TryYield();

            const auto& block = blockMetaExt.data_blocks(blockIndex);
            YT_VERIFY(block.block_index() == blockIndex);

            auto blockLastKey = FromProto<TUnversionedOwningRow>(block.last_key());
            TUnversionedOwningRow trimmedBlockLastKey(blockLastKey.FirstNElements(SliceComparator_.GetLength()));
            auto blockUpperBound = TOwningKeyBound::FromRow(
                /*row*/ std::move(trimmedBlockLastKey),
                /*isInclusive*/ true,
                /*isUpper*/ true);

            i64 chunkRowCount = block.chunk_row_count();
            i64 rowCount = BlockDescriptors_.empty()
                ? chunkRowCount
                : chunkRowCount - BlockDescriptors_.back().RowCount;

            TBlockDescriptor blockDescriptor{
                .UpperBound = blockUpperBound,
                .RowCount = rowCount,
                .ChunkRowCount = chunkRowCount,
            };
            BlockDescriptors_.push_back(std::move(blockDescriptor));
        }

        TReadLimit sliceLowerLimit(SliceReq_.lower_limit(), /*isUpper*/ false, SliceReq_.key_column_count());
        TReadLimit sliceUpperLimit(SliceReq_.upper_limit(), /*isUpper*/ true, SliceReq_.key_column_count());
        sliceLowerLimit.KeyBound() = ShortenKeyBound(sliceLowerLimit.KeyBound(), keyColumnCount);
        sliceUpperLimit.KeyBound() = ShortenKeyBound(sliceUpperLimit.KeyBound(), keyColumnCount);

        if (sliceLowerLimit.KeyBound()) {
            SliceLowerBound_ = sliceLowerLimit.KeyBound();
        } else {
            SliceLowerBound_ = ChunkLowerBound_;
        }

        if (sliceUpperLimit.KeyBound()) {
            SliceUpperBound_ = sliceUpperLimit.KeyBound();
        } else {
            SliceUpperBound_ = ChunkUpperBound_;
        }

        SliceStartRowIndex_ = sliceLowerLimit.GetRowIndex().value_or(0);
        SliceEndRowIndex_ = sliceUpperLimit.GetRowIndex().value_or(chunkRowCount);
    }

    void Clear()
    {
        TPeriodicYielder yielder(SlicerYieldPeriod);

        while (!BlockDescriptors_.empty()) {
            BlockDescriptors_.pop_back();
            yielder.TryYield();
        }
    }

    std::vector<TChunkSlice> Slice()
    {
        i64 sliceDataWeight = SliceReq_.slice_data_weight();
        bool sliceByKeys = SliceReq_.slice_by_keys();

        std::vector<TChunkSlice> slices;

        bool sliceStarted = false;
        TOwningKeyBound currentSliceLowerBound;
        i64 currentSliceStartRowIndex = 0;

        auto startSlice = [&] (
            const TOwningKeyBound& sliceLowerBound,
            i64 sliceStartRowIndex)
        {
            YT_VERIFY(!sliceStarted);
            sliceStarted = true;

            currentSliceLowerBound = sliceLowerBound;
            currentSliceStartRowIndex = sliceStartRowIndex;
        };

        auto endSlice = [&] (
            const TOwningKeyBound& sliceUpperBound,
            i64 sliceEndRowIndex)
        {
            YT_VERIFY(sliceStarted);
            sliceStarted = false;

            TChunkSlice slice;
            slice.LowerLimit.KeyBound() = currentSliceLowerBound;
            slice.UpperLimit.KeyBound() = sliceUpperBound;
            slice.RowCount = sliceEndRowIndex - currentSliceStartRowIndex;
            slice.DataWeight = slice.RowCount * DataWeightPerRow_;
            if (!sliceByKeys) {
                slice.LowerLimit.SetRowIndex(currentSliceStartRowIndex);
                slice.UpperLimit.SetRowIndex(sliceEndRowIndex);
            }
            slices.push_back(std::move(slice));
        };

        // Upper bounds of intersection of last block and request.
        TOwningKeyBound lastBlockUpperBound;
        i64 lastBlockEndRowIndex = -1;

        TPeriodicYielder yielder(SlicerYieldPeriod);

        for (int blockIndex = 0; blockIndex < std::ssize(BlockDescriptors_); ++blockIndex) {
            yielder.TryYield();

            const auto& block = BlockDescriptors_[blockIndex];
            auto blockLowerBound = blockIndex == 0
                ? ChunkLowerBound_
                : BlockDescriptors_[blockIndex - 1].UpperBound.Invert();
            const auto& blockUpperBound = block.UpperBound;

            if (!sliceByKeys && blockIndex > 0) {
                blockLowerBound = blockLowerBound.ToggleInclusiveness();
            }

            // This might happen if block consisnts of single key.
            if (SliceComparator_.IsRangeEmpty(blockLowerBound, blockUpperBound)) {
                blockLowerBound = blockLowerBound.ToggleInclusiveness();
                YT_VERIFY(!SliceComparator_.IsRangeEmpty(blockLowerBound, blockUpperBound));
            }

            i64 blockStartRowIndex = blockIndex == 0
                ? 0
                : BlockDescriptors_[blockIndex - 1].ChunkRowCount;
            i64 blockEndRowIndex = block.ChunkRowCount;

            // Block is completely to the left of the request by keys.
            if (SliceComparator_.IsRangeEmpty(SliceLowerBound_, blockUpperBound)) {
                continue;
            }
            // Block is completely to the right of the request by row indices.
            if (SliceStartRowIndex_ >= blockEndRowIndex) {
                continue;
            }

            // Block is completely to the right of the request by keys.
            if (SliceComparator_.IsRangeEmpty(blockLowerBound, SliceUpperBound_)) {
                break;
            }
            // Block is completely to the right of the request by keys.
            if (SliceEndRowIndex_ <= blockStartRowIndex) {
                break;
            }

            // Intersect block's ranges with request's ranges.
            const auto& lowerBound = SliceComparator_.CompareKeyBounds(blockLowerBound, SliceLowerBound_) > 0
                ? blockLowerBound
                : SliceLowerBound_;

            const auto& upperBound = SliceComparator_.CompareKeyBounds(blockUpperBound, SliceUpperBound_) < 0
                ? blockUpperBound
                : SliceUpperBound_;

            i64 startRowIndex = std::max<i64>(blockStartRowIndex, SliceStartRowIndex_);
            i64 endRowIndex = std::min<i64>(blockEndRowIndex, SliceEndRowIndex_);

            if (!sliceStarted) {
                startSlice(lowerBound, startRowIndex);
            }

            if (sliceByKeys) {
                bool canSliceHere = true;
                i64 currentSliceRowCount = endRowIndex - currentSliceStartRowIndex;
                if (blockIndex + 1 < std::ssize(BlockDescriptors_)) {
                    const auto& nextBlock = BlockDescriptors_[blockIndex + 1];
                    // We are inside maniac key, so can't slice chunk here.
                    if (upperBound == nextBlock.UpperBound) {
                        canSliceHere = false;
                    }
                }
                if (canSliceHere && currentSliceRowCount * DataWeightPerRow_ >= sliceDataWeight) {
                    endSlice(upperBound, endRowIndex);
                }
            } else {
                i64 rowsPerDataSlice = std::max<i64>(1, DivCeil<i64>(sliceDataWeight, DataWeightPerRow_));
                while (endRowIndex - currentSliceStartRowIndex >= rowsPerDataSlice) {
                    yielder.TryYield();

                    i64 currentSliceEndRowIndex = currentSliceStartRowIndex + rowsPerDataSlice;
                    YT_VERIFY(currentSliceEndRowIndex > startRowIndex &&
                        currentSliceEndRowIndex <= endRowIndex);

                    endSlice(upperBound, currentSliceEndRowIndex);

                    if (currentSliceEndRowIndex < endRowIndex) {
                        startSlice(lowerBound, currentSliceEndRowIndex);
                    } else {
                        break;
                    }
                }
            }

            lastBlockUpperBound = upperBound;
            lastBlockEndRowIndex = endRowIndex;
        }

        // Finish last slice.
        if (sliceStarted) {
            endSlice(lastBlockUpperBound, lastBlockEndRowIndex);
        }

        return slices;
    }

private:
    //! Represents a block of chunk.
    struct TBlockDescriptor
    {
        //! Keys upper bound in block.
        TOwningKeyBound UpperBound;

        //! Amount of rows in block.
        i64 RowCount;

        //! Total amount of rows in block and all the previous rows.
        i64 ChunkRowCount;
    };
    std::vector<TBlockDescriptor> BlockDescriptors_;

    const NChunkClient::NProto::TSliceRequest& SliceReq_;
    const NChunkClient::NProto::TChunkMeta& Meta_;

    TOwningKeyBound SliceLowerBound_;
    TOwningKeyBound SliceUpperBound_;

    i64 SliceStartRowIndex_;
    i64 SliceEndRowIndex_;

    TComparator SliceComparator_;

    TOwningKeyBound ChunkLowerBound_;
    TOwningKeyBound ChunkUpperBound_;

    i64 DataWeightPerRow_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

std::vector<TChunkSlice> SliceChunk(
    const NChunkClient::NProto::TSliceRequest& sliceReq,
    const NChunkClient::NProto::TChunkMeta& meta)
{
    TSortedChunkSlicer slicer(sliceReq, meta);
    auto result = slicer.Slice();
    slicer.Clear();

    return result;
}

////////////////////////////////////////////////////////////////////////////////

void ToProto(
    NChunkClient::NProto::TChunkSlice* protoChunkSlice,
    const TChunkSlice& chunkSlice)
{
    if (!chunkSlice.LowerLimit.IsTrivial()) {
        ToProto(protoChunkSlice->mutable_lower_limit(), chunkSlice.LowerLimit);
    }
    if (!chunkSlice.UpperLimit.IsTrivial()) {
        ToProto(protoChunkSlice->mutable_upper_limit(), chunkSlice.UpperLimit);
    }
    protoChunkSlice->set_data_weight_override(chunkSlice.DataWeight);
    protoChunkSlice->set_row_count_override(chunkSlice.RowCount);
}

void ToProto(
    const TKeySetWriterPtr& keysWriter,
    const TKeySetWriterPtr& keyBoundsWriter,
    NChunkClient::NProto::TChunkSlice* protoChunkSlice,
    const TChunkSlice& chunkSlice)
{
    if (chunkSlice.LowerLimit.KeyBound()) {
        int index = keyBoundsWriter->WriteKey(chunkSlice.LowerLimit.KeyBound().Prefix);
        protoChunkSlice->mutable_lower_limit()->set_key_bound_is_inclusive(chunkSlice.LowerLimit.KeyBound().IsInclusive);
        YT_VERIFY(keysWriter->WriteKey(KeyBoundToLegacyRow(chunkSlice.LowerLimit.KeyBound())) == index);
        protoChunkSlice->mutable_lower_limit()->set_key_index(index);
    }

    if (chunkSlice.LowerLimit.GetRowIndex()) {
        protoChunkSlice->mutable_lower_limit()->set_row_index(*chunkSlice.LowerLimit.GetRowIndex());
    }

    if (chunkSlice.UpperLimit.KeyBound()) {
        int index = keyBoundsWriter->WriteKey(chunkSlice.UpperLimit.KeyBound().Prefix);
        protoChunkSlice->mutable_upper_limit()->set_key_bound_is_inclusive(chunkSlice.UpperLimit.KeyBound().IsInclusive);
        YT_VERIFY(keysWriter->WriteKey(KeyBoundToLegacyRow(chunkSlice.UpperLimit.KeyBound())) == index);
        protoChunkSlice->mutable_upper_limit()->set_key_index(index);
    }

    if (chunkSlice.UpperLimit.GetRowIndex()) {
        protoChunkSlice->mutable_upper_limit()->set_row_index(*chunkSlice.UpperLimit.GetRowIndex());
    }

    protoChunkSlice->set_data_weight_override(chunkSlice.DataWeight);
    protoChunkSlice->set_row_count_override(chunkSlice.RowCount);
}

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

std::optional<THashSet<int>> GetBlockFilter(
    const NChunkClient::NProto::TChunkMeta& chunkMeta,
    const std::optional<std::vector<TColumnStableName>>& columnStableNames)
{
    if (columnStableNames) {
        TNameTablePtr nameTable;
        if (auto nameTableExt = FindProtoExtension<TNameTableExt>(chunkMeta.extensions())) {
            nameTable = FromProto<TNameTablePtr>(*nameTableExt);
        } else if (auto schemaExt = FindProtoExtension<TTableSchemaExt>(chunkMeta.extensions())) {
            nameTable = TNameTable::FromSchemaStable(FromProto<TTableSchema>(*schemaExt));
        } else {
            return {};
        }

        THashSet<int> blockFilter;

        auto columnMetaExt = FindProtoExtension<TColumnMetaExt>(chunkMeta.extensions());

        if (!columnMetaExt) {
            return {};
        }

        for (const auto& columName : *columnStableNames) {
            if (auto columnId = nameTable->FindId(columName.Underlying())) {
                YT_VERIFY(*columnId < columnMetaExt->columns_size());
                auto columnMeta = columnMetaExt->columns(*columnId);
                for (const auto& segment : columnMeta.segments()) {
                    blockFilter.insert(segment.block_index());
                }
            }
        }

        return blockFilter;
    }

    return {};
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

i64 GetChunkSliceDataWeight(
    const NChunkClient::NProto::TReqGetChunkSliceDataWeights::TChunkSlice& weightedChunkRequest,
    const NChunkClient::NProto::TChunkMeta& chunkMeta,
    const std::optional<std::vector<TColumnStableName>>& columnStableNames)
{
    auto miscExt = GetProtoExtension<NChunkClient::NProto::TMiscExt>(chunkMeta.extensions());
    auto chunkId = FromProto<TChunkId>(weightedChunkRequest.chunk_id());

    i64 chunkDataWeight = miscExt.data_weight();
    i64 chunkUncompressedSize = miscExt.uncompressed_data_size();
    i64 chunkRowCount = miscExt.row_count();

    auto chunkFormat = CheckedEnumCast<EChunkFormat>(chunkMeta.format());
    switch (chunkFormat) {
        case EChunkFormat::TableUnversionedSchemalessHorizontal:
        case EChunkFormat::TableUnversionedColumnar:
        case EChunkFormat::TableVersionedSimple:
        case EChunkFormat::TableVersionedSlim:
        case EChunkFormat::TableVersionedIndexed:
        case EChunkFormat::TableVersionedColumnar:
            break;
        default:
            THROW_ERROR_EXCEPTION("Unsupported format %Qlv for chunk %v",
                chunkFormat,
                chunkId);
    }

    TComparator chunkComparator;
    if (auto schemaExt = FindProtoExtension<TTableSchemaExt>(chunkMeta.extensions())) {
        chunkComparator = FromProto<TTableSchema>(*schemaExt).ToComparator();
    } else {
        auto keyColumnsExt = GetProtoExtension<TKeyColumnsExt>(chunkMeta.extensions());
        int keyColumnCount = keyColumnsExt.names_size();
        chunkComparator = TComparator(std::vector<ESortOrder>(keyColumnCount, ESortOrder::Ascending));
    }

    auto blockFilter = GetBlockFilter(chunkMeta, columnStableNames);

    TOwningKeyBound chunkLowerBound;
    TOwningKeyBound chunkUpperBound;
    YT_VERIFY(FindBoundaryKeyBounds(chunkMeta, &chunkLowerBound, &chunkUpperBound));

    TReadLimit sliceLowerLimit(
        weightedChunkRequest.lower_limit(),
        /*isUpper*/ false,
        chunkComparator.GetLength());
    TReadLimit sliceUpperLimit(
        weightedChunkRequest.upper_limit(),
        /*isUpper*/ true,
        chunkComparator.GetLength());

    auto sliceLowerBound = sliceLowerLimit.KeyBound()
        ? sliceLowerLimit.KeyBound()
        : chunkLowerBound;
    auto sliceUpperBound = sliceUpperLimit.KeyBound()
        ? sliceUpperLimit.KeyBound()
        : chunkUpperBound;

    sliceLowerBound = ShortenKeyBound(sliceLowerBound, chunkComparator.GetLength());
    sliceUpperBound = ShortenKeyBound(sliceUpperBound, chunkComparator.GetLength());

    auto sliceStartRowIndex = sliceLowerLimit.GetRowIndex().value_or(0);
    auto sliceEndRowIndex = sliceUpperLimit.GetRowIndex().value_or(chunkRowCount);

    i64 sliceUncompressedSize = 0;

    auto blockMetaExt = GetProtoExtension<TDataBlockMetaExt>(chunkMeta.extensions());
    auto blockCount = blockMetaExt.data_blocks_size();
    for (int blockIndex = 0; blockIndex < blockCount; ++blockIndex) {
        const auto& block = blockMetaExt.data_blocks(blockIndex);
        YT_VERIFY(block.block_index() == blockIndex);

        // We can skip blocks which do not contain any of the requested columns.
        if (blockFilter && !blockFilter->contains(blockIndex)) {
            continue;
        }

        auto blockLastKeyRow = FromProto<TUnversionedOwningRow>(block.last_key());
        auto blockLastKey = TKey::FromRowUnchecked(blockLastKeyRow);

        // Block is completely to the left of the slice by key bound.
        if (!chunkComparator.TestKey(blockLastKey, sliceLowerBound)) {
            continue;
        }

        // Block is completely to the left of the slice by row index.
        if (block.chunk_row_count() <= sliceStartRowIndex) {
            continue;
        }

        // Block is partially to the right of the slice by key bound.
        // We can break here, since in all current layouts blocks in sorted tables are sorted by last_key.
        if (!chunkComparator.TestKey(blockLastKey, sliceUpperBound)) {
            break;
        }

        // Block is partially to the right of the slice by row index.
        // We can break here, since in all current layouts chunk_row_counts are monotonous.
        if (block.chunk_row_count() > sliceEndRowIndex) {
            break;
        }

        sliceUncompressedSize += block.uncompressed_size();
    }

    auto sliceDataWeight = static_cast<double>(chunkDataWeight) * sliceUncompressedSize / chunkUncompressedSize;
    return static_cast<i64>(sliceDataWeight);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTableClient
