#include "chunk_slice.h"

#include "chunk_meta_extensions.h"
#include "key_set.h"

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>

#include <yt/yt/client/table_client/comparator.h>
#include <yt/yt/client/table_client/key_bound.h>
#include <yt/yt/client/table_client/name_table.h>
#include <yt/yt/client/table_client/schema.h>

#include <yt/yt/core/concurrency/periodic_yielder.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/library/numeric/util.h>

#include <library/cpp/yt/memory/non_null_ptr.h>

namespace NYT::NTableClient {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NTableClient::NProto;

using NYT::FromProto;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr auto SlicerYieldPeriod = TDuration::MilliSeconds(10);

////////////////////////////////////////////////////////////////////////////////

class TSortedChunkSlicer
    : private TNonCopyable
{
public:
    struct TFinishManiacKeyResult
    {
        TCompactVector<TChunkSlice, 2> Slices = {};
        int LastProcessedBlockIndex = -1;
    };

    TSortedChunkSlicer(
        const NChunkClient::NProto::TSliceRequest& sliceReq,
        const NChunkClient::NProto::TChunkMeta& meta,
        TOwningKeyBound chunkLowerBound,
        TOwningKeyBound chunkUpperBound)
        : Meta_(meta)
        , MiscExt_(GetProtoExtension<NChunkClient::NProto::TMiscExt>(Meta_.extensions()))
        , SliceComparator_(CreateSliceComparator(sliceReq, Meta_))
        , ChunkDataWeight_(MiscExt_.has_data_weight()
            ? MiscExt_.data_weight()
            : MiscExt_.uncompressed_data_size())
        , ChunkLowerBound_(ShortenKeyBound(std::move(chunkLowerBound), SliceComparator_.GetLength()))
        , ChunkUpperBound_(ShortenKeyBound(std::move(chunkUpperBound), SliceComparator_.GetLength()))
        , SliceLowerLimit_([&] {
            TReadLimit sliceLowerLimit(sliceReq.lower_limit(), /*isUpper*/ false, sliceReq.key_column_count());
            sliceLowerLimit.KeyBound() = ShortenKeyBound(sliceLowerLimit.KeyBound(), SliceComparator_.GetLength());
            return sliceLowerLimit;
        }())
        , SliceUpperLimit_([&] {
            TReadLimit sliceUpperLimit(sliceReq.upper_limit(), /*isUpper*/ true, sliceReq.key_column_count());
            sliceUpperLimit.KeyBound() = ShortenKeyBound(sliceUpperLimit.KeyBound(), SliceComparator_.GetLength());
            return sliceUpperLimit;
        }())
        , SliceLowerBound_(SliceLowerLimit_.KeyBound() ? SliceLowerLimit_.KeyBound() : ChunkLowerBound_)
        , SliceUpperBound_(SliceUpperLimit_.KeyBound() ? SliceUpperLimit_.KeyBound() : ChunkUpperBound_)
        , SliceStartRowIndex_(SliceLowerLimit_.GetRowIndex().value_or(0))
        , SliceEndRowIndex_(SliceUpperLimit_.GetRowIndex().value_or(MiscExt_.row_count()))
        , HasRowLimits_(SliceLowerLimit_.GetRowIndex() || SliceUpperLimit_.GetRowIndex())
        , MinManiacDataWeight_(YT_OPTIONAL_FROM_PROTO(sliceReq, min_maniac_data_weight))
        , SliceDataWeight_(sliceReq.slice_data_weight())
        , SliceByKeys_(sliceReq.slice_by_keys())
    {
        YT_VERIFY(MiscExt_.row_count() > 0);

        auto blockMetaExt = GetProtoExtension<TDataBlockMetaExt>(Meta_.extensions());
        int blockCount = blockMetaExt.data_blocks_size();
        BlockDescriptors_.reserve(blockCount);

        auto yielder = CreatePeriodicYielder(SlicerYieldPeriod);

        for (int blockIndex = 0; blockIndex < blockCount; ++blockIndex) {
            yielder.TryYield();

            const auto& block = blockMetaExt.data_blocks(blockIndex);
            YT_VERIFY(block.block_index() == blockIndex);

            // TODO(coteeq): All these bounds could've been stored in a TRowBuffer.
            auto blockLastKey = FromProto<TUnversionedOwningRow>(block.last_key());
            auto blockUpperBound = TOwningKeyBound::FromRow(
                /*row*/ TUnversionedOwningRow(blockLastKey.FirstNElements(SliceComparator_.GetLength())),
                /*isInclusive*/ true,
                /*isUpper*/ true);

            i64 chunkRowCount = block.chunk_row_count();
            i64 rowCount = BlockDescriptors_.empty()
                ? chunkRowCount
                : chunkRowCount - BlockDescriptors_.back().ChunkRowCount;

            TBlockDescriptor blockDescriptor{
                .UpperBound = blockUpperBound,
                .RowCount = rowCount,
                .ChunkRowCount = chunkRowCount,
            };
            BlockDescriptors_.push_back(std::move(blockDescriptor));
        }
    }

    void Clear()
    {
        auto yielder = CreatePeriodicYielder(SlicerYieldPeriod);

        while (!BlockDescriptors_.empty()) {
            BlockDescriptors_.pop_back();
            yielder.TryYield();
        }
    }

    //! Prerequisites:
    //! 1. block.UpperBound.Key should be allowed by sliceReq's ranges.
    //! 2. sliceStarted = true.
    std::optional<TFinishManiacKeyResult> TryFinishManiacKey(
        int blockIndex,
        const TOwningKeyBound& currentSliceLowerBound,
        i64 currentSliceStartRowIndex) const
    {
        if (!MinManiacDataWeight_) {
            return {};
        }
        YT_VERIFY(SliceByKeys_);
        // Construct up to two chunks:
        // the maniac one and the preceeding one.
        //
        // Let the maniac key be 'M' and we have such blocks:
        //
        // leftSlice     maniacSlice
        // <------><------------------------>
        // |ABCD|EEMMM|MMMMM|MMMMM|MMMMM|MMMMZZZ|
        //      ^                 ^
        //      |                 |
        //      |                 ` lastFullManiacBlockIndex
        //      |
        //      ` blockIndex is here
        //
        //             ^^^^^^^^^^^^^^^^^ fullManiacBlocksAhead
        //
        // BlockDescriptors_[blockIndex].UpperBound is our maniac candidate.
        const auto& block = BlockDescriptors_[blockIndex];
        const auto& maniacKeyAsUpperBound = block.UpperBound;
        int fullManiacBlocksAhead = 0;
        while (blockIndex + fullManiacBlocksAhead + 1 < std::ssize(BlockDescriptors_)
            && maniacKeyAsUpperBound == BlockDescriptors_[blockIndex + fullManiacBlocksAhead + 1].UpperBound)
        {
            ++fullManiacBlocksAhead;
        }

        // We are definitely not a maniac if we can fit in a block.
        if (fullManiacBlocksAhead == 0) {
            return {};
        }

        int lastFullManiacBlockIndex = blockIndex + fullManiacBlocksAhead;
        const auto& lastFullManiacBlock = BlockDescriptors_[lastFullManiacBlockIndex];

        i64 fullManiacRowCount = lastFullManiacBlock.ChunkRowCount - block.ChunkRowCount;
        i64 nextChunkRowCount = lastFullManiacBlockIndex + 1 < std::ssize(BlockDescriptors_)
            ? BlockDescriptors_[lastFullManiacBlockIndex + 1].RowCount
            : i64(0);

        TFinishManiacKeyResult result;
        result.LastProcessedBlockIndex = lastFullManiacBlockIndex;

        // NB(coteeq): We compare opimistic estimate on purpose.
        // Hopefully, the caller knows what he is doing and will ensure that
        // MinManiacDataWeight_ >> blockSize, reducing the error margin.
        if (fullManiacRowCount < *MinManiacDataWeight_) {
            // We still need to advance blockIndex, otherwise the algorithm will
            // be O(n^2).
            // NB: It is safe to just jump to the last maniac block, because
            // we know for sure, that there is no RowIndex in SliceReq_'s limits
            // and the key does not change. So internal structures of |Slice|
            // method will not be spoiled by this jump.
            return result;
        }

        // We found a maniac key!

        YT_VERIFY(maniacKeyAsUpperBound.IsUpper);
        // If the range specified by client does not allow maniac key, we
        // should've stopped processing the chunk (and never even enter tryFinishManiacSlice).
        // Otherwise, maniacKeyAsUpperBound must have been taken from
        // block's last_key, so it must be inclusive by construction.
        YT_VERIFY(maniacKeyAsUpperBound.IsInclusive);

        // Flush leftSlice.

        auto leftSliceUpperBound = maniacKeyAsUpperBound.ToggleInclusiveness();

        // If maniac is the first key in the flushing slice, the leftSlice will be empty.
        if (!SliceComparator_.IsRangeEmpty(currentSliceLowerBound, leftSliceUpperBound)) {
            TChunkSlice slice;
            slice.LowerLimit.KeyBound() = currentSliceLowerBound;
            slice.UpperLimit.KeyBound() = leftSliceUpperBound;
            EstimateSliceSizeFromRowCount(&slice, block.ChunkRowCount - currentSliceStartRowIndex);
            result.Slices.push_back(std::move(slice));
        }

        // Flush maniac slice.

        TChunkSlice maniacSlice;
        maniacSlice.LowerLimit.KeyBound() = maniacKeyAsUpperBound;
        maniacSlice.LowerLimit.KeyBound().IsUpper = false;
        maniacSlice.UpperLimit.KeyBound() = maniacKeyAsUpperBound;

        // We do not know how many rows have the maniac key, so take a pessimistic estimate
        i64 maniacPessimisticRowCount = block.RowCount + fullManiacRowCount + nextChunkRowCount;
        EstimateSliceSizeFromRowCount(&maniacSlice, maniacPessimisticRowCount);

        result.Slices.push_back(std::move(maniacSlice));

        return result;
    }

    std::vector<TChunkSlice> Slice() const
    {
        // TODO(coteeq): Extract this state machine at the class level.
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
            EstimateSliceSizeFromRowCount(&slice, sliceEndRowIndex - currentSliceStartRowIndex);
            if (!SliceByKeys_) {
                slice.LowerLimit.SetRowIndex(currentSliceStartRowIndex);
                slice.UpperLimit.SetRowIndex(sliceEndRowIndex);
            }
            slices.push_back(std::move(slice));
        };

        // Upper bounds of intersection of last block and request.
        TOwningKeyBound lastBlockUpperBound;
        i64 lastBlockEndRowIndex = -1;

        auto yielder = CreatePeriodicYielder(SlicerYieldPeriod);

        for (int blockIndex = 0; blockIndex < std::ssize(BlockDescriptors_); ++blockIndex) {
            yielder.TryYield();

            const auto& block = BlockDescriptors_[blockIndex];
            auto blockLowerBound = blockIndex == 0
                ? ChunkLowerBound_
                : BlockDescriptors_[blockIndex - 1].UpperBound.Invert();

            if (!SliceByKeys_ && blockIndex > 0) {
                blockLowerBound = blockLowerBound.ToggleInclusiveness();
            }

            const auto& blockUpperBound = block.UpperBound;
            // This might happen if block consists of a single key.
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
            // Block is completely to the left of the request by row indices.
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
            const auto& lowerBound = SliceComparator_.CompareKeyBounds(blockLowerBound, SliceLowerBound_) >= 0
                ? blockLowerBound
                : SliceLowerBound_;

            // If it is not, there is no point in checking for maniac key, since
            // the whole slicing will just end at the current block.
            bool upperBoundIsInRange = SliceComparator_.CompareKeyBounds(blockUpperBound, SliceUpperBound_) <= 0;

            const auto& upperBound = upperBoundIsInRange
                ? blockUpperBound
                : SliceUpperBound_;

            i64 startRowIndex = std::max<i64>(blockStartRowIndex, SliceStartRowIndex_);
            i64 endRowIndex = std::min<i64>(blockEndRowIndex, SliceEndRowIndex_);

            if (!sliceStarted) {
                startSlice(lowerBound, startRowIndex);
            }

            if (SliceByKeys_) {
                // TODO(coteeq): Isolate maniac even if row limits are specified.
                if (upperBoundIsInRange && !HasRowLimits_) {
                    auto finishManiacKeyResult = TryFinishManiacKey(
                        blockIndex,
                        currentSliceLowerBound,
                        currentSliceStartRowIndex);
                    if (finishManiacKeyResult) {
                        for (auto&& slice : finishManiacKeyResult->Slices) {
                            slices.push_back(std::move(slice));
                        }
                        blockIndex = finishManiacKeyResult->LastProcessedBlockIndex;

                        // If we did not flush the maniac, we still need to apply non-maniac logic.
                        if (!finishManiacKeyResult->Slices.empty()) {
                            sliceStarted = false;
                            // Do not apply other logic. Just go straight to the next block.
                            continue;
                        } else {
                            endRowIndex = BlockDescriptors_[blockIndex].ChunkRowCount;
                        }
                    }
                }

                bool canSliceHere = true;
                i64 currentSliceRowCount = endRowIndex - currentSliceStartRowIndex;
                if (blockIndex + 1 < std::ssize(BlockDescriptors_)) {
                    const auto& nextBlock = BlockDescriptors_[blockIndex + 1];
                    // We are inside of a huge key, so can't slice chunk here.
                    if (upperBound == nextBlock.UpperBound) {
                        canSliceHere = false;
                    }
                }
                if (canSliceHere && EstimateDataWeight(currentSliceRowCount) >= SliceDataWeight_) {
                    endSlice(upperBound, endRowIndex);
                }
            } else {
                double dataWeightPerRow = static_cast<double>(ChunkDataWeight_) / MiscExt_.row_count();
                i64 rowsPerDataSlice = std::max<i64>(1, std::ceil(SliceDataWeight_ / dataWeightPerRow));
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

    const NChunkClient::NProto::TChunkMeta& Meta_;
    const NChunkClient::NProto::TMiscExt MiscExt_;

    const TComparator SliceComparator_;

    const i64 ChunkDataWeight_;

    const TOwningKeyBound ChunkLowerBound_;
    const TOwningKeyBound ChunkUpperBound_;

    const TReadLimit SliceLowerLimit_;
    const TReadLimit SliceUpperLimit_;

    const TOwningKeyBound SliceLowerBound_;
    const TOwningKeyBound SliceUpperBound_;

    const i64 SliceStartRowIndex_;
    const i64 SliceEndRowIndex_;

    const bool HasRowLimits_;

    const std::optional<i64> MinManiacDataWeight_;

    const i64 SliceDataWeight_;
    const bool SliceByKeys_;

    std::vector<TBlockDescriptor> BlockDescriptors_;

    double ComputeRowSelectivityFactor(i64 rowCount) const
    {
        return static_cast<double>(rowCount) / MiscExt_.row_count();
    }

    void EstimateSliceSizeFromRowCount(TNonNullPtr<TChunkSlice> slice, i64 rowCount) const
    {
        YT_VERIFY(rowCount >= 0);
        double rowSelectivityFactor = ComputeRowSelectivityFactor(rowCount);

        slice->RowCount = rowCount;
        slice->DataWeight = std::max(SignedSaturationConversion(ChunkDataWeight_ * rowSelectivityFactor), 1l);
        // NB(apollo1321): It is possible to compute [un]compressed data size exactly for slices.
        // I'm not sure if such precision is really needed. It may be worth supporting in the future if necessary.
        slice->CompressedDataSize = std::max(SignedSaturationConversion(MiscExt_.compressed_data_size() * rowSelectivityFactor), 1l);
        slice->UncompressedDataSize = std::max(SignedSaturationConversion(MiscExt_.uncompressed_data_size() * rowSelectivityFactor), 1l);
    }

    i64 EstimateDataWeight(i64 rowCount) const
    {
        return std::max(SignedSaturationConversion(ChunkDataWeight_ * ComputeRowSelectivityFactor(rowCount)), 1l);
    }

    static TComparator CreateSliceComparator(
        const NChunkClient::NProto::TSliceRequest& sliceReq,
        const NChunkClient::NProto::TChunkMeta& meta)
    {
        TComparator chunkComparator;
        if (auto schemaExt = FindProtoExtension<TTableSchemaExt>(meta.extensions())) {
            chunkComparator = FromProto<TTableSchema>(*schemaExt).ToComparator();
        } else {
            // NB(gritukan): Very old chunks do not have schema, but they are always sorted in ascending order.
            auto keyColumnsExt = GetProtoExtension<TKeyColumnsExt>(meta.extensions());
            int keyColumnCount = keyColumnsExt.names_size();
            chunkComparator = TComparator(std::vector<ESortOrder>(keyColumnCount, ESortOrder::Ascending));
        }

        int keyColumnCount = std::min<int>(chunkComparator.GetLength(), sliceReq.key_column_count());
        return chunkComparator.Trim(keyColumnCount);
    }
};

////////////////////////////////////////////////////////////////////////////////

void ValidateChunkFormat(EChunkFormat chunkFormat, TChunkId chunkId)
{
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
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

void FormatValue(TStringBuilderBase* builder, const TChunkSlice& slice, TStringBuf /*spec*/)
{
    builder->AppendFormat("LowerLimit: %v, UpperLimit: %v, RowCount: %v, DataWeight: %v",
        slice.LowerLimit,
        slice.UpperLimit,
        slice.RowCount,
        slice.DataWeight);
}

////////////////////////////////////////////////////////////////////////////////

std::vector<TChunkSlice> SliceChunk(
    const NChunkClient::NProto::TSliceRequest& sliceReq,
    const NChunkClient::NProto::TChunkMeta& meta)
{
    ValidateChunkFormat(FromProto<EChunkFormat>(meta.format()), FromProto<TChunkId>(sliceReq.chunk_id()));
    TOwningKeyBound chunkLowerBound;
    TOwningKeyBound chunkUpperBound;
    YT_VERIFY(FindBoundaryKeyBounds(meta, &chunkLowerBound, &chunkUpperBound));
    TSortedChunkSlicer slicer(sliceReq, meta, std::move(chunkLowerBound), std::move(chunkUpperBound));
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
    protoChunkSlice->set_compressed_data_size_override(chunkSlice.CompressedDataSize);
    protoChunkSlice->set_uncompressed_data_size_override(chunkSlice.UncompressedDataSize);
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
    protoChunkSlice->set_compressed_data_size_override(chunkSlice.CompressedDataSize);
    protoChunkSlice->set_uncompressed_data_size_override(chunkSlice.UncompressedDataSize);
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

    auto chunkFormat = FromProto<EChunkFormat>(chunkMeta.format());
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
