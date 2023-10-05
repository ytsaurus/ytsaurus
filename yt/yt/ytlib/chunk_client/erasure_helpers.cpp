#include "erasure_helpers.h"
#include "chunk_meta_extensions.h"
#include "chunk_reader.h"
#include "chunk_writer.h"
#include "dispatcher.h"
#include "public.h"

#include <yt/yt/client/misc/workload.h>

#include <yt/yt/library/erasure/impl/codec.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/misc/checksum.h>
#include <yt/yt/core/misc/numeric_helpers.h>
#include <yt/yt/core/misc/memory_reference_tracker.h>

#include <library/cpp/yt/assert/assert.h>

#include <util/random/random.h>

#include <algorithm>

namespace NYT::NChunkClient::NErasureHelpers {

using namespace NYT::NErasure;
using namespace NConcurrency;
using namespace NProto;
using NYT::FromProto;

////////////////////////////////////////////////////////////////////////////////

TPartIndexList GetParityPartIndices(const ICodec* codec)
{
    TPartIndexList result;
    for (int index = codec->GetDataPartCount(); index < codec->GetTotalPartCount(); ++index) {
        result.push_back(index);
    }
    return result;
}

static int GetStripeBlockCount(const TErasurePlacementExt& placementExt, int partIndex, int stripeIndex)
{
    const auto& partInfo = placementExt.part_infos(partIndex);
    const auto& nextPartInfo = placementExt.part_infos((partIndex + 1) % placementExt.part_infos_size());
    int nextPartStripeIndex = stripeIndex + (partIndex + 1) / placementExt.part_infos_size();

    if (nextPartStripeIndex < nextPartInfo.first_block_index_per_stripe_size()) {
        return nextPartInfo.first_block_index_per_stripe(nextPartStripeIndex) -
            partInfo.first_block_index_per_stripe(stripeIndex);
    }

    // There is no next part for last part of the last stripe, use total block count instead.
    int totalBlockCount = 0;
    for (int index = 0; index < placementExt.part_infos_size(); ++index) {
        totalBlockCount += placementExt.part_infos(index).block_sizes_size();
    }
    return totalBlockCount - partInfo.first_block_index_per_stripe(stripeIndex);
}

////////////////////////////////////////////////////////////////////////////////

i64 TPartRange::Size() const
{
    return std::max(static_cast<i64>(0), End - Begin);
}

bool TPartRange::IsEmpty() const
{
    return Begin >= End;
}

TPartRange::operator bool() const
{
    return !IsEmpty();
}

bool operator == (const TPartRange& lhs, const TPartRange& rhs)
{
    return lhs.Begin == rhs.Begin && lhs.End == rhs.End;
}

TPartRange Intersection(const TPartRange& lhs, const TPartRange& rhs)
{
    i64 beginMax = std::max(lhs.Begin, rhs.Begin);
    i64 endMin = std::min(lhs.End, rhs.End);
    return TPartRange({beginMax, endMin});
}

std::vector<TPartRange> Union(const std::vector<TPartRange>& ranges_)
{
    auto ranges = ranges_;
    auto comparer = [] (const TPartRange& lhs, const TPartRange& rhs) {
        if (lhs.Begin != rhs.Begin) {
            return lhs.Begin < rhs.Begin;
        }
        return lhs.End < rhs.End;
    };
    std::sort(ranges.begin(), ranges.end(), comparer);

    bool opened = false;
    i64 start = 0;
    i64 end = 0;
    std::vector<TPartRange> result;
    for (const auto& range : ranges) {
        if (opened) {
            if (range.Begin > end) {
                result.push_back(TPartRange({start, end}));
                start = range.Begin;
                end = range.End;
            } else {
                end = std::max(end, range.End);
            }
        } else {
            opened = true;
            end = range.End;
        }
    }
    if (opened) {
        result.push_back(TPartRange({start, end}));
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

TParityPartSplitInfo::TParityPartSplitInfo(
    const TErasurePlacementExt& placementExt)
    : BlockSize_(placementExt.parity_block_size())
    , StripeBlockCounts_(FromProto<std::vector<int>>(placementExt.parity_block_count_per_stripe()))
    , StripeLastBlockSizes_(FromProto<std::vector<i64>>(placementExt.parity_last_block_size_per_stripe()))
{
    YT_VERIFY(StripeBlockCounts_.size() > 0);
    YT_VERIFY(StripeLastBlockSizes_.size() == StripeBlockCounts_.size());
    YT_VERIFY(BlockSize_ > 0);
}

i64 TParityPartSplitInfo::GetStripeOffset(int stripeIndex) const
{
    YT_VERIFY(stripeIndex >= 0 && stripeIndex <= std::ssize(StripeBlockCounts_));

    i64 result = 0;
    for (int index = 0; index < stripeIndex; ++index) {
        YT_VERIFY(StripeBlockCounts_[index] > 0);
        result += StripeLastBlockSizes_[index] + (StripeBlockCounts_[index] - 1) * BlockSize_;
    }
    return result;
}

i64 TParityPartSplitInfo::GetPartSize() const
{
    return GetStripeOffset(StripeBlockCounts_.size());
}

std::vector<TPartRange> TParityPartSplitInfo::SplitRangesByStripesAndAlignToParityBlocks(
    const std::vector<TPartRange>& ranges) const
{
    YT_VERIFY(std::is_sorted(
        ranges.begin(),
        ranges.end(),
        [] (TPartRange left, TPartRange right) {
            return left.End <= right.Begin;
        }
    ));

    std::vector<TPartRange> result;

    auto rangeIt = ranges.begin();
    TPartRange stripe = {};
    TPartRange range = {};

    for (int stripeIndex = 0; stripeIndex < std::ssize(StripeBlockCounts_); ++stripeIndex) {
        stripe.Begin = stripe.End;
        if (StripeBlockCounts_[stripeIndex] > 0) {
            stripe.End += (StripeBlockCounts_[stripeIndex] - 1) * BlockSize_ + StripeLastBlockSizes_[stripeIndex];
        }

        while (!range.IsEmpty() || rangeIt != ranges.end()) {
            if (range.IsEmpty()) {
                range = *rangeIt;
                ++rangeIt;
                continue;
            } else if (range.Begin >= stripe.End) {
                break;
            }

            TPartRange output;

            // Get next block of intersection with current stripe.
            output.Begin = stripe.Begin + Max(0L, RoundDown(range.Begin - stripe.Begin, BlockSize_));
            output.End = Min(stripe.End, output.Begin + BlockSize_);

            if (output.Begin >= output.End) {
                YT_VERIFY(range.End <= stripe.Begin);
                range = {};
                continue;
            }
            result.push_back(output);
            range.Begin = output.End;
        }
    }
    return result;
}

std::vector<TPartRange> TParityPartSplitInfo::GetParityBlockRanges() const
{
    i64 offset = 0;
    std::vector<TPartRange> result;

    for (int stripeIndex = 0; stripeIndex < std::ssize(StripeLastBlockSizes_); ++stripeIndex) {
        YT_VERIFY(StripeBlockCounts_[stripeIndex] > 0);

        for (int index = 0; index < StripeBlockCounts_[stripeIndex] - 1; ++index) {
            result.push_back({.Begin = offset, .End = offset + BlockSize_});
            offset += BlockSize_;
        }

        result.push_back({.Begin = offset, .End = offset + StripeLastBlockSizes_[stripeIndex]});
        offset += StripeLastBlockSizes_[stripeIndex];
    }
    return result;
}

std::vector<TPartRange> TParityPartSplitInfo::GetBlockRanges(int partIndex, const TErasurePlacementExt& placementExt) const
{
    int dataPartCount = placementExt.part_infos_size();
    if (partIndex >= dataPartCount) {
        return GetParityBlockRanges();
    }

    const auto& partInfo = placementExt.part_infos(partIndex);

    i64 offset = 0;
    int indexInPart = 0;
    std::vector<TPartRange> result;

    for (int stripeIndex = 0; stripeIndex < std::ssize(StripeLastBlockSizes_); ++stripeIndex) {
        int blockCount = GetStripeBlockCount(placementExt, partIndex, stripeIndex);

        i64 offsetInStripe = 0;
        for (int indexInStripe = 0; indexInStripe < blockCount; ++indexInStripe) {
            i64 blockSize = partInfo.block_sizes(indexInPart);
            result.push_back({.Begin = offset + offsetInStripe, .End = offset + offsetInStripe + blockSize});
            offsetInStripe += blockSize;
            ++indexInPart;
        }

        offset += BlockSize_ * (StripeBlockCounts_[stripeIndex] - 1) + StripeLastBlockSizes_[stripeIndex];
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

class TPartWriter::TImpl
    : public TRefCounted
{
public:
    TImpl(
        const TWorkloadDescriptor& workloadDescriptor,
        IChunkWriterPtr writer,
        const std::vector<TPartRange>& blockRanges,
        bool computeChecksum)
        : WorkloadDescriptor_(workloadDescriptor)
        , Writer_(writer)
        , BlockRanges_(blockRanges)
        , ComputeChecksum_(computeChecksum)
    { }

    TFuture<void> Consume(const TPartRange& range, const TSharedRef& block)
    {
        // Validate range monotonicity.
        YT_VERIFY(range.Begin == Cursor_);
        Cursor_ = range.End;

        std::vector<TBlock> blocksToWrite;

        // Fill current block if it is started.
        if (PositionInCurrentBlock_ > 0) {
            i64 copySize = std::min(block.Size(), CurrentBlock_.Size() - PositionInCurrentBlock_);
            memcpy(CurrentBlock_.Begin() + PositionInCurrentBlock_, block.Begin(), copySize);
            PositionInCurrentBlock_ += copySize;

            if (PositionInCurrentBlock_ == std::ssize(CurrentBlock_)) {
                blocksToWrite.push_back(TBlock(CurrentBlock_));

                ++CurrentBlockIndex_;
                PositionInCurrentBlock_ = 0;
            }
        }

        // Processing part blocks that fit inside given block.
        while (CurrentBlockIndex_ < std::ssize(BlockRanges_) && BlockRanges_[CurrentBlockIndex_].End <= range.End) {
            int blockPosition = BlockRanges_[CurrentBlockIndex_].Begin + PositionInCurrentBlock_ - range.Begin;
            YT_VERIFY(blockPosition >= 0);

            auto size = BlockRanges_[CurrentBlockIndex_].Size() - PositionInCurrentBlock_;
            blocksToWrite.push_back(TBlock(block.Slice(blockPosition, blockPosition + size)));
            ++CurrentBlockIndex_;
            PositionInCurrentBlock_ = 0;
        }

        // Process part block that just overlap with given block.
        if (CurrentBlockIndex_ < std::ssize(BlockRanges_) &&
            BlockRanges_[CurrentBlockIndex_].Begin + PositionInCurrentBlock_ < range.End) {

            int blockPosition = BlockRanges_[CurrentBlockIndex_].Begin + PositionInCurrentBlock_ - range.Begin;
            YT_VERIFY(blockPosition >= 0);

            CurrentBlock_ = TSharedMutableRef::Allocate(BlockRanges_[CurrentBlockIndex_].Size());
            i64 copySize = Min<i64>(block.Size(), range.Size() - blockPosition);
            memcpy(CurrentBlock_.Begin(), block.Begin() + blockPosition, copySize);
            PositionInCurrentBlock_ += copySize;
        }

        if (ComputeChecksum_) {
            for (auto& block : blocksToWrite) {
                block.Checksum = block.GetOrComputeChecksum();
                BlockChecksums_.push_back(block.Checksum);
            }
        }

        if (!Writer_->WriteBlocks(WorkloadDescriptor_, blocksToWrite)) {
            return Writer_->GetReadyEvent();
        }
        return VoidFuture;
    }

    TChecksum GetPartChecksum() const
    {
        return CombineChecksums(BlockChecksums_);
    }

private:
    const TWorkloadDescriptor WorkloadDescriptor_;
    const IChunkWriterPtr Writer_;
    const std::vector<TPartRange> BlockRanges_;
    const bool ComputeChecksum_;

    TSharedMutableRef CurrentBlock_;
    int CurrentBlockIndex_ = 0;
    i64 PositionInCurrentBlock_ = 0;
    i64 Cursor_ = 0;

    std::vector<TChecksum> BlockChecksums_;
};

TPartWriter::TPartWriter(
    const TWorkloadDescriptor& workloadDescriptor,
    IChunkWriterPtr writer,
    const std::vector<TPartRange>& blockRanges,
    bool computeChecksum)
    : Impl_(New<TImpl>(workloadDescriptor, writer, blockRanges, computeChecksum))
{ }

TPartWriter::~TPartWriter()
{ }

TFuture<void> TPartWriter::Consume(const TPartRange& range, const TSharedRef& block)
{
    return Impl_->Consume(range, block);
}

TChecksum TPartWriter::GetPartChecksum() const
{
    return Impl_->GetPartChecksum();
}

////////////////////////////////////////////////////////////////////////////////


class TPartReader::TImpl
    : public TRefCounted
{
public:
    TImpl(
        IBlocksReaderPtr reader,
        IChunkReader::TReadBlocksOptions readBlockOptions,
        const std::vector<TPartRange>& blockRanges)
        : Reader_(reader)
        , ReadBlockOptions_(readBlockOptions)
        , BlockRanges_(blockRanges)
    { }

    virtual TFuture<TSharedRef> Produce(const TPartRange& range)
    {
        if (PreviousRange_ && (*PreviousRange_ == range)) {
            return MakeFuture(LastResult_);
        }

        if (PreviousRange_) {
            // Requested ranges must be monotonic.
            YT_VERIFY(*PreviousRange_ == range || PreviousRange_->End <= range.Begin);
        }
        PreviousRange_ = range;

        // XXX(ignat): This may be optimized using monotonicity.
        std::vector<int> blockIndexes;
        for (int index = 0; index < std::ssize(BlockRanges_); ++index) {
            if (Intersection(BlockRanges_[index], range)) {
                blockIndexes.push_back(index);
            }
        }

        std::vector<int> indicesToRequest;
        for (int index : blockIndexes) {
            if (RequestedBlocks_.find(index) == RequestedBlocks_.end()) {
                indicesToRequest.push_back(index);
            }
        }

        if (!indicesToRequest.empty()) {
            auto blocksFuture = BIND(
                [=, this, this_ = MakeStrong(this)] () {
                    return Reader_->ReadBlocks(indicesToRequest);
                })
                // Or simple Via?
                .AsyncVia(TDispatcher::Get()->GetReaderInvoker())
                .Run();

            return blocksFuture.Apply(BIND(
                [=, this, this_ = MakeStrong(this)] (const TErrorOr<std::vector<TBlock>>& errorOrBlocks) -> TSharedRef {
                    if (!errorOrBlocks.IsOK()) {
                        THROW_ERROR TError(errorOrBlocks);
                    }
                    OnBlocksRead(indicesToRequest, TBlock::Unwrap(errorOrBlocks.Value()));
                    return BuildBlock(range);
                }));
        } else {
            return MakeFuture(BuildBlock(range));
        }
    }

private:
    const IBlocksReaderPtr Reader_;
    const IChunkReader::TReadBlocksOptions ReadBlockOptions_;
    const std::vector<TPartRange> BlockRanges_;

    THashMap<int, TSharedRef> RequestedBlocks_;
    std::optional<TPartRange> PreviousRange_;

    TSharedRef LastResult_;

    void OnBlocksRead(const std::vector<int>& indicesToRequest, const std::vector<TSharedRef>& blocks)
    {
        YT_VERIFY(indicesToRequest.size() == blocks.size());
        for (int index = 0; index < std::ssize(indicesToRequest); ++index) {
            RequestedBlocks_[indicesToRequest[index]] = blocks[index];
        }
    }

    TSharedRef BuildBlock(const TPartRange& range)
    {
        TSharedMutableRef result;

        // We use lazy initialization.
        auto initialize = [&] () {
            if (!result) {
                struct TErasureWriterSliceTag { };
                auto ref = TrackMemory(
                    ReadBlockOptions_.ClientOptions.MemoryReferenceTracker,
                    TSharedMutableRef::Allocate<TErasureWriterSliceTag>(range.Size()));
                result = TSharedMutableRef(const_cast<char*>(ref.Begin()), ref.Size(), ref.GetHolder());
            }
        };

        for (int index = 0; index < std::ssize(BlockRanges_) && BlockRanges_[index].Begin < range.End; ++index) {
            if (BlockRanges_[index].End <= range.Begin) {
                RequestedBlocks_.erase(index);
                continue;
            }

            i64 innerStart = Max<i64>(0, range.Begin - BlockRanges_[index].Begin);
            i64 innerEnd = Min(BlockRanges_[index].Size(), range.End - BlockRanges_[index].Begin);

            auto block = RequestedBlocks_[index];
            if (range.Size() == innerEnd - innerStart) {
                LastResult_ = block.Slice(innerStart, innerEnd);
                return LastResult_;
            }

            initialize();
            i64 pos = BlockRanges_[index].Begin + innerStart - range.Begin;
            std::copy(block.Begin() + innerStart, block.Begin() + innerEnd, result.Begin() + pos);

            // Cleanup blocks that we would not use anymore.
            if (range.End >= BlockRanges_[index].End) {
                RequestedBlocks_.erase(index);
            }
        }

        initialize();

        LastResult_ = result;
        return LastResult_;
    }
};

TPartReader::TPartReader(
    IBlocksReaderPtr reader,
    IChunkReader::TReadBlocksOptions readBlockOptions,
    const std::vector<TPartRange>& blockRanges)
    : Impl_(New<TImpl>(
        std::move(reader),
        std::move(readBlockOptions),
        blockRanges))
{ }

TPartReader::~TPartReader()
{ }

TFuture<TSharedRef> TPartReader::Produce(const TPartRange& range)
{
    return Impl_->Produce(range);
}

////////////////////////////////////////////////////////////////////////////////

class TPartEncoder::TImpl
    : public TRefCounted
{
public:
    TImpl(
        const ICodec* codec,
        const TPartIndexList missingPartIndices,
        const TParityPartSplitInfo& splitInfo,
        const std::vector<TPartRange>& encodeRanges,
        const std::vector<IPartBlockProducerPtr>& producers,
        const std::vector<IPartBlockConsumerPtr>& consumers)
        : Codec_(codec)
        , MissingPartIndices_(missingPartIndices)
        , ParityPartSplitInfo_(splitInfo)
        , EncodeRanges_(encodeRanges)
        , Producers_(producers)
        , Consumers_(consumers)
    { }

    TFuture<std::vector<TSharedRef>> ProduceBlocks(const TPartRange& range)
    {
        std::vector<TFuture<TSharedRef>> asyncBlocks;
        for (const auto& producer : Producers_) {
            asyncBlocks.push_back(producer->Produce(range));
        }
        return AllSucceeded(asyncBlocks);
    }

    TFuture<void> ConsumeBlocks(const TPartRange& range, const std::vector<TSharedRef>& blocks)
    {
        YT_VERIFY(blocks.size() == Consumers_.size());

        std::vector<TFuture<void>> asyncResults;
        for (int index = 0; index < std::ssize(blocks); ++index) {
            asyncResults.push_back(Consumers_[index]->Consume(range, blocks[index]));
        }
        return AllSucceeded(asyncResults);
    }

    void Run()
    {
        auto windowAlignedRanges = ParityPartSplitInfo_.SplitRangesByStripesAndAlignToParityBlocks(EncodeRanges_);

        for (const auto& range : windowAlignedRanges) {
            auto blocks = WaitFor(ProduceBlocks(range))
                .ValueOrThrow();

            std::vector<TSharedRef> decodedBlocks;
            if (GetParityPartIndices(Codec_) == MissingPartIndices_) {
                YT_VERIFY(std::ssize(blocks) == Codec_->GetDataPartCount());
                decodedBlocks = Codec_->Encode(blocks);
            } else {
                decodedBlocks = Codec_->Decode(blocks, MissingPartIndices_);
            }
            WaitFor(ConsumeBlocks(range, decodedBlocks))
                .ThrowOnError();
        }
    }

private:
    const ICodec* const Codec_;
    const TPartIndexList MissingPartIndices_;
    const TParityPartSplitInfo ParityPartSplitInfo_;
    const std::vector<TPartRange> EncodeRanges_;
    const std::vector<IPartBlockProducerPtr> Producers_;
    const std::vector<IPartBlockConsumerPtr> Consumers_;
};

////////////////////////////////////////////////////////////////////////////////

TPartEncoder::TPartEncoder(
    const ICodec* codec,
    const TPartIndexList missingPartIndices,
    const TParityPartSplitInfo& splitInfo,
    const std::vector<TPartRange>& encodeRanges,
    const std::vector<IPartBlockProducerPtr>& producers,
    const std::vector<IPartBlockConsumerPtr>& consumers)
    : Impl_(New<TPartEncoder::TImpl>(
        codec,
        missingPartIndices,
        splitInfo,
        encodeRanges,
        producers,
        consumers))
{ }

TPartEncoder::~TPartEncoder()
{ }

void TPartEncoder::Run()
{
    return Impl_->Run();
}

////////////////////////////////////////////////////////////////////////////////

TFuture<TRefCountedChunkMetaPtr> GetPlacementMeta(
    const IChunkReaderPtr& reader,
    const TClientChunkReadOptions& options)
{
    return reader->GetMeta(
        options,
        /*partitionTag*/ std::nullopt,
        std::vector<int>{ TProtoExtensionTag<NProto::TErasurePlacementExt>::Value });
}

////////////////////////////////////////////////////////////////////////////////

TDataBlocksPlacementInParts BuildDataBlocksPlacementInParts(
    const std::vector<int>& blockIndexes,
    const TErasurePlacementExt& placementExt,
    const TParityPartSplitInfo& paritySplitInfo)
{
    const auto& partInfos = placementExt.part_infos();
    YT_VERIFY(!partInfos.empty());
    YT_VERIFY(partInfos[0].first_block_index_per_stripe(0) == 0);

    auto result = TDataBlocksPlacementInParts(partInfos.size());

    for (int indexInRequest = 0; indexInRequest < std::ssize(blockIndexes); ++indexInRequest) {
        int blockIndex = blockIndexes[indexInRequest];
        YT_VERIFY(blockIndex >= 0);

        int stripeIndex;
        {
            auto it = std::upper_bound(
                partInfos[0].first_block_index_per_stripe().begin(),
                partInfos[0].first_block_index_per_stripe().end(),
                blockIndex
            );

            stripeIndex = (it - partInfos[0].first_block_index_per_stripe().begin()) - 1;
            YT_VERIFY(stripeIndex >= 0);
        }

        int partIndex;
        {
            auto it = std::partition_point(
                partInfos.begin(),
                partInfos.end(),
                [&] (const TPartInfo& partInfo) {
                    return partInfo.first_block_index_per_stripe(stripeIndex) <= blockIndex;
                }
            );

            partIndex = (it - partInfos.begin() - 1);
            while (partIndex > 0 && GetStripeBlockCount(placementExt, partIndex, stripeIndex) == 0) {
                --partIndex;
            }
            YT_VERIFY(partIndex >= 0);
        }

        int indexInStripe = blockIndex - partInfos[partIndex].first_block_index_per_stripe(stripeIndex);

        int indexInPart = 0;
        for (int index = 0; index < stripeIndex; ++index) {
            indexInPart += GetStripeBlockCount(placementExt, partIndex, index);
        }
        indexInPart += indexInStripe;

        i64 size = partInfos[partIndex].block_sizes(indexInPart);

        i64 offset = paritySplitInfo.GetStripeOffset(stripeIndex);
        for (int index = indexInPart - indexInStripe; index < indexInPart; ++index) {
            offset += partInfos[partIndex].block_sizes(index);
        }

        result[partIndex].IndexesInRequest.push_back(indexInRequest);
        result[partIndex].IndexesInPart.push_back(indexInPart);
        result[partIndex].Ranges.push_back({
            .Begin = offset,
            .End = offset + size
        });
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient::NErasureHelpers
