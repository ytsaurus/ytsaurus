#include "erasure_helpers.h"
#include "chunk_meta_extensions.h"
#include "chunk_reader.h"
#include "chunk_writer.h"
#include "dispatcher.h"
#include "public.h"

#include <yt/ytlib/misc/workload.h>

#include <yt/core/concurrency/scheduler.h>

#include <yt/core/misc/numeric_helpers.h>
#include <yt/core/misc/checksum.h>

#include <util/random/random.h>

namespace NYT {
namespace NChunkClient {
namespace NErasureHelpers {

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

TParityPartSplitInfo::TParityPartSplitInfo(int parityBlockCount, i64 parityBlockSize, i64 lastBlockSize)
    : BlockCount(parityBlockCount)
    , BlockSize(parityBlockSize)
    , LastBlockSize(lastBlockSize)
{
    YCHECK(BlockCount > 0);
    YCHECK(BlockSize > 0);
}

TParityPartSplitInfo TParityPartSplitInfo::Build(i64 parityBlockSize, i64 parityPartSize)
{
    int parityBlockCount = DivCeil(parityPartSize, parityBlockSize);
    i64 lastBlockSize = parityPartSize - (parityBlockSize * (parityBlockCount - 1));
    return TParityPartSplitInfo(parityBlockCount, parityBlockSize, lastBlockSize);
}

i64 TParityPartSplitInfo::GetPartSize() const
{
    return BlockSize * (BlockCount - 1) + LastBlockSize;
}

std::vector<TPartRange> TParityPartSplitInfo::GetRanges() const
{
    std::vector<TPartRange> result;
    for (int index = 0; index + 1 < BlockCount; ++index) {
        result.push_back(TPartRange({index * BlockSize, (index + 1) * BlockSize}));
    }
    result.push_back(TPartRange({(BlockCount - 1) * BlockSize, (BlockCount - 1) * BlockSize + LastBlockSize}));
    return result;
}

std::vector<i64> TParityPartSplitInfo::GetSizes() const
{
    std::vector<i64> result(BlockCount, BlockSize);
    result.back() = LastBlockSize;
    return result;
}

////////////////////////////////////////////////////////////////////////////////

class TPartWriter::TImpl
    : public TRefCounted
{
public:
    TImpl(IChunkWriterPtr writer, const std::vector<i64>& blockSizes, bool computeChecksum)
        : Writer_(writer)
        , BlockSizes_(blockSizes)
        , ComputeChecksum_(computeChecksum)
    { }

    TFuture<void> Consume(const TPartRange& range, const TSharedRef& block)
    {
        // Validate range monotonicity.
        YCHECK(range.Begin == Cursor_);
        Cursor_ = range.End;

        i64 blockPosition = 0;
        std::vector<TBlock> blocksToWrite;

        // Fill current block if it is started.
        if (PositionInCurrentBlock_ > 0) {
            i64 copySize = std::min(block.Size(), CurrentBlock_.Size() - PositionInCurrentBlock_);
            memcpy(CurrentBlock_.Begin() + PositionInCurrentBlock_, block.Begin(), copySize);
            PositionInCurrentBlock_ += copySize;
            blockPosition += copySize;

            if (PositionInCurrentBlock_ == CurrentBlock_.Size()) {
                blocksToWrite.push_back(TBlock(CurrentBlock_));

                ++CurrentBlockIndex_;
                PositionInCurrentBlock_ = 0;
            }
        }

        // Processing part blocks that fit inside given block.
        while (CurrentBlockIndex_ < BlockSizes_.size() &&
            blockPosition + BlockSizes_[CurrentBlockIndex_] <= block.Size())
        {
            auto size = BlockSizes_[CurrentBlockIndex_];
            blocksToWrite.push_back(TBlock(block.Slice(blockPosition, blockPosition + size)));
            blockPosition += size;
            ++CurrentBlockIndex_;
        }

        // Process part block that just overlap with given block.
        if (CurrentBlockIndex_ < BlockSizes_.size() && blockPosition < block.Size()) {
            CurrentBlock_ = TSharedMutableRef::Allocate(BlockSizes_[CurrentBlockIndex_]);
            i64 copySize = block.Size() - blockPosition;
            memcpy(CurrentBlock_.Begin(), block.Begin() + blockPosition, copySize);
            PositionInCurrentBlock_ = copySize;
        }

        if (ComputeChecksum_) {
            for (auto& block : blocksToWrite) {
                block.Checksum = block.GetOrComputeChecksum();
                BlockChecksums_.push_back(block.Checksum);
            }
        }

        if (!Writer_->WriteBlocks(blocksToWrite)) {
            return Writer_->GetReadyEvent();
        }
        return VoidFuture;
    }

    TChecksum GetPartChecksum() const
    {
        return CombineChecksums(BlockChecksums_);
    }

private:
    const IChunkWriterPtr Writer_;
    const std::vector<i64> BlockSizes_;
    const bool ComputeChecksum_;

    TSharedMutableRef CurrentBlock_;
    int CurrentBlockIndex_ = 0;
    size_t PositionInCurrentBlock_ = 0;
    i64 Cursor_ = 0;

    std::vector<TChecksum> BlockChecksums_;
};

TPartWriter::TPartWriter(IChunkWriterPtr writer, const std::vector<i64>& blockSizes, bool computeChecksum)
    : Impl_(New<TImpl>(writer, blockSizes, computeChecksum))
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
        const std::vector<i64>& blockSizes)
        : Reader_(reader)
        , BlockRanges_(SizesToConsecutiveRanges(blockSizes))
    { }

    virtual TFuture<TSharedRef> Produce(const TPartRange& range)
    {
        if (PreviousRange_ && (*PreviousRange_ == range)) {
            return MakeFuture(LastResult_);
        }

        if (PreviousRange_) {
            // Requested ranges must be monotonic.
            YCHECK(*PreviousRange_ == range || PreviousRange_->End <= range.Begin);
        }
        PreviousRange_ = range;

        // XXX(ignat): This may be optimized using monotonicity.
        std::vector<int> blockIndexes;
        for (int index = 0; index < BlockRanges_.size(); ++index) {
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
                [=, this_ = MakeStrong(this)] () {
                    return Reader_->ReadBlocks(indicesToRequest);
                })
                // Or simple Via?
                .AsyncVia(TDispatcher::Get()->GetReaderInvoker())
                .Run();

            return blocksFuture.Apply(BIND(
                [=, this_ = MakeStrong(this)] (const TErrorOr<std::vector<TBlock>>& errorOrBlocks) -> TSharedRef {
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
    const std::vector<TPartRange> BlockRanges_;

    THashMap<int, TSharedRef> RequestedBlocks_;
    TNullable<TPartRange> PreviousRange_;

    TSharedRef LastResult_;

    void OnBlocksRead(const std::vector<int>& indicesToRequest, const std::vector<TSharedRef>& blocks)
    {
        YCHECK(indicesToRequest.size() == blocks.size());
        for (int index = 0; index < indicesToRequest.size(); ++index) {
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
                result = TSharedMutableRef::Allocate<TErasureWriterSliceTag>(range.Size());
            }
        };

        i64 pos = 0;
        i64 currentStart = 0;

        for (int index = 0; index < BlockRanges_.size(); ++index) {
            i64 innerStart = std::max(static_cast<i64>(0), range.Begin - currentStart);
            i64 innerEnd = std::min(static_cast<i64>(BlockRanges_[index].Size()), range.End - currentStart);

            if (innerStart < innerEnd) {
                auto block = RequestedBlocks_[index];
                if (range.Size() == innerEnd - innerStart) {
                    LastResult_ = block.Slice(innerStart, innerEnd);
                    return LastResult_;
                }

                initialize();
                std::copy(block.Begin() + innerStart, block.Begin() + innerEnd, result.Begin() + pos);

                pos += (innerEnd - innerStart);

                // Cleanup blocks that we would not use anymore.
                if (range.End >= BlockRanges_[index].End) {
                    RequestedBlocks_.erase(index);
                }
            }
            currentStart += BlockRanges_[index].Size();

            if (pos == range.Size() || currentStart >= range.End) {
                break;
            }

        }

        initialize();

        LastResult_ = result;
        return LastResult_;
    }

    std::vector<TPartRange> SizesToConsecutiveRanges(const std::vector<i64>& sizes)
    {
        std::vector<TPartRange> ranges;
        i64 pos = 0;
        for (const auto& size : sizes) {
            i64 start = pos;
            i64 end = pos + size;
            ranges.push_back(TPartRange({start, end}));
            pos += size;
        }
        return ranges;
    }
};

TPartReader::TPartReader(
    IBlocksReaderPtr reader,
    const std::vector<i64>& blockSizes)
    : Impl_(New<TImpl>(
        reader,
        blockSizes))
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

    std::vector<TPartRange> GetSplitRanges(const TPartRange& range)
    {
        std::vector<TPartRange> ranges;
        i64 pos = range.Begin;
        while (pos < range.End) {
            TPartRange currentRange;
            currentRange.Begin = (pos / ParityPartSplitInfo_.BlockSize) * ParityPartSplitInfo_.BlockSize;
            currentRange.End = std::min(currentRange.Begin + ParityPartSplitInfo_.BlockSize, ParityPartSplitInfo_.GetPartSize());
            ranges.push_back(currentRange);
            pos = currentRange.End;
        }
        return ranges;
    }

    TFuture<std::vector<TSharedRef>> ProduceBlocks(const TPartRange& range)
    {
        std::vector<TFuture<TSharedRef>> asyncBlocks;
        for (const auto& producer : Producers_) {
            asyncBlocks.push_back(producer->Produce(range));
        }
        return Combine(asyncBlocks);
    }

    TFuture<void> ConsumeBlocks(const TPartRange& range, const std::vector<TSharedRef>& blocks)
    {
        YCHECK(blocks.size() == Consumers_.size());

        std::vector<TFuture<void>> asyncResults;
        for (int index = 0; index < blocks.size(); ++index) {
            asyncResults.push_back(Consumers_[index]->Consume(range, blocks[index]));
        }
        return Combine(asyncResults);
    }

    void Run()
    {
        for (const auto& range : EncodeRanges_) {
            auto windowRanges = GetSplitRanges(range);
            for (const auto& windowRange : windowRanges) {
                auto blocks = WaitFor(ProduceBlocks(windowRange))
                    .ValueOrThrow();

                std::vector<TSharedRef> decodedBlocks;
                if (GetParityPartIndices(Codec_) == MissingPartIndices_) {
                    YCHECK(blocks.size() == Codec_->GetDataPartCount());
                    decodedBlocks = Codec_->Encode(blocks);
                } else {
                    decodedBlocks = Codec_->Decode(blocks, MissingPartIndices_);
                }
                WaitFor(ConsumeBlocks(windowRange, decodedBlocks))
                    .ThrowOnError();
            }
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

void TPartEncoder::Run()
{
    return Impl_->Run();
}

////////////////////////////////////////////////////////////////////////////////

TFuture<NProto::TErasurePlacementExt> GetPlacementMeta(
    const IChunkReaderPtr& reader,
    const TWorkloadDescriptor& workloadDescriptor)
{
    return reader->GetMeta(
        workloadDescriptor,
        Null,
        std::vector<int>{
            TProtoExtensionTag<NProto::TErasurePlacementExt>::Value
        }).Apply(BIND([] (const NProto::TChunkMeta& meta) {
            return GetProtoExtension<NProto::TErasurePlacementExt>(meta.extensions());
        }));
}

TParityPartSplitInfo GetParityPartSplitInfo(const TErasurePlacementExt& placementExt)
{
    return TParityPartSplitInfo(
        placementExt.parity_block_count(),
        placementExt.parity_block_size(),
        placementExt.parity_last_block_size());
}

std::vector<i64> GetBlockSizes(int partIndex, const TErasurePlacementExt& placementExt)
{
    auto splitInfo = GetParityPartSplitInfo(placementExt);
    int dataPartCount = placementExt.part_infos().size();
    if (partIndex < dataPartCount) {
        const auto& blockSizesProto = placementExt.part_infos().Get(partIndex).block_sizes();
        return std::vector<i64>(blockSizesProto.begin(), blockSizesProto.end());
    } else {
        return splitInfo.GetSizes();
    }
}

////////////////////////////////////////////////////////////////////////////////

TDataBlocksPlacementInParts BuildDataBlocksPlacementInParts(
    const std::vector<int>& blockIndexes,
    const TErasurePlacementExt& placementExt)
{
    struct TPartComparer
    {
        bool operator()(int position, const TPartInfo& info) const
        {
            return position < info.first_block_index();
        }
    };

    auto partInfos = NYT::FromProto<std::vector<TPartInfo>>(placementExt.part_infos());
    YCHECK(partInfos.front().first_block_index() == 0);
    for (int i = 0; i + 1 < partInfos.size(); ++i) {
        YCHECK(partInfos[i].first_block_index() + partInfos[i].block_sizes().size() ==
            partInfos[i + 1].first_block_index());
    }

    auto result = TDataBlocksPlacementInParts(partInfos.size());

    int indexInRequest = 0;
    for (int blockIndex : blockIndexes) {
        YCHECK(blockIndex >= 0);

        // Binary search of part containing given block.
        auto it = std::upper_bound(partInfos.begin(), partInfos.end(), blockIndex, TPartComparer());
        YCHECK(it != partInfos.begin());
        do {
            --it;
        } while (it != partInfos.begin() && (it->first_block_index() > blockIndex || it->block_sizes().size() == 0));
        YCHECK(blockIndex >= it->first_block_index());
        YCHECK(it != partInfos.end());

        int partIndex = it - partInfos.begin();
        result[partIndex].IndexesInRequest.push_back(indexInRequest);

        // Calculate index in the part.
        int indexInPart = blockIndex - it->first_block_index();
        YCHECK(indexInPart < it->block_sizes().size());
        result[partIndex].IndexesInPart.push_back(indexInPart);

        // Calculate range of block inside part.
        i64 start = 0;
        int index = 0;
        for (; index < indexInPart; ++index) {
            start += it->block_sizes().Get(index);
        }
        auto range = TPartRange({start, start + it->block_sizes().Get(index)});
        result[partIndex].Ranges.push_back(range);

        ++indexInRequest;
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

TErasureChunkReaderBase::TErasureChunkReaderBase(
    NErasure::ICodec* codec,
    const std::vector<IChunkReaderAllowingRepairPtr>& readers)
    : Codec_(codec)
    , Readers_(readers)
{ }

TFuture<TChunkMeta> TErasureChunkReaderBase::GetMeta(
    const TWorkloadDescriptor& workloadDescriptor,
    const TNullable<int>& partitionTag,
    const TNullable<std::vector<int>>& extensionTags)
{
    YCHECK(!partitionTag);
    if (extensionTags) {
        for (const auto& forbiddenTag : {TProtoExtensionTag<TBlocksExt>::Value}) {
            auto it = std::find(extensionTags->begin(), extensionTags->end(), forbiddenTag);
            YCHECK(it == extensionTags->end());
        }
    }

    auto& reader = Readers_[RandomNumber(Readers_.size())];
    return reader->GetMeta(workloadDescriptor, partitionTag, extensionTags);
}

TChunkId TErasureChunkReaderBase::GetChunkId() const
{
    return Readers_.front()->GetChunkId();
}

TFuture<void> TErasureChunkReaderBase::PreparePlacementMeta(const TWorkloadDescriptor& workloadDescriptor)
{
    if (PlacementExtFuture_) {
        return PlacementExtFuture_;
    }

    {
        TGuard<TSpinLock> guard(PlacementExtLock_);

        if (!PlacementExtFuture_) {
            PlacementExtFuture_ = GetPlacementMeta(this, workloadDescriptor).Apply(
                BIND(&TErasureChunkReaderBase::OnGotPlacementMeta, MakeStrong(this))
                    .AsyncVia(TDispatcher::Get()->GetReaderInvoker()));
        }

        return PlacementExtFuture_;
    }
}

void TErasureChunkReaderBase::OnGotPlacementMeta(const TErasurePlacementExt& placementExt)
{
    PlacementExt_ = placementExt;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NErasureHelpers
} // namespace NChunkClient
} // namespace NYT

