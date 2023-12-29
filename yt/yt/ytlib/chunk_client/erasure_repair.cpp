#include "erasure_repair.h"

#include "chunk_reader.h"
#include "chunk_writer.h"
#include "config.h"
#include "deferred_chunk_meta.h"
#include "dispatcher.h"
#include "erasure_helpers.h"
#include "private.h"
#include "chunk_reader_options.h"
#include "chunk_reader_statistics.h"
#include "erasure_adaptive_repair.h"

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/concurrency/scheduler.h>
#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/misc/checksum.h>

#include <yt/yt/library/erasure/impl/codec.h>

#include <util/random/random.h>

#include <numeric>

namespace NYT::NChunkClient {

using namespace NErasure;
using namespace NConcurrency;
using namespace NChunkClient::NProto;
using namespace NErasureHelpers;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

//! Caching chunk reader that assumes monotonic requests for block indexes with possible overlaps.
//! Also supports functionality to save blocks with given indexes.
class TSequentialCachingBlocksReader
    : public IBlocksReader
{
public:
    TSequentialCachingBlocksReader(
        IChunkReaderPtr reader,
        const IChunkReader::TReadBlocksOptions& options,
        const std::vector<int>& blocksToSave = {})
        : UnderlyingReader_(reader)
        , ReadBlocksOptions_(options)
        , BlocksToSave_(blocksToSave)
        , SavedBlocks_(blocksToSave.size())
    {
        for (size_t index = 0; index < blocksToSave.size(); ++index) {
            BlockIndexToBlocksToSaveIndex_[blocksToSave[index]] = index;
        }
    }

    TFuture<std::vector<TBlock>> ReadBlocks(const std::vector<int>& blockIndexes) override
    {
        if (blockIndexes.empty()) {
            return MakeFuture(std::vector<TBlock>());
        }

        while (!CachedBlocks_.empty() && CachedBlocks_.front().first < blockIndexes.front()) {
            CachedBlocks_.pop_front();
        }

        std::vector<TBlock> resultBlocks;

        int index = 0;
        while (index < std::ssize(blockIndexes) && index < std::ssize(CachedBlocks_)) {
            resultBlocks.push_back(CachedBlocks_[index].second);
            ++index;
        }

        YT_VERIFY(index == std::ssize(CachedBlocks_));

        if (index < std::ssize(blockIndexes)) {
            auto blockIndexesToRequest = std::vector<int>(blockIndexes.begin() + index, blockIndexes.end());
            auto blocksFuture = UnderlyingReader_->ReadBlocks(
                ReadBlocksOptions_,
                blockIndexesToRequest);
            return blocksFuture.Apply(BIND([=, this, this_ = MakeStrong(this)] (const std::vector<TBlock>& blocks) mutable {
                for (int index = 0; index < std::ssize(blockIndexesToRequest); ++index) {
                    auto blockIndex = blockIndexesToRequest[index];
                    auto block = blocks[index];
                    auto it = BlockIndexToBlocksToSaveIndex_.find(blockIndex);
                    if (it != BlockIndexToBlocksToSaveIndex_.end()) {
                        SavedBlocks_[it->second] = block;
                    }
                    CachedBlocks_.push_back(std::pair(blockIndex, block));
                }
                resultBlocks.insert(resultBlocks.end(), blocks.begin(), blocks.end());
                return resultBlocks;
            }));
        } else {
            return MakeFuture(resultBlocks);
        }
    }

    TFuture<void> ReadMissingBlocksToSave()
    {
        std::vector<int> indexesToRead;
        THashMap<int, int> blockIndexToSavedBlocksIndex;
        int counter = 0;
        for (int index = 0; index < std::ssize(BlocksToSave_); ++index) {
            if (!SavedBlocks_[index]) {
                indexesToRead.push_back(BlocksToSave_[index]);
                blockIndexToSavedBlocksIndex[counter++] = index;
            }
        }
        auto blocksFuture = UnderlyingReader_->ReadBlocks(
            ReadBlocksOptions_,
            indexesToRead);
        return blocksFuture.Apply(BIND([=, this, this_ = MakeStrong(this)] (const std::vector<TBlock>& blocks) mutable {
            for (int index = 0; index < std::ssize(blocks); ++index) {
                int savedBlocksIndex = GetOrCrash(blockIndexToSavedBlocksIndex, index);
                SavedBlocks_[savedBlocksIndex] = blocks[index];
            }
        }));
    }

    std::vector<TBlock> GetSavedBlocks() const
    {
        std::vector<TBlock> result;
        for (const auto& blockOrNull : SavedBlocks_) {
            YT_VERIFY(blockOrNull);
            result.push_back(*blockOrNull);
        }
        return result;
    }

private:
    const IChunkReaderPtr UnderlyingReader_;
    const IChunkReader::TReadBlocksOptions ReadBlocksOptions_;
    const std::vector<int> BlocksToSave_;
    THashMap<int, int> BlockIndexToBlocksToSaveIndex_;

    std::vector<std::optional<TBlock>> SavedBlocks_;
    std::deque<std::pair<int, TBlock>> CachedBlocks_;
};

DECLARE_REFCOUNTED_TYPE(TSequentialCachingBlocksReader)
DEFINE_REFCOUNTED_TYPE(TSequentialCachingBlocksReader)

////////////////////////////////////////////////////////////////////////////////

class TRepairAllPartsSession
    : public TRefCounted
{
public:
    TRepairAllPartsSession(
        ICodec* codec,
        const TPartIndexList& erasedIndices,
        const std::vector<IChunkReaderAllowingRepairPtr>& readers,
        const std::vector<IChunkWriterPtr>& writers,
        const IChunkReader::TReadBlocksOptions& options)
        : Codec_(codec)
        , Readers_(readers)
        , Writers_(writers)
        , ErasedIndices_(erasedIndices)
        , ReadBlocksOptions_(options)
    {
        YT_VERIFY(erasedIndices.size() == writers.size());
    }

    TFuture<void> Run()
    {
        if (Readers_.empty()) {
            return VoidFuture;
        }

        return BIND(&TRepairAllPartsSession::DoRun, MakeStrong(this))
            .AsyncVia(TDispatcher::Get()->GetReaderInvoker())
            .Run();
    }

private:
    const ICodec* const Codec_;
    const std::vector<IChunkReaderAllowingRepairPtr> Readers_;
    const std::vector<IChunkWriterPtr> Writers_;
    const TPartIndexList ErasedIndices_;
    const IChunkReader::TReadBlocksOptions ReadBlocksOptions_;

    TParityPartSplitInfo ParityPartSplitInfo_;

    std::vector<std::vector<TPartRange>> ErasedPartBlockRanges_;
    std::vector<std::vector<TPartRange>> RepairPartBlockRanges_;

    i64 ErasedDataSize_ = 0;
    int ErasedBlockCount_ = 0;

    void DoRun()
    {
        // Open writers.
        {
            std::vector<TFuture<void>> asyncResults;
            for (auto writer : Writers_) {
                asyncResults.push_back(writer->Open());
            }
            WaitFor(AllSucceeded(asyncResults))
                .ThrowOnError();
        }

        // Get placement extension.
        auto placementMeta = WaitFor(GetPlacementMeta(
            Readers_.front(),
            ReadBlocksOptions_.ClientOptions))
            .ValueOrThrow();
        auto placementExt = GetProtoExtension<NProto::TErasurePlacementExt>(placementMeta->extensions());
        ProcessPlacementExt(placementExt);

        // Prepare erasure part readers.
        std::vector<IPartBlockProducerPtr> blockProducers;
        for (int index = 0; index < std::ssize(Readers_); ++index) {
            auto monotonicReader = New<TSequentialCachingBlocksReader>(
                Readers_[index],
                ReadBlocksOptions_);
            blockProducers.push_back(New<TPartReader>(
                monotonicReader,
                ReadBlocksOptions_,
                RepairPartBlockRanges_[index]));
        }

        // Prepare erasure part writers.
        std::vector<TPartWriterPtr> writerConsumers;
        std::vector<IPartBlockConsumerPtr> blockConsumers;
        for (int index = 0; index < std::ssize(Writers_); ++index) {
            writerConsumers.push_back(New<TPartWriter>(
                ReadBlocksOptions_.ClientOptions.WorkloadDescriptor,
                Writers_[index],
                ErasedPartBlockRanges_[index],
                /*computeChecksums*/ true));
            blockConsumers.push_back(writerConsumers.back());
        }

        // Run encoder.
        std::vector<TPartRange> ranges(1, TPartRange{0, ParityPartSplitInfo_.GetPartSize()});
        auto encoder = New<TPartEncoder>(
            Codec_,
            ErasedIndices_,
            ParityPartSplitInfo_,
            ranges,
            blockProducers,
            blockConsumers);
        encoder->Run();

        // Fetch chunk meta.
        const auto& reader = Readers_[RandomNumber(Readers_.size())];
        auto meta = WaitFor(reader->GetMeta(ReadBlocksOptions_.ClientOptions))
            .ValueOrThrow();
        auto deferredMeta = New<TDeferredChunkMeta>();
        deferredMeta->CopyFrom(*meta);
        deferredMeta->Finalize();

        // Validate repaired parts checksums.
        if (placementExt.part_checksums_size() != 0) {
            YT_VERIFY(placementExt.part_checksums_size() == Codec_->GetTotalPartCount());

            for (int index = 0; index < std::ssize(Writers_); ++index) {
                TChecksum repairedPartChecksum = writerConsumers[index]->GetPartChecksum();
                TChecksum expectedPartChecksum = placementExt.part_checksums(ErasedIndices_[index]);

                YT_VERIFY(expectedPartChecksum == NullChecksum || repairedPartChecksum == expectedPartChecksum);
            }
        }

        // Close all writers.
        {
            std::vector<TFuture<void>> asyncResults;
            for (auto writer : Writers_) {
                asyncResults.push_back(writer->Close(ReadBlocksOptions_.ClientOptions.WorkloadDescriptor, deferredMeta));
            }
            WaitFor(AllSucceeded(asyncResults))
                .ThrowOnError();
        }
    }

    void ProcessPlacementExt(const TErasurePlacementExt& placementExt)
    {
        ParityPartSplitInfo_ = TParityPartSplitInfo(placementExt);

        auto repairIndices = Codec_->GetRepairIndices(ErasedIndices_);
        YT_VERIFY(repairIndices);
        YT_VERIFY(repairIndices->size() == Readers_.size());

        for (int i = 0; i < std::ssize(Readers_); ++i) {
            int repairIndex = (*repairIndices)[i];
            auto blockRanges = ParityPartSplitInfo_.GetBlockRanges(repairIndex, placementExt);
            RepairPartBlockRanges_.push_back(blockRanges);
        }

        for (int erasedIndex : ErasedIndices_) {
            auto blockRanges = ParityPartSplitInfo_.GetBlockRanges(erasedIndex, placementExt);
            ErasedPartBlockRanges_.push_back(blockRanges);
            ErasedBlockCount_ += blockRanges.size();
            for (auto range : blockRanges) {
                ErasedDataSize_ += range.Size();
            }
        }
    }
};

TFuture<void> RepairErasedParts(
    NErasure::ICodec* codec,
    const NErasure::TPartIndexList& erasedIndices,
    const std::vector<IChunkReaderAllowingRepairPtr>& readers,
    const std::vector<IChunkWriterPtr>& writers,
    const IChunkReader::TReadBlocksOptions& options)
{
    auto session = New<TRepairAllPartsSession>(
        codec,
        erasedIndices,
        readers,
        writers,
        options);
    return session->Run();
}

////////////////////////////////////////////////////////////////////////////////

class TPartBlockSaver
    : public IPartBlockConsumer
{
public:
    TPartBlockSaver(const std::vector<TPartRange>& ranges)
        : Ranges_(ranges)
        , Blocks_(ranges.size())
    {
        for (int index = 0; index < std::ssize(Ranges_); ++index) {
            auto size = Ranges_[index].Size();
            Blocks_[index] = TSharedMutableRef::Allocate(size);
            TotalBytes_ += size;
        }
    }

    TFuture<void> Consume(const TPartRange& range, const TSharedRef& block) override
    {
        if (LastRange_ && *LastRange_ == range) {
            return VoidFuture;
        }

        YT_VERIFY(!LastRange_ || LastRange_->End <= range.Begin);
        LastRange_ = range;

        for (int index = 0; index < std::ssize(Ranges_); ++index) {
            auto blockRange = Ranges_[index];
            auto intersection = Intersection(blockRange, range);
            if (!intersection) {
                continue;
            }
            memcpy(
                Blocks_[index].Begin() + (intersection.Begin - blockRange.Begin),
                block.Begin() + (intersection.Begin - range.Begin),
                intersection.Size());
            SavedBytes_ += intersection.Size();
        }

        return VoidFuture;
    }

    std::vector<TBlock> GetSavedBlocks()
    {
        YT_VERIFY(TotalBytes_ == SavedBytes_);
        std::vector<TBlock> result;
        for (const auto& block : Blocks_) {
            result.emplace_back(TSharedRef(block));
        }
        return result;
    }

private:
    const std::vector<TPartRange> Ranges_;

    std::vector<TSharedMutableRef> Blocks_;
    i64 TotalBytes_ = 0;
    i64 SavedBytes_ = 0;

    std::optional<TPartRange> LastRange_;
};

class TEmptyPartBlockConsumer
    : public IPartBlockConsumer
{
public:
    TFuture<void> Consume(const TPartRange& /*range*/, const TSharedRef& /*block*/) override
    {
        return MakeFuture(TError());
    }
};

DECLARE_REFCOUNTED_TYPE(TPartBlockSaver)
DEFINE_REFCOUNTED_TYPE(TPartBlockSaver)

class TRepairingErasureReaderSession
    : public TRefCounted
{
public:
    TRepairingErasureReaderSession(
        TChunkId chunkId,
        ICodec* codec,
        TPartIndexList erasedIndices,
        std::vector<IChunkReaderAllowingRepairPtr> readers,
        TErasurePlacementExt placementExt,
        std::vector<int> blockIndexes,
        IChunkReader::TReadBlocksOptions options,
        IInvokerPtr readerInvoker,
        TLogger logger)
        : ChunkId_(chunkId)
        , Codec_(codec)
        , ErasedIndices_(std::move(erasedIndices))
        , Readers_(std::move(readers))
        , PlacementExt_(std::move(placementExt))
        , BlockIndexes_(std::move(blockIndexes))
        , ReadBlocksOptions_(std::move(options))
        , ReaderInvoker_(std::move(readerInvoker))
        , Logger(std::move(logger))
        , ParityPartSplitInfo_(PlacementExt_)
        , DataBlocksPlacementInParts_(BuildDataBlocksPlacementInParts(
            BlockIndexes_,
            PlacementExt_,
            ParityPartSplitInfo_))
    {
        auto repairIndices = *Codec_->GetRepairIndices(ErasedIndices_);
        YT_VERIFY(std::is_sorted(ErasedIndices_.begin(), ErasedIndices_.end()));
        YT_VERIFY(std::is_sorted(repairIndices.begin(), repairIndices.end()));

        for (int partIndex : repairIndices) {
            RepairPartBlockRanges_.push_back(
                ParityPartSplitInfo_.GetBlockRanges(partIndex, PlacementExt_));
        }
        for (int erasedIndex : ErasedIndices_) {
            ErasedPartBlockRanges_.push_back(
                ParityPartSplitInfo_.GetBlockRanges(erasedIndex, PlacementExt_));
        }

        auto dataPartCount = Codec_->GetDataPartCount();

        std::vector<TPartRange> repairRanges;

        // Index in Readers_ array, we consider part in ascending order and support index of current reader.
        int readerIndex = 0;

        // Prepare data part readers and block savers.
        for (int partIndex = 0; partIndex < dataPartCount; ++partIndex) {
            auto blocksPlacementInPart = DataBlocksPlacementInParts_[partIndex];
            if (std::binary_search(ErasedIndices_.begin(), ErasedIndices_.end(), partIndex)) {
                PartBlockSavers_.push_back(New<TPartBlockSaver>(blocksPlacementInPart.Ranges));
                repairRanges.insert(
                    repairRanges.end(),
                    blocksPlacementInPart.Ranges.begin(),
                    blocksPlacementInPart.Ranges.end());
            } else {
                auto partReader = New<TSequentialCachingBlocksReader>(
                    Readers_[readerIndex++],
                    ReadBlocksOptions_,
                    blocksPlacementInPart.IndexesInPart);
                AllPartReaders_.push_back(partReader);
                if (std::binary_search(repairIndices.begin(), repairIndices.end(), partIndex)) {
                    RepairPartReaders_.push_back(partReader);
                }
            }
        }

        // Finish building repair part readers.
        for (auto partIndex : repairIndices) {
            if (partIndex >= dataPartCount) {
                RepairPartReaders_.push_back(New<TSequentialCachingBlocksReader>(
                    Readers_[readerIndex++],
                    ReadBlocksOptions_));
            }
        }

        // Build part block producers.
        for (int index = 0; index < std::ssize(repairIndices); ++index) {
            BlockProducers_.push_back(New<TPartReader>(
                RepairPartReaders_[index],
                ReadBlocksOptions_,
                RepairPartBlockRanges_[index]));
        }

        // Build part block consumers.
        BlockConsumers_.insert(BlockConsumers_.end(), PartBlockSavers_.begin(), PartBlockSavers_.end());
        for (auto partIndex : ErasedIndices_) {
            if (partIndex >= dataPartCount) {
                BlockConsumers_.push_back(New<TEmptyPartBlockConsumer>());
            }
        }

        // Simplify repair ranges.
        RepairRanges_ = Union(repairRanges);
    }

    TFuture<std::vector<TBlock>> Run()
    {
        return BIND(&TRepairingErasureReaderSession::RepairBlocks, MakeStrong(this))
            .AsyncVia(ReaderInvoker_)
            .Run()
            .Apply(BIND(&TRepairingErasureReaderSession::ReadRemainingBlocks, MakeStrong(this)))
            .Apply(BIND(&TRepairingErasureReaderSession::BuildResult, MakeStrong(this)));
    }

private:
    const TChunkId ChunkId_;
    const ICodec* const Codec_;
    const TPartIndexList ErasedIndices_;
    const std::vector<IChunkReaderAllowingRepairPtr> Readers_;
    const TErasurePlacementExt PlacementExt_;
    const std::vector<int> BlockIndexes_;
    const IChunkReader::TReadBlocksOptions ReadBlocksOptions_;
    const IInvokerPtr ReaderInvoker_;
    const TLogger Logger;

    TParityPartSplitInfo ParityPartSplitInfo_;
    TDataBlocksPlacementInParts DataBlocksPlacementInParts_;
    std::vector<std::vector<TPartRange>> ErasedPartBlockRanges_;
    std::vector<std::vector<TPartRange>> RepairPartBlockRanges_;

    std::vector<TSequentialCachingBlocksReaderPtr> AllPartReaders_;
    std::vector<TSequentialCachingBlocksReaderPtr> RepairPartReaders_;
    std::vector<TPartBlockSaverPtr> PartBlockSavers_;

    std::vector<IPartBlockProducerPtr> BlockProducers_;
    std::vector<IPartBlockConsumerPtr> BlockConsumers_;

    std::vector<TPartRange> RepairRanges_;


    void RepairBlocks()
    {
        auto encoder = New<TPartEncoder>(
            Codec_,
            ErasedIndices_,
            ParityPartSplitInfo_,
            RepairRanges_,
            BlockProducers_,
            BlockConsumers_);
        encoder->Run();
    }

    void ReadRemainingBlocks()
    {
        std::vector<TFuture<void>> asyncResults;
        for (auto reader : AllPartReaders_) {
            asyncResults.push_back(reader->ReadMissingBlocksToSave());
        }
        WaitFor(AllSucceeded(asyncResults))
            .ThrowOnError();
    }

    std::vector<TBlock> BuildResult()
    {
        std::vector<TBlock> result(BlockIndexes_.size());
        int partBlockSaverIndex = 0;
        int partReaderIndex = 0;
        for (int partIndex = 0; partIndex < Codec_->GetDataPartCount(); ++partIndex) {
            auto blocksPlacementInPart = DataBlocksPlacementInParts_[partIndex];

            std::vector<TBlock> blocks;
            bool isRepairedPart = std::binary_search(ErasedIndices_.begin(), ErasedIndices_.end(), partIndex);
            if (isRepairedPart) {
                blocks = PartBlockSavers_[partBlockSaverIndex++]->GetSavedBlocks();
            } else {
                blocks = AllPartReaders_[partReaderIndex++]->GetSavedBlocks();
            }

            for (int index = 0; index < std::ssize(blocksPlacementInPart.IndexesInRequest); ++index) {
                int indexInRequest = blocksPlacementInPart.IndexesInRequest[index];

                if (isRepairedPart && PlacementExt_.block_checksums_size() != 0) {
                    int blockIndex = BlockIndexes_[indexInRequest];
                    YT_VERIFY(blockIndex < PlacementExt_.block_checksums_size());

                    TChecksum actualChecksum = GetChecksum(blocks[index].Data);
                    TChecksum expectedChecksum = PlacementExt_.block_checksums(blockIndex);

                    if (actualChecksum != expectedChecksum) {
                        auto error = TError("Invalid block checksum in repaired part")
                            << TErrorAttribute("chunk_id", ChunkId_)
                            << TErrorAttribute("block_index", blockIndex)
                            << TErrorAttribute("expected_checksum", expectedChecksum)
                            << TErrorAttribute("actual_checksum", actualChecksum)
                            << TErrorAttribute("recalculated_checksum", GetChecksum(blocks[index].Data));

                        YT_LOG_ALERT(error);
                        THROW_ERROR error;
                    }
                }
                result[indexInRequest] = blocks[index];
            }
        }
        return result;
    }
};

TFuture<std::vector<TBlock>> ExecuteErasureRepairingSession(
    TChunkId chunkId,
    NErasure::ICodec* codec,
    NErasure::TPartIndexList erasedIndices,
    std::vector<IChunkReaderAllowingRepairPtr> readers,
    std::vector<int> blockIndexes,
    IChunkReader::TReadBlocksOptions options,
    IInvokerPtr readerInvoker,
    NLogging::TLogger logger,
    NChunkClient::NProto::TErasurePlacementExt placementExt)
{
    return New<TRepairingErasureReaderSession>(
        chunkId,
        codec,
        std::move(erasedIndices),
        std::move(readers),
        std::move(placementExt),
        std::move(blockIndexes),
        std::move(options),
        std::move(readerInvoker),
        std::move(logger))
        ->Run();
}

////////////////////////////////////////////////////////////////////////////////

class TNullChunkWriter
    : public IChunkWriter
{
public:
    TFuture<void> Open() override
    {
        return VoidFuture;
    }

    TFuture<void> Cancel() override
    {
        return VoidFuture;
    }

    bool WriteBlock(const TWorkloadDescriptor&, const TBlock&) override
    {
        return true;
    }

    bool WriteBlocks(const TWorkloadDescriptor&, const std::vector<TBlock>&) override
    {
        return true;
    }

    TFuture<void> GetReadyEvent() override
    {
        return VoidFuture;
    }

    TFuture<void> Close(const TWorkloadDescriptor&, const TDeferredChunkMetaPtr&) override
    {
        return VoidFuture;
    }

    const NChunkClient::NProto::TChunkInfo& GetChunkInfo() const override
    {
        YT_UNIMPLEMENTED();
    }

    const NChunkClient::NProto::TDataStatistics& GetDataStatistics() const override
    {
        YT_UNIMPLEMENTED();
    }

    TChunkReplicaWithLocationList GetWrittenChunkReplicas() const override
    {
        YT_UNIMPLEMENTED();
    }

    TChunkId GetChunkId() const override
    {
        YT_UNIMPLEMENTED();
    }

    NErasure::ECodec GetErasureCodecId() const override
    {
        YT_UNIMPLEMENTED();
    }

    bool IsCloseDemanded() const override
    {
        YT_UNIMPLEMENTED();
    }
};


//! Creates chunk writers for repaired parts.
//! param #erasedPartIndices -- holds indices that job asked to repair.
//! param #bannedPartIndices -- is a superset of #erasedPartIndices, contains indices that currently unavailable.
std::vector<IChunkWriterPtr> CreateWritersForRepairing(
    const TPartIndexList& erasedPartIndices,
    const TPartIndexList& bannedPartIndices,
    TPartWriterFactory factory)
{
    TPartIndexSet requiredSet;
    for (auto partIndex : erasedPartIndices) {
        requiredSet.set(partIndex);
    }

    std::vector<IChunkWriterPtr> writers;
    writers.reserve(bannedPartIndices.size());

    //! All unavailable parts should be repaired, but result must be saved only for erased parts.
    for (auto partIndex : bannedPartIndices) {
        if (requiredSet.test(partIndex)) {
            writers.push_back(factory(partIndex));
        } else {
            writers.push_back(New<TNullChunkWriter>());
        }
    }

    return writers;
}

////////////////////////////////////////////////////////////////////////////////

TFuture<void> CancelWriters(const std::vector<IChunkWriterPtr>& writers)
{
    std::vector<TFuture<void>> futures;
    futures.reserve(writers.size());

    for (auto& writer : writers) {
        futures.push_back(writer->Cancel());
    }

    return AllSucceeded(std::move(futures));
}

////////////////////////////////////////////////////////////////////////////////

TFuture<void> AdaptiveRepairErasedParts(
    TChunkId chunkId,
    ICodec* codec,
    TErasureReaderConfigPtr config,
    const TPartIndexList& erasedIndices,
    const std::vector<IChunkReaderAllowingRepairPtr>& allReaders,
    TPartWriterFactory writerFactory,
    const IChunkReader::TReadBlocksOptions& options,
    const NLogging::TLogger& logger,
    NProfiling::TCounter adaptivelyRepairedCounter)
{
    auto invoker = TDispatcher::Get()->GetReaderInvoker();
    auto observer = New<TRepairingReadersObserver>(codec, config, invoker, allReaders);

    auto session = New<TAdaptiveErasureRepairingSession>(
        chunkId,
        codec,
        observer,
        allReaders,
        invoker,
        TAdaptiveErasureRepairingSession::TTarget{
            .Erased = erasedIndices,
        },
        logger,
        std::move(adaptivelyRepairedCounter));

    return session->Run<void>(
        [=] (const TPartIndexList& bannedIndices, const std::vector<IChunkReaderAllowingRepairPtr>& availableReaders) {
            auto writers = CreateWritersForRepairing(erasedIndices, bannedIndices, writerFactory);
            YT_VERIFY(writers.size() == bannedIndices.size());

            auto future = RepairErasedParts(codec, bannedIndices, availableReaders, writers, options);
            return future.Apply(BIND([writers, Logger = logger] (const TError& error) {
                if (error.IsOK()) {
                    return MakeFuture(error);
                }

                auto cancelResults = WaitFor(CancelWriters(writers));
                if (!cancelResults.IsOK()) {
                    YT_LOG_WARNING(cancelResults, "Failed to cancel chunk writers");
                    return MakeFuture(TError(
                        NChunkClient::EErrorCode::UnrecoverableRepairError,
                        "Failed to cancel chunk writers")
                        << cancelResults);
                }

                return MakeFuture(error);
            }));
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient

