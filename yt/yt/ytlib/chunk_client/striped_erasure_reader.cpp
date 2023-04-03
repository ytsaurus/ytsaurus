#include "striped_erasure_reader.h"

#include "private.h"
#include "block_fetcher.h"
#include "chunk_reader_allowing_repair.h"
#include "chunk_writer.h"
#include "chunk_meta_extensions.h"
#include "config.h"
#include "deferred_chunk_meta.h"
#include "dispatcher.h"

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>

#include <yt/yt/client/chunk_client/chunk_replica.h>

#include <yt/yt/library/erasure/impl/codec.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/misc/collection_helpers.h>

namespace NYT::NChunkClient {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ChunkClientLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TSegmentPartDescriptor
{
    int SegmentIndex;
    int PartIndex;
};

using TSegmentPartFetchPlan = std::vector<TSegmentPartDescriptor>;

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TSegmentPartFetcher
    : public TRefCounted
{
public:
    TSegmentPartFetcher(
        TErasureReaderConfigPtr config,
        const NErasure::ICodec* codec,
        std::vector<IChunkReaderPtr> partReaders,
        const TSegmentPartFetchPlan& plan,
        const NProto::TStripedErasurePlacementExt& placement,
        TChunkReaderMemoryManagerPtr memoryManager,
        IBlockCachePtr blockCache,
        const IChunkReader::TReadBlocksOptions& readBlocksOptions)
        : Codec_(codec)
        , Placement_(placement)
        , HeavyInvoker_(TDispatcher::Get()->GetReaderInvoker())
    {
        YT_VERIFY(!partReaders.empty());
        auto chunkId = ErasureChunkIdFromPartId(partReaders.front()->GetChunkId());

        for (int readerIndex = 0; readerIndex < std::ssize(partReaders); ++readerIndex) {
            const auto& partReader = partReaders[readerIndex];
            auto partIndex = ReplicaIndexFromErasurePartId(partReader->GetChunkId());

            EmplaceOrCrash(PartIndexToReaderIndex_, partIndex, readerIndex);
        }

        NErasure::TPartIndexList erasedIndices;
        for (int partIndex = 0; partIndex < Codec_->GetTotalPartCount(); ++partIndex) {
            if (!PartIndexToReaderIndex_.contains(partIndex)) {
                erasedIndices.push_back(partIndex);
            }
        }

        YT_VERIFY(Codec_->CanRepair(erasedIndices));

        std::vector<i64> segmentSizes;
        segmentSizes.reserve(placement.segment_block_counts_size());
        int blockIndex = 0;
        for (int segmentIndex = 0; segmentIndex < placement.segment_block_counts_size(); ++segmentIndex) {
            i64 segmentSize = 0;
            for (int index = 0; index < placement.segment_block_counts(segmentIndex); ++index) {
                segmentSize += placement.block_sizes(blockIndex++);
            }
            segmentSizes.push_back(segmentSize);
        }

        THashMap<int, NErasure::TPartIndexSet> segmentIndexToRequiredParts;
        for (const auto& descriptor : plan) {
            auto& segmentState = SegmentIndexToState_[descriptor.SegmentIndex];
            segmentState.Parts.resize(Codec_->GetTotalPartCount());
            segmentState.PartIndexToRequestCount.resize(Codec_->GetTotalPartCount());

            if (!PartIndexToReaderIndex_.contains(descriptor.PartIndex)) {
                segmentState.NeedRepair = true;
            }
            segmentIndexToRequiredParts[descriptor.SegmentIndex].set(descriptor.PartIndex);
        }

        std::vector<TBlockFetcher::TBlockInfo> blockInfos;
        THashSet<int> seenRepairBlockIndices;
        for (int segmentPartIndex = 0; segmentPartIndex < std::ssize(plan); ++segmentPartIndex) {
            const auto& descriptor = plan[segmentPartIndex];
            auto segmentIndex = descriptor.SegmentIndex;
            auto partIndex = descriptor.PartIndex;

            auto requestPart = [&] (int partIndex) {
                blockInfos.push_back({
                    .ReaderIndex = GetOrCrash(PartIndexToReaderIndex_, partIndex),
                    .BlockIndex = segmentIndex,
                    .Priority = segmentPartIndex,
                    .UncompressedDataSize = segmentSizes[segmentIndex] / Codec_->GetDataPartCount()
                });
            };

            auto& segmentState = SegmentIndexToState_[segmentIndex];
            if (segmentState.NeedRepair) {
                if (seenRepairBlockIndices.insert(segmentIndex).second) {
                    auto partsToFetch = GetPartsToFetch(GetOrCrash(segmentIndexToRequiredParts, segmentIndex));
                    if (!partsToFetch) {
                        THROW_ERROR_EXCEPTION("Unable to read segment %v of chunk %v with repair",
                            segmentIndex,
                            chunkId);
                    }
                    for (auto partIndex : *partsToFetch) {
                        requestPart(partIndex);
                    }
                }

                segmentState.PartIndexToRequestCount[partIndex]++;
            } else if (++segmentState.PartIndexToRequestCount[partIndex] > 0) {
                requestPart(partIndex);
            }
        }

        BlockFetcher_ = New<TBlockFetcher>(
            std::move(config),
            std::move(blockInfos),
            std::move(memoryManager),
            std::move(partReaders),
            std::move(blockCache),
            NCompression::ECodec::None,
            /*compressionRatio*/ 1.0,
            readBlocksOptions.ClientOptions,
            readBlocksOptions.SessionInvoker);
        BlockFetcher_->Start();
    }

    TFuture<TBlock> ReadSegmentPart(const TSegmentPartDescriptor& descriptor)
    {
        auto& segmentState = GetOrCrash(SegmentIndexToState_, descriptor.SegmentIndex);

        auto& requestCounter = segmentState.PartIndexToRequestCount[descriptor.PartIndex];

        auto& future = segmentState.Parts[descriptor.PartIndex];
        if (!future) {
            DoReadSegmentPart(descriptor);
            YT_VERIFY(future);
        }

        YT_VERIFY(requestCounter > 0);
        auto result = future;

        if (--requestCounter == 0) {
            future = {};
        }

        return result;
    }

private:
    const NErasure::ICodec* const Codec_;

    const NProto::TStripedErasurePlacementExt Placement_;

    const IInvokerPtr HeavyInvoker_;

    TBlockFetcherPtr BlockFetcher_;

    THashMap<int, int> PartIndexToReaderIndex_;

    struct TSegmentState
    {
        TCompactVector<int, TypicalReplicaCount> PartIndexToRequestCount;
        TCompactVector<TFuture<TBlock>, TypicalReplicaCount> Parts;

        bool NeedRepair = false;
    };
    THashMap<int, TSegmentState> SegmentIndexToState_;

    std::optional<NErasure::TPartIndexList> GetPartsToFetch(const NErasure::TPartIndexSet& requiredParts)
    {
        NErasure::TPartIndexSet partsToFetch;
        NErasure::TPartIndexList erasedParts;
        for (int partIndex = 0; partIndex < Codec_->GetTotalPartCount(); ++partIndex) {
            if (requiredParts.test(partIndex)) {
                if (PartIndexToReaderIndex_.contains(partIndex)) {
                    partsToFetch.set(partIndex);
                } else {
                    erasedParts.push_back(partIndex);
                }
            }
        }

        auto repairIndices = Codec_->GetRepairIndices(erasedParts);
        if (!repairIndices) {
            return {};
        }

        for (auto repairIndex : *repairIndices) {
            partsToFetch.set(repairIndex);
        }

        NErasure::TPartIndexList result;
        for (int partIndex = 0; partIndex < Codec_->GetTotalPartCount(); ++partIndex) {
            if (partsToFetch.test(partIndex)) {
                result.push_back(partIndex);
            }
        }

        return result;
    }

    void DoReadSegmentPart(const TSegmentPartDescriptor& descriptor)
    {
        const auto& segmentState = SegmentIndexToState_[descriptor.SegmentIndex];
        if (segmentState.NeedRepair) {
            DoRepairReadSegmentPart(descriptor);
        } else {
            DoRegularReadSegmentPart(descriptor);
        }
    }

    void DoRegularReadSegmentPart(const TSegmentPartDescriptor& descriptor)
    {
        auto partFuture = FetchSegmentPart(descriptor);
        auto& segmentState = GetOrCrash(SegmentIndexToState_, descriptor.SegmentIndex);
        YT_VERIFY(!segmentState.Parts[descriptor.PartIndex]);
        YT_VERIFY(segmentState.PartIndexToRequestCount[descriptor.PartIndex] > 0);
        segmentState.Parts[descriptor.PartIndex] = std::move(partFuture);
    }

    void DoRepairReadSegmentPart(const TSegmentPartDescriptor& descriptor)
    {
        auto getPartDescriptor = [descriptor] (int partIndex) {
            return TSegmentPartDescriptor{
                .SegmentIndex = descriptor.SegmentIndex,
                .PartIndex = partIndex,
            };
        };

        auto* segmentState = &GetOrCrash(SegmentIndexToState_, descriptor.SegmentIndex);

        NErasure::TPartIndexSet requiredParts;
        for (int partIndex = 0; partIndex < Codec_->GetTotalPartCount(); ++partIndex) {
            if (segmentState->PartIndexToRequestCount[partIndex] > 0) {
                requiredParts.set(partIndex);
            }
        }
        auto partsToFetch = *GetPartsToFetch(requiredParts);

        std::vector<TFuture<TBlock>> partFutures(Codec_->GetTotalPartCount(), MakeFuture(TBlock()));
        std::vector<bool> partFetched(Codec_->GetTotalPartCount());
        for (auto partIndex : partsToFetch) {
            partFutures[partIndex] = FetchSegmentPart(getPartDescriptor(partIndex));
            partFetched[partIndex] = true;
        }

        auto partsFuture = AllSucceeded(std::move(partFutures))
            .ApplyUnique(
                BIND([=, this, this_ = MakeStrong(this)] (std::vector<TBlock>&& parts) {
                    for (auto partIndex : partsToFetch) {
                        ValidateChecksum(getPartDescriptor(partIndex), &parts[partIndex]);
                    }

                    return parts;
                })
                .AsyncVia(HeavyInvoker_));

        NErasure::TPartIndexList erasedIndices;
        for (int partIndex = 0; partIndex < Codec_->GetTotalPartCount(); ++partIndex) {
            if (segmentState->PartIndexToRequestCount[partIndex] > 0) {
                if (partFetched[partIndex]) {
                    segmentState->Parts[partIndex] = partsFuture.Apply(BIND([=] (const std::vector<TBlock>& parts) {
                        return parts[partIndex];
                    }));
                } else {
                    erasedIndices.push_back(partIndex);
                }
            }
        }

        auto repairIndices = *Codec_->GetRepairIndices(erasedIndices);
        for (auto partIndex : repairIndices) {
            YT_VERIFY(partFetched[partIndex]);
        }

        auto repairFuture = partsFuture.ApplyUnique(
            BIND([=, this, this_ = MakeStrong(this)] (std::vector<TBlock>&& blocks) {
                std::vector<TSharedRef> repairParts;
                repairParts.reserve(repairIndices.size());
                for (auto partIndex : repairIndices) {
                    repairParts.push_back(blocks[partIndex].Data);
                }

                std::vector<TBlock> erasedParts;
                erasedParts.reserve(erasedIndices.size());
                for (auto erasedPart : Codec_->Decode(std::move(repairParts), erasedIndices)) {
                    erasedParts.push_back(TBlock(erasedPart));
                }

                for (int erasedPartIndex = 0; erasedPartIndex < std::ssize(erasedIndices); ++erasedPartIndex) {
                    auto partIndex = erasedIndices[erasedPartIndex];
                    ValidateChecksum(getPartDescriptor(partIndex), &erasedParts[erasedPartIndex]);
                }

                return erasedParts;
            })
            .AsyncVia(HeavyInvoker_));

        for (int erasedPartIndex = 0; erasedPartIndex < std::ssize(erasedIndices); ++erasedPartIndex) {
            auto partIndex = erasedIndices[erasedPartIndex];
            segmentState->Parts[partIndex] = repairFuture.Apply(BIND([=] (const std::vector<TBlock>& parts) {
                return parts[erasedPartIndex];
            }));
        }
    }

    TFuture<TBlock> FetchSegmentPart(const TSegmentPartDescriptor& descriptor)
    {
        auto readerIndex = GetOrCrash(PartIndexToReaderIndex_, descriptor.PartIndex);

        return BlockFetcher_->FetchBlock(readerIndex, descriptor.SegmentIndex)
            .ApplyUnique(
                BIND([=, this, this_ = MakeStrong(this)] (TBlock&& block) {
                    ValidateChecksum(descriptor, &block);

                    return block;
                })
                .AsyncVia(HeavyInvoker_));
    }

    void ValidateChecksum(
        const TSegmentPartDescriptor& descriptor,
        TBlock* block)
    {
        const auto& partInfo = Placement_.part_infos(descriptor.PartIndex);
        auto expectedChecksum = partInfo.segment_checksums(descriptor.SegmentIndex);

        block->Checksum = expectedChecksum;
        if (auto error = block->ValidateChecksum(); !error.IsOK()) {
            YT_LOG_ALERT(error);
            THROW_ERROR_EXCEPTION(error);
        }
    }
};

using TSegmentPartFetcherPtr = TIntrusivePtr<TSegmentPartFetcher>;

////////////////////////////////////////////////////////////////////////////////

class TErasureReaderSessionBase
    : public virtual TRefCounted
{
public:
    TErasureReaderSessionBase(
        const TErasureReaderConfigPtr config,
        const NErasure::ICodec* codec,
        std::vector<IChunkReaderAllowingRepairPtr> partReaders,
        TChunkReaderMemoryManagerPtr memoryManager,
        IBlockCachePtr blockCache,
        IChunkReader::TReadBlocksOptions readBlocksOptions)
        : Config_(std::move(config))
        , Codec_(codec)
        , PartReaders_(std::move(partReaders))
        , MemoryManager_(std::move(memoryManager))
        , BlockCache_(std::move(blockCache))
        , ReadBlocksOptions_(std::move(readBlocksOptions))
    { }

protected:
    const TErasureReaderConfigPtr Config_;

    const NErasure::ICodec* const Codec_;
    const std::vector<IChunkReaderAllowingRepairPtr> PartReaders_;

    const TChunkReaderMemoryManagerPtr MemoryManager_;
    const IBlockCachePtr BlockCache_;

    const IChunkReader::TReadBlocksOptions ReadBlocksOptions_;

    NProto::TStripedErasurePlacementExt PlacementExt_;

    TFuture<void> FetchPlacementExt()
    {
        const auto& reader = PartReaders_[RandomNumber(PartReaders_.size())];
        return reader->GetMeta(
            ReadBlocksOptions_.ClientOptions,
            /*partitionTag*/ std::nullopt,
            std::vector<int>{
                TProtoExtensionTag<NProto::TStripedErasurePlacementExt>::Value
            }).Apply(BIND([this, this_ = MakeStrong(this)] (const TRefCountedChunkMetaPtr& meta) {
                PlacementExt_ = GetProtoExtension<NProto::TStripedErasurePlacementExt>(meta->extensions());
            }));
    }
};

////////////////////////////////////////////////////////////////////////////////

class TErasureRepairSession
    : public TErasureReaderSessionBase
{
public:
    TErasureRepairSession(
        TErasureReaderConfigPtr config,
        const NErasure::ICodec* codec,
        std::vector<IChunkReaderAllowingRepairPtr> partReaders,
        std::vector<IChunkWriterPtr> partWriters,
        TChunkReaderMemoryManagerPtr memoryManager,
        IBlockCachePtr blockCache,
        IChunkReader::TReadBlocksOptions readBlocksOptions)
        : TErasureReaderSessionBase(
            std::move(config),
            std::move(codec),
            std::move(partReaders),
            std::move(memoryManager),
            std::move(blockCache),
            std::move(readBlocksOptions))
        , PartWriters_(std::move(partWriters))
    {
        for (const auto& writer : PartWriters_) {
            ErasedIndices_.push_back(ReplicaIndexFromErasurePartId(writer->GetChunkId()));
        }
    }

    TFuture<void> Run()
    {
        return BIND(&TErasureRepairSession::DoRun, MakeStrong(this))
            .AsyncVia(TDispatcher::Get()->GetReaderInvoker())
            .Run();
    }

private:
    const std::vector<IChunkWriterPtr> PartWriters_;
    NErasure::TPartIndexList ErasedIndices_;

    void DoRun()
    {
        // Open writers.
        {
            std::vector<TFuture<void>> futures;
            futures.reserve(PartWriters_.size());
            for (const auto& writer : PartWriters_) {
                futures.push_back(writer->Open());
            }
            WaitFor(AllSucceeded(std::move(futures)))
                .ThrowOnError();
        }

        // Fetch placement meta extension.
        WaitFor(FetchPlacementExt())
            .ThrowOnError();

        // Prepare segment part fetcher.
        TSegmentPartFetcherPtr fetcher;
        {
            TSegmentPartFetchPlan fetchPlan;
            for (int segmentIndex = 0; segmentIndex < PlacementExt_.segment_block_counts_size(); ++segmentIndex) {
                for (auto partIndex : ErasedIndices_) {
                    fetchPlan.push_back({
                        .SegmentIndex = segmentIndex,
                        .PartIndex = partIndex
                    });
                }
            }

            std::vector<IChunkReaderPtr> readers;
            readers.reserve(PartReaders_.size());
            for (const auto& reader : PartReaders_) {
                readers.push_back(reader);
            }

            fetcher = New<TSegmentPartFetcher>(
                Config_,
                Codec_,
                std::move(readers),
                fetchPlan,
                PlacementExt_,
                MemoryManager_,
                BlockCache_,
                ReadBlocksOptions_);
        }


        // Write erased parts.
        for (int segmentIndex = 0; segmentIndex < PlacementExt_.segment_block_counts_size(); ++segmentIndex) {
            for (int writerIndex = 0; writerIndex < std::ssize(PartWriters_); ++writerIndex) {
                auto partIndex = ErasedIndices_[writerIndex];
                auto segmentPart = WaitFor(fetcher->ReadSegmentPart(TSegmentPartDescriptor{
                    .SegmentIndex = segmentIndex,
                    .PartIndex = partIndex
                }))
                    .ValueOrThrow();

                const auto& writer = PartWriters_[writerIndex];
                if (!writer->WriteBlock(ReadBlocksOptions_.ClientOptions.WorkloadDescriptor, segmentPart)) {
                    WaitFor(writer->GetReadyEvent())
                        .ThrowOnError();
                }
            }
        }

        // Fetch chunk meta.
        const auto& reader = PartReaders_[RandomNumber(PartReaders_.size())];
        auto meta = WaitFor(reader->GetMeta(ReadBlocksOptions_.ClientOptions))
            .ValueOrThrow();

        auto deferredMeta = New<TDeferredChunkMeta>();
        deferredMeta->CopyFrom(*meta);
        deferredMeta->Finalize();

        // Close all writers.
        {
            std::vector<TFuture<void>> futures;
            futures.reserve(PartWriters_.size());
            for (const auto& writer : PartWriters_) {
                futures.push_back(writer->Close(
                    ReadBlocksOptions_.ClientOptions.WorkloadDescriptor,
                    deferredMeta));
            }
            WaitFor(AllSucceeded(std::move(futures)))
                .ThrowOnError();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TFuture<void> RepairErasedPartsStriped(
    TErasureReaderConfigPtr config,
    const NErasure::ICodec* codec,
    std::vector<IChunkReaderAllowingRepairPtr> partReaders,
    std::vector<IChunkWriterPtr> partWriters,
    TChunkReaderMemoryManagerPtr memoryManager,
    IBlockCachePtr blockCache,
    IChunkReader::TReadBlocksOptions readBlocksOptions)
{
    auto repairSession = New<TErasureRepairSession>(
        std::move(config),
        codec,
        std::move(partReaders),
        std::move(partWriters),
        std::move(memoryManager),
        std::move(blockCache),
        std::move(readBlocksOptions));
    return repairSession->Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
