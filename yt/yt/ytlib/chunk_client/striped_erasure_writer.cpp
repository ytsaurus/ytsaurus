#include "striped_erasure_writer.h"

#include "block.h"
#include "block_reorderer.h"
#include "chunk_meta_extensions.h"
#include "chunk_writer.h"
#include "deferred_chunk_meta.h"
#include "dispatcher.h"
#include "session_id.h"

#include <yt/yt/ytlib/chunk_client/proto/chunk_info.pb.h>

#include <yt/yt/client/chunk_client/chunk_replica.h>
#include <yt/yt/client/chunk_client/config.h>

#include <yt/yt/client/misc/workload.h>

#include <yt/yt/library/erasure/impl/codec.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/misc/atomic_object.h>
#include <yt/yt/core/misc/numeric_helpers.h>

#include <yt/yt/core/rpc/dispatcher.h>

namespace NYT::NChunkClient {

using namespace NConcurrency;

using ::NYT::FromProto;
using ::NYT::ToProto;

////////////////////////////////////////////////////////////////////////////////

class TStripedErasureWriter
    : public IChunkWriter
{
public:
    TStripedErasureWriter(
        TErasureWriterConfigPtr config,
        NErasure::ECodec codecId,
        TSessionId sessionId,
        const TWorkloadDescriptor& workloadDescriptor,
        std::vector<IChunkWriterPtr> writers)
        : Config_(std::move(config))
        , Codec_(NErasure::GetCodec(codecId))
        , SessionId_(sessionId)
        , WorkloadDescriptor_(workloadDescriptor)
        , Writers_(std::move(writers))
        , WriterReadyEvents_(Writers_.size(), VoidFuture)
        , WriterInvoker_(TDispatcher::Get()->GetWriterInvoker())
        , HeavyInvoker_(CreateFixedPriorityInvoker(
            NRpc::TDispatcher::Get()->GetPrioritizedCompressionPoolInvoker(),
            workloadDescriptor.GetPriority()))
        , BlockReorderer_(Config_)
    {
        YT_VERIFY(std::ssize(Writers_) == Codec_->GetTotalPartCount());

        for (int index = 0; index < Codec_->GetTotalPartCount(); ++index) {
            PlacementExt_.add_part_infos();
        }

        ReadyEvent_.Set();
    }

    TFuture<void> Open() override
    {
        YT_VERIFY(!Opened_);

        std::vector<TFuture<void>> futures;
        futures.reserve(Writers_.size());
        for (const auto& writer : Writers_) {
            futures.push_back(writer->Open());
        }

        return AllSucceeded(std::move(futures))
            .Apply(BIND(&TStripedErasureWriter::OnWriterOpened, MakeStrong(this)));
    }

    bool WriteBlock(const TWorkloadDescriptor& /*workloadDescriptor*/, const TBlock& block) override
    {
        YT_VERIFY(Opened_ && !Closed_);

        auto canHandleMore = UpdateWindowSize(GetWriterMemoryUsage(block.Size()));
        WriterInvoker_->Invoke(BIND(&TStripedErasureWriter::DoWriteBlock, MakeStrong(this), block));

        return canHandleMore;
    }

    bool WriteBlocks(
        const TWorkloadDescriptor& workloadDescriptor,
        const std::vector<TBlock>& blocks) override
    {
        YT_VERIFY(Opened_ && !Closed_);

        bool canHandleMore = true;
        for (const auto& block : blocks) {
            canHandleMore = WriteBlock(workloadDescriptor, block);
        }

        return canHandleMore;
    }

    TFuture<void> GetReadyEvent() override
    {
        YT_VERIFY(Opened_ && !Closed_);

        auto guard = Guard(ReadyEventLock_);

        return ReadyEvent_.ToFuture();
    }

    TFuture<void> Close(const TWorkloadDescriptor& /*workloadDescriptor*/, const TDeferredChunkMetaPtr& chunkMeta) override
    {
        YT_VERIFY(Opened_ && !Closed_);

        return BIND(&TStripedErasureWriter::DoClose, MakeStrong(this), chunkMeta)
            .AsyncVia(WriterInvoker_)
            .Run();
    }

    const NChunkClient::NProto::TChunkInfo& GetChunkInfo() const override
    {
        YT_VERIFY(Closed_);

        return ChunkInfo_;
    }

    const NChunkClient::NProto::TDataStatistics& GetDataStatistics() const override
    {
        YT_UNIMPLEMENTED();
    }

    TChunkReplicaWithLocationList GetWrittenChunkReplicas() const override
    {
        TChunkReplicaWithLocationList replicas;
        replicas.reserve(Writers_.size());
        for (int index = 0; index < std::ssize(Writers_); ++index) {
            auto partReplicas = Writers_[index]->GetWrittenChunkReplicas();
            YT_VERIFY(std::size(partReplicas) == 1);
            auto replica = partReplicas.front();
            replica = TChunkReplicaWithLocation(
                replica.GetNodeId(),
                /*replicaIndex*/ index,
                replica.GetMediumIndex(),
                replica.GetChunkLocationUuid());
            replicas.push_back(replica);
        }

        return replicas;
    }

    TChunkId GetChunkId() const override
    {
        return SessionId_.ChunkId;
    }

    NErasure::ECodec GetErasureCodecId() const override
    {
        return Codec_->GetId();
    }

    bool IsCloseDemanded() const override
    {
        for (const auto& writer : Writers_) {
            if (writer->IsCloseDemanded()) {
                return true;
            }
        }

        return false;
    }

    TFuture<void> Cancel() override
    {
        std::vector<TFuture<void>> futures;
        futures.reserve(Writers_.size());
        for (const auto& writer : Writers_) {
            futures.push_back(writer->Cancel());
        }

        return AllSucceeded(std::move(futures));
    }

private:
    const TErasureWriterConfigPtr Config_;
    const NErasure::ICodec* const Codec_;
    const TSessionId SessionId_;
    const TWorkloadDescriptor WorkloadDescriptor_;

    const std::vector<IChunkWriterPtr> Writers_;
    std::vector<TFuture<void>> WriterReadyEvents_;

    // NB: Invoker is serialized.
    const IInvokerPtr WriterInvoker_;
    const IInvokerPtr HeavyInvoker_;

    std::atomic<bool> Opened_ = false;
    std::atomic<bool> Closed_ = false;

    // Amount of memory consumed by blocks that are added to writer but not flushed to
    // underlying writers yet.
    i64 WindowSize_ = 0;

    // Becomes set when window size goes below the limit.
    TPromise<void> ReadyEvent_ = NewPromise<void>();
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, ReadyEventLock_);

    // Blocks in a current group. Group is a set of contiguous blocks that are reordered
    // using block reorderer and then flushed.
    std::vector<TBlock> CurrentGroup_;
    i64 CurrentGroupSize_ = 0;

    // Queue of blocks waiting for flush.
    std::queue<TBlock> FlushQueue_;
    i64 FlushQueueSize_ = 0;
    bool Closing_ = false;
    bool Flushing_ = false;
    // Becomes set when last block is flushed and writers can be closed.
    const TPromise<void> FlushPromise_ = NewPromise<void>();

    TBlockReorderer BlockReorderer_;

    NProto::TStripedErasurePlacementExt PlacementExt_;
    NProto::TChunkInfo ChunkInfo_;

    void OnWriterOpened()
    {
        VERIFY_THREAD_AFFINITY_ANY();

        Opened_ = true;
    }

    i64 GetWriterMemoryUsage(i64 blockSize) const
    {
        VERIFY_THREAD_AFFINITY_ANY();

        return blockSize * Codec_->GetTotalPartCount() / Codec_->GetDataPartCount();
    }

    bool UpdateWindowSize(i64 delta)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        auto guard = Guard(ReadyEventLock_);

        bool wasFull = WindowSize_ >= Config_->WriterWindowSize;
        WindowSize_ += delta;
        bool isFull = WindowSize_ >= Config_->WriterWindowSize;

        if (wasFull && !isFull) {
            auto readyEvent = ReadyEvent_;

            guard.Release();

            readyEvent.TrySet();
        } else if (!wasFull && isFull) {
            // ReadyEvent_ can be still not set because it is set outside of
            // critical section. Technically, we can just ignore it, but let's
            // set it here to avoid writer freeze in case of some bugs.
            ReadyEvent_.TrySet();

            if (ReadyEvent_.Get().IsOK()) {
                ReadyEvent_ = NewPromise<void>();
            }
        }

        return !isFull;
    }

    void DoWriteBlock(TBlock block)
    {
        VERIFY_INVOKER_AFFINITY(WriterInvoker_);

        try {
            CurrentGroupSize_ += block.Size();
            CurrentGroup_.push_back(std::move(block));

            MaybeFlushCurrentGroup();
        } catch (const std::exception& ex) {
            OnFailed(ex);
        }
    }

    void MaybeFlushCurrentGroup()
    {
        VERIFY_INVOKER_AFFINITY(WriterInvoker_);

        if (CurrentGroupSize_ >= Config_->WriterGroupSize) {
            FlushCurrentGroup();
        }
    }

    void FlushCurrentGroup()
    {
        VERIFY_INVOKER_AFFINITY(WriterInvoker_);

        BlockReorderer_.ReorderBlocks(CurrentGroup_);

        for (const auto& block : CurrentGroup_) {
            FlushQueue_.push(block);
            FlushQueueSize_ += block.Size();
        }
        CurrentGroup_.clear();
        CurrentGroupSize_ = 0;

        FlushBlocks();
    }

    void FlushBlocks()
    {
        VERIFY_INVOKER_AFFINITY(WriterInvoker_);

        // Prevent concurrent flushes.
        if (std::exchange(Flushing_, true)) {
            return;
        }
        auto guard = Finally([&] { Flushing_ = false; });

        std::vector<TBlock> segment;
        i64 segmentSize = 0;

        auto getBlock = [&] {
            auto block = std::move(FlushQueue_.front());
            FlushQueue_.pop();
            FlushQueueSize_ -= block.Size();
            return block;
        };

        while (!FlushQueue_.empty()) {
            auto block = getBlock();

            segment.push_back(block);
            segmentSize += block.Size();

            // segmentSize / dataPartCount >= desiredSegmentPartSize
            auto desiredSegmentSize = Config_->DesiredSegmentPartSize * Codec_->GetDataPartCount();
            if (segmentSize >= desiredSegmentSize || FlushQueue_.empty()) {
                // Small segments are bad, so if the rest of the group is small enough,
                // we add it to last segment.
                if (FlushQueueSize_ < desiredSegmentSize / 2) {
                    while (!FlushQueue_.empty()) {
                        segment.push_back(getBlock());
                    }
                }
                FlushSegment(std::move(segment));
                segmentSize = 0;
            }
        }

        if (Closing_) {
            FlushPromise_.TrySet();
        }
    }

    void FlushSegment(std::vector<TBlock> segment)
    {
        VERIFY_INVOKER_AFFINITY(WriterInvoker_);

        PlacementExt_.add_segment_block_counts(std::ssize(segment));
        for (const auto& block : segment) {
            PlacementExt_.add_block_sizes(block.Size());
            PlacementExt_.add_block_checksums(block.GetOrComputeChecksum());
        }

        auto parts = WaitFor(BIND(&TStripedErasureWriter::EncodeSegment, MakeStrong(this), Passed(std::move(segment)))
            .AsyncVia(HeavyInvoker_)
            .Run())
            .ValueOrThrow();
        YT_VERIFY(parts.size() == Writers_.size());

        for (int index = 0; index < std::ssize(Writers_); ++index) {
            auto readyEvent = WriterReadyEvents_[index];
            if (!readyEvent.IsSet() || !readyEvent.Get().IsOK()) {
                WaitFor(readyEvent)
                    .ThrowOnError();
            }

            const auto& writer = Writers_[index];
            const auto& block = parts[index];
            if (!writer->WriteBlock(WorkloadDescriptor_, block)) {
                WriterReadyEvents_[index] = writer->GetReadyEvent();
            }

            UpdateWindowSize(-GetWriterMemoryUsage(block.Size()));

            auto* partInfo = PlacementExt_.mutable_part_infos(index);
            partInfo->add_segment_sizes(block.Size());
            partInfo->add_segment_checksums(block.GetOrComputeChecksum());
        }
    }

    std::vector<TBlock> EncodeSegment(std::vector<TBlock> segment)
    {
        VERIFY_INVOKER_AFFINITY(HeavyInvoker_);

        i64 segmentSize = 0;
        for (const auto& block : segment) {
            segmentSize += block.Size();
        }

        i64 dataSize;

        auto dataPartCount = Codec_->GetDataPartCount();
        if (Codec_->IsBytewise()) {
            dataSize = RoundUp<i64>(segmentSize, dataPartCount);
        } else {
            dataSize = RoundUp<i64>(segmentSize, dataPartCount * Codec_->GetWordSize());
        }

        auto partSize = dataSize / dataPartCount;

        struct TErasureWriterTag { };
        auto data = TSharedMutableRef::Allocate<TErasureWriterTag>(dataSize);

        i64 dataOffset = 0;
        for (const auto& block : segment) {
            std::copy(block.Data.begin(), block.Data.end(), data.begin() + dataOffset);
            dataOffset += block.Size();
        }
        std::fill(data.begin() + dataOffset, data.end(), 0);

        std::vector<TSharedRef> dataParts;
        dataParts.reserve(dataPartCount);
        for (int index = 0; index < dataPartCount; ++index) {
            dataParts.push_back(data.Slice(partSize * index, partSize * (index + 1)));
        }
        auto parityParts = Codec_->Encode(dataParts);

        std::vector<TBlock> parts;
        parts.reserve(Codec_->GetTotalPartCount());
        for (const auto& part : dataParts) {
            parts.push_back(TBlock(part));
        }
        for (const auto& part : parityParts) {
            parts.push_back(TBlock(part));
        }
        for (const auto& part : parts) {
            part.GetOrComputeChecksum();
        }

        return parts;
    }

    void DoClose(TDeferredChunkMetaPtr chunkMeta)
    {
        VERIFY_INVOKER_AFFINITY(WriterInvoker_);

        Closing_ = true;
        FlushCurrentGroup();

        WaitFor(FlushPromise_.ToFuture())
            .ThrowOnError();

        FillChunkMeta(chunkMeta);

        YT_VERIFY(ReadyEvent_.IsSet());
        ReadyEvent_.Get().ThrowOnError();

        std::vector<TFuture<void>> futures;
        futures.reserve(Writers_.size());
        for (const auto& writer : Writers_) {
            futures.push_back(writer->Close(WorkloadDescriptor_, chunkMeta));
        }

        WaitFor(AllSucceeded(std::move(futures)))
            .ThrowOnError();

        i64 diskSpace = 0;
        for (const auto& writer : Writers_) {
            diskSpace += writer->GetChunkInfo().disk_space();
        }
        ChunkInfo_.set_disk_space(diskSpace);

        Closed_ = true;
    }

    void FillChunkMeta(const TDeferredChunkMetaPtr& chunkMeta)
    {
        VERIFY_INVOKER_AFFINITY(WriterInvoker_);

        {
            auto chunkFeatures = FromProto<EChunkFeatures>(chunkMeta->features());
            chunkFeatures |= EChunkFeatures::StripedErasure;
            chunkMeta->set_features(ToProto<ui64>(chunkFeatures));
        }

        auto miscExt = GetProtoExtension<NProto::TMiscExt>(chunkMeta->extensions());
        miscExt.set_striped_erasure(true);
        SetProtoExtension(chunkMeta->mutable_extensions(), miscExt);

        SetProtoExtension(chunkMeta->mutable_extensions(), PlacementExt_);

        chunkMeta->BlockIndexMapping() = BlockReorderer_.BlockIndexMapping();

        chunkMeta->Finalize();
    }

    void OnFailed(const TError& error)
    {
        VERIFY_THREAD_AFFINITY_ANY();

        FlushPromise_.TrySet(error);

        auto guard = Guard(ReadyEventLock_);

        if (ReadyEvent_.IsSet()) {
            ReadyEvent_ = NewPromise<void>();
        }

        ReadyEvent_.Set(error);
    }
};

////////////////////////////////////////////////////////////////////////////////

IChunkWriterPtr CreateStripedErasureWriter(
    TErasureWriterConfigPtr config,
    NErasure::ECodec codecId,
    TSessionId sessionId,
    const TWorkloadDescriptor& workloadDescriptor,
    std::vector<IChunkWriterPtr> writers)
{
    return New<TStripedErasureWriter>(
        std::move(config),
        codecId,
        sessionId,
        workloadDescriptor,
        std::move(writers));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
