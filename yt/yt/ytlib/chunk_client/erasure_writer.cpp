#include "erasure_writer.h"

#include "block_reorderer.h"
#include "chunk_meta_extensions.h"
#include "chunk_writer.h"
#include "config.h"
#include "deferred_chunk_meta.h"
#include "dispatcher.h"
#include "replication_writer.h"
#include "helpers.h"
#include "erasure_helpers.h"
#include "block.h"
#include "private.h"

#include <yt/yt/ytlib/chunk_client/proto/chunk_info.pb.h>
#include <yt/yt/ytlib/chunk_client/chunk_service_proxy.h>

#include <yt/yt/client/api/client.h>
#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/client/chunk_client/chunk_replica.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/scheduler.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

#include <yt/yt/library/erasure/impl/codec.h>

#include <yt/yt/core/misc/numeric_helpers.h>
#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/rpc/dispatcher.h>

namespace NYT::NChunkClient {

using namespace NErasure;
using namespace NConcurrency;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NErasureHelpers;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////
// Helpers

// Split blocks into continuous groups of approximately equal sizes.
std::vector<std::vector<TBlock>> SplitBlocks(
    const std::vector<TBlock>& blocks,
    int groupCount)
{
    i64 totalSize = 0;
    for (const auto& block : blocks) {
        totalSize += block.Size();
    }

    std::vector<std::vector<TBlock>> groups(1);
    i64 currentSize = 0;
    for (const auto& block : blocks) {
        groups.back().push_back(block);
        currentSize += block.Size();
        // Current group is fulfilled if currentSize / currentGroupCount >= totalSize / groupCount
        while (currentSize * groupCount >= totalSize * std::ssize(groups) &&
               std::ssize(groups) < groupCount)
        {
            groups.push_back(std::vector<TBlock>());
        }
    }

    YT_VERIFY(std::ssize(groups) == groupCount);

    return groups;
}

class TBlockGroupReader
    : public TNonCopyable
{
public:
    TBlockGroupReader(std::vector<std::vector<TBlock>> groups)
    {
        for (auto blocks : groups) {
            Readers_.emplace_back(blocks);
        }
    }

    std::vector<TSharedRef> Read(i64 size, i64* maxDataSize)
    {
        YT_VERIFY(maxDataSize);
        *maxDataSize = 0;

        std::vector<TSharedRef> result(Readers_.size());
        for (int index = 0; index < std::ssize(Readers_); ++index) {
            i64 dataSize;
            result[index] = Readers_[index].Read(size, &dataSize);
            *maxDataSize = Max(*maxDataSize, dataSize);
        }

        return result;
    }

    bool Empty() const
    {
        bool result = true;
        for (const auto& r : Readers_) {
            result = result && r.Empty();
        }
        return result;
    }

private:
    class TSingleGroupReader
        : public TMoveOnly
    {
    public:
        TSingleGroupReader(const std::vector<TBlock>& blocks)
            : Blocks_(TBlock::Unwrap(blocks))
        { }

        TSharedRef Read(i64 size, i64* dataSize)
        {
            YT_VERIFY(dataSize);

            if (std::ssize(Buffer_) < size) {
                struct TErasureWriterSliceTag { };
                Buffer_ = TSharedMutableRef::Allocate<TErasureWriterSliceTag>(size);
            }

            *dataSize = 0;
            while (CurrentBlock_ < std::ssize(Blocks_) && *dataSize < size) {
                const auto& block = Blocks_[CurrentBlock_];

                i64 toWrite = Min<i64>(block.Size(), size - *dataSize);
                std::copy(block.begin(), block.begin() + toWrite, Buffer_.begin() + *dataSize);

                if (toWrite < std::ssize(block)) {
                    Blocks_[CurrentBlock_] = block.Slice(toWrite, block.Size());
                } else {
                    ++CurrentBlock_;
                }

                *dataSize += toWrite;
            }

            std::fill(Buffer_.begin() + *dataSize, Buffer_.begin() + size, 0);
            return Buffer_;
        }

        bool Empty() const
        {
            return CurrentBlock_ >= std::ssize(Blocks_);
        }

    private:
        TSharedMutableRef Buffer_;
        std::vector<TSharedRef> Blocks_;
        int CurrentBlock_ = 0;
    };

    std::vector<TSingleGroupReader> Readers_;
};

class TErasurePartWriterWrapper
    : public TRefCounted
{
public:
    TErasurePartWriterWrapper(const TWorkloadDescriptor& workloadDescriptor, IChunkWriterPtr output)
        : WorkloadDescriptor_(workloadDescriptor)
        , Output_(output)
    { }

    void WriteStripe(int startBlockIndex, std::vector<TBlock> blocks)
    {
        StripeStartBlockIndices_.push_back(startBlockIndex);
        StripeBlockCounts_.push_back(blocks.size());

        for (const auto& block : blocks) {
            TBlock blockWithChecksum(block);
            blockWithChecksum.Checksum = block.GetOrComputeChecksum();
            HashCombine(Checksum_, blockWithChecksum.Checksum);

            BlockSizes_.push_back(blockWithChecksum.Size());

            if (!Output_->WriteBlock(WorkloadDescriptor_, blockWithChecksum)) {
                WaitFor(Output_->GetReadyEvent())
                    .ThrowOnError();
            }
        }
    }

    NProto::TPartInfo GetPartInfo()
    {
        NProto::TPartInfo info;
        NYT::ToProto(info.mutable_first_block_index_per_stripe(), StripeStartBlockIndices_);
        NYT::ToProto(info.mutable_block_sizes(), BlockSizes_);
        return info;
    }

    std::vector<int> GetStripeBlockCounts()
    {
        return StripeBlockCounts_;
    }

    TChecksum GetPartChecksum()
    {
        return Checksum_;
    }

private:
    const TWorkloadDescriptor WorkloadDescriptor_;
    IChunkWriterPtr Output_;

    std::vector<i64> BlockSizes_;
    std::vector<int> StripeStartBlockIndices_;
    std::vector<int> StripeBlockCounts_;
    TChecksum Checksum_ = NullChecksum;
};

DEFINE_REFCOUNTED_TYPE(TErasurePartWriterWrapper)

using TErasurePartWriterWrapperPtr = TIntrusivePtr<TErasurePartWriterWrapper>;

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TErasureWriter
    : public IChunkWriter
{
public:
    TErasureWriter(
        TErasureWriterConfigPtr config,
        TSessionId sessionId,
        ECodec codecId,
        const std::vector<IChunkWriterPtr>& writers,
        const TWorkloadDescriptor& workloadDescriptor)
        : Config_(config)
        , SessionId_(sessionId)
        , CodecId_(codecId)
        , Codec_(NErasure::GetCodec(CodecId_))
        , WorkloadDescriptor_(workloadDescriptor)
        , ErasureWindowSize_(RoundUp<i64>(config->ErasureWindowSize, Codec_->GetWordSize()))
        , ReadyEvent_(VoidFuture)
        , Writers_(writers)
        , BlockReorderer_(config)
    {
        YT_VERIFY(std::ssize(writers) == Codec_->GetTotalPartCount());
        VERIFY_INVOKER_THREAD_AFFINITY(TDispatcher::Get()->GetWriterInvoker(), WriterThread);

        ChunkInfo_.set_disk_space(0);
        for (const auto& writer : writers) {
            WriterWrappers_.push_back(New<TErasurePartWriterWrapper>(workloadDescriptor, writer));
        }
    }

    TFuture<void> Open() override
    {
        return BIND(&TErasureWriter::DoOpen, MakeStrong(this))
            .AsyncVia(TDispatcher::Get()->GetWriterInvoker())
            .Run();
    }

    bool WriteBlock(const TWorkloadDescriptor& workloadDescriptor, const TBlock& block) override;

    bool WriteBlocks(
        const TWorkloadDescriptor& workloadDescriptor,
        const std::vector<TBlock>& blocks) override
    {
        for (const auto& block : blocks) {
            WriteBlock(workloadDescriptor, block);
        }
        return ReadyEvent_.IsSet() && ReadyEvent_.Get().IsOK();
    }

    TFuture<void> GetReadyEvent() override
    {
        return ReadyEvent_;
    }

    const NProto::TChunkInfo& GetChunkInfo() const override
    {
        return ChunkInfo_;
    }

    const NProto::TDataStatistics& GetDataStatistics() const override
    {
        YT_ABORT();
    }

    ECodec GetErasureCodecId() const override
    {
        return CodecId_;
    }

    TChunkReplicaWithLocationList GetWrittenChunkReplicas() const override
    {
        TChunkReplicaWithLocationList result;
        for (int i = 0; i < std::ssize(Writers_); ++i) {
            auto replicas = Writers_[i]->GetWrittenChunkReplicas();
            YT_VERIFY(replicas.size() == 1);
            result.emplace_back(
                replicas[0].GetNodeId(),
                i,
                replicas[0].GetMediumIndex(),
                replicas[0].GetChunkLocationUuid());
        }
        return result;
    }

    bool IsCloseDemanded() const override
    {
        bool isCloseDemanded = false;
        for (const auto& writer : Writers_) {
            isCloseDemanded |= writer->IsCloseDemanded();
        }
        return isCloseDemanded;
    }

    TFuture<void> Close(
        const TWorkloadDescriptor& workloadDescriptor,
        const TDeferredChunkMetaPtr& chunkMeta) override;

    TChunkId GetChunkId() const override
    {
        return SessionId_.ChunkId;
    }

    TFuture<void> Cancel() override
    {
        std::vector<TFuture<void>> cancelFutures;
        cancelFutures.reserve(Writers_.size());
        for (const auto& writer : Writers_) {
            cancelFutures.push_back(writer->Cancel());
        }
        return AllSucceeded(std::move(cancelFutures));
    }

private:
    const TErasureWriterConfigPtr Config_;
    const TSessionId SessionId_;
    const ECodec CodecId_;
    ICodec* const Codec_;
    const TWorkloadDescriptor WorkloadDescriptor_;

    const i64 ErasureWindowSize_;

    bool IsOpen_ = false;
    TFuture<void> ReadyEvent_;

    std::vector<IChunkWriterPtr> Writers_;
    std::vector<TErasurePartWriterWrapperPtr> WriterWrappers_;

    std::vector<TBlock> Blocks_;
    i64 AccumulatedSize_ = 0;
    int LastFlushedBlockIndex_ = 0;

    std::vector<TChecksum> BlockChecksums_;

    NProto::TChunkInfo ChunkInfo_;

    TBlockReorderer BlockReorderer_;

    DECLARE_THREAD_AFFINITY_SLOT(WriterThread);

    void DoOpen();

    TFuture<void> Flush(std::vector<TBlock> blocks);

    TFuture<void> WriteDataBlocks(const std::vector<std::vector<TBlock>>& groups);
    TFuture<void> EncodeAndWriteParityBlocks(const std::vector<std::vector<TBlock>>& groups);

    void FillChunkMeta(const TDeferredChunkMetaPtr& chunkMeta);

    void DoClose(const TWorkloadDescriptor& workloadDescriptor, TDeferredChunkMetaPtr chunkMeta);
};

////////////////////////////////////////////////////////////////////////////////

void TErasureWriter::DoOpen()
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    std::vector<TFuture<void>> asyncResults;
    for (auto writer : Writers_) {
        asyncResults.push_back(writer->Open());
    }
    WaitFor(AllSucceeded(asyncResults))
        .ThrowOnError();

    IsOpen_ = true;
}

bool TErasureWriter::WriteBlock(const TWorkloadDescriptor& /*workloadDescriptor*/, const TBlock& block)
{
    Blocks_.push_back(block);
    AccumulatedSize_ += block.Size();

    if (Config_->ErasureStoreOriginalBlockChecksums) {
        Blocks_.back().Checksum = block.GetOrComputeChecksum();
        BlockChecksums_.push_back(block.Checksum);
    }

    if (Config_->ErasureStripeSize &&
        AccumulatedSize_ >= *Config_->ErasureStripeSize * Codec_->GetDataPartCount())
    {
        ReadyEvent_ = ReadyEvent_.Apply(
            BIND(&TErasureWriter::Flush, MakeStrong(this), Passed(std::move(Blocks_)))
            .AsyncVia(TDispatcher::Get()->GetWriterInvoker()));

        AccumulatedSize_ = 0;
        Blocks_.clear();
    }

    return ReadyEvent_.IsSet() && ReadyEvent_.Get().IsOK();
}

TFuture<void> TErasureWriter::WriteDataBlocks(const std::vector<std::vector<TBlock>>& groups)
{
    VERIFY_THREAD_AFFINITY(WriterThread);
    YT_VERIFY(groups.size() <= Writers_.size());

    int blockIndex = LastFlushedBlockIndex_;

    std::vector<TFuture<void>> asyncResults;
    for (int index = 0; index < std::ssize(groups); ++index) {
        asyncResults.push_back(
            BIND(
                &TErasurePartWriterWrapper::WriteStripe,
                WriterWrappers_[index],
                blockIndex,
                groups[index])
            .AsyncVia(TDispatcher::Get()->GetWriterInvoker())
            .Run());

        blockIndex += groups[index].size();
    }

    LastFlushedBlockIndex_ = blockIndex;
    return AllSucceeded(asyncResults);
}

TFuture<void> TErasureWriter::EncodeAndWriteParityBlocks(const std::vector<std::vector<TBlock>>& groups)
{
    VERIFY_INVOKER_AFFINITY(NRpc::TDispatcher::Get()->GetCompressionPoolInvoker());

    std::vector<std::vector<TBlock>> parityBlocks(Codec_->GetParityPartCount());

    TBlockGroupReader reader(groups);
    while (!reader.Empty()) {
        i64 blockSize;
        auto codecInput = reader.Read(ErasureWindowSize_, &blockSize);
        blockSize = RoundUp<i64>(blockSize, Codec_->GetWordSize());

        for (int index = 0; index < std::ssize(codecInput); ++index) {
            codecInput[index] = codecInput[index].Slice(0, blockSize);
        }

        auto codecOutput = Codec_->Encode(codecInput);
        for (int index = 0; index < std::ssize(codecOutput); ++index) {
            TBlock block(codecOutput[index]);
            block.Checksum = block.GetOrComputeChecksum();
            parityBlocks[index].push_back(block);
        }
    }

    std::vector<TFuture<void>> asyncResults;
    for (int index = 0; index < std::ssize(parityBlocks); ++index) {
        int partIndex = index + Codec_->GetDataPartCount();
        asyncResults.push_back(
            BIND(
                &TErasurePartWriterWrapper::WriteStripe,
                WriterWrappers_[partIndex],
                /*blockIndex*/ 0,
                parityBlocks[index])
            .AsyncVia(TDispatcher::Get()->GetWriterInvoker())
            .Run());
    }
    return AllSucceeded(asyncResults);
}

TFuture<void> TErasureWriter::Flush(std::vector<TBlock> blocks)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    if (blocks.empty()) {
        return VoidFuture;
    }

    BlockReorderer_.ReorderBlocks(blocks);
    auto groups = SplitBlocks(blocks, Codec_->GetDataPartCount());

    auto compressionInvoker = CreateFixedPriorityInvoker(
        NRpc::TDispatcher::Get()->GetPrioritizedCompressionPoolInvoker(),
        WorkloadDescriptor_.GetPriority());

    std::vector<TFuture<void>> asyncResults {
        WriteDataBlocks(groups),
        BIND(&TErasureWriter::EncodeAndWriteParityBlocks, MakeStrong(this), groups)
            .AsyncVia(compressionInvoker)
            .Run()
    };

    return AllSucceeded(asyncResults);
}

TFuture<void> TErasureWriter::Close(const TWorkloadDescriptor& workloadCategory, const TDeferredChunkMetaPtr& chunkMeta)
{
    YT_VERIFY(IsOpen_);

    return BIND(&TErasureWriter::DoClose, MakeStrong(this), workloadCategory, chunkMeta)
            .AsyncVia(TDispatcher::Get()->GetWriterInvoker())
            .Run();
}

void TErasureWriter::FillChunkMeta(const TDeferredChunkMetaPtr& chunkMeta)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    NProto::TErasurePlacementExt placementExt;
    for (int index = 0; index < Codec_->GetDataPartCount(); ++index) {
        auto* info = placementExt.add_part_infos();
        *info = WriterWrappers_[index]->GetPartInfo();
    }

    for (int index = 0; index < Codec_->GetTotalPartCount(); ++index) {
        placementExt.add_part_checksums(WriterWrappers_[index]->GetPartChecksum());
    }

    auto parityPartInfo = WriterWrappers_[Codec_->GetDataPartCount()]->GetPartInfo();
    auto parityPartStripeBlockCounts = WriterWrappers_[Codec_->GetDataPartCount()]->GetStripeBlockCounts();

    int stripeEndIndex = 0;
    for (int blockCount : parityPartStripeBlockCounts) {
        stripeEndIndex += blockCount;

        placementExt.add_parity_block_count_per_stripe(blockCount);
        placementExt.add_parity_last_block_size_per_stripe(parityPartInfo.block_sizes(stripeEndIndex - 1));
    }

    placementExt.set_parity_block_size(ErasureWindowSize_);
    placementExt.set_parity_part_count(Codec_->GetParityPartCount());

    if (Config_->ErasureStoreOriginalBlockChecksums) {
        NYT::ToProto(placementExt.mutable_block_checksums(), BlockChecksums_);
    }

    chunkMeta->BlockIndexMapping() = BlockReorderer_.BlockIndexMapping();
    SetProtoExtension(chunkMeta->mutable_extensions(), placementExt);
    chunkMeta->Finalize();
}

void TErasureWriter::DoClose(const TWorkloadDescriptor& workloadCategory, TDeferredChunkMetaPtr chunkMeta)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    WaitFor(
        ReadyEvent_.Apply(
            BIND(&TErasureWriter::Flush, MakeStrong(this), std::move(Blocks_))
            .AsyncVia(TDispatcher::Get()->GetWriterInvoker())))
        .ThrowOnError();

    FillChunkMeta(chunkMeta);

    std::vector<TFuture<void>> asyncResults;
    for (const auto& writer : Writers_) {
        asyncResults.push_back(writer->Close(workloadCategory, chunkMeta));
    }

    WaitFor(AllSucceeded(asyncResults))
        .ThrowOnError();

    i64 diskSpace = 0;
    for (const auto& writer : Writers_) {
        diskSpace += writer->GetChunkInfo().disk_space();
    }
    ChunkInfo_.set_disk_space(diskSpace);
}

////////////////////////////////////////////////////////////////////////////////

IChunkWriterPtr CreateErasureWriter(
    TErasureWriterConfigPtr config,
    TSessionId sessionId,
    ECodec codecId,
    const std::vector<IChunkWriterPtr>& writers,
    const TWorkloadDescriptor& workloadDescriptor)
{
    return New<TErasureWriter>(
        config,
        sessionId,
        codecId,
        writers,
        workloadDescriptor);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient
