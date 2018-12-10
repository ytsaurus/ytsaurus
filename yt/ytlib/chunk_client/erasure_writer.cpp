#include "erasure_writer.h"
#include "public.h"
#include "chunk_meta_extensions.h"
#include "chunk_writer.h"
#include "config.h"
#include "dispatcher.h"
#include "replication_writer.h"
#include "helpers.h"
#include "erasure_helpers.h"
#include "block.h"
#include "private.h"

#include <yt/client/api/client.h>

#include <yt/ytlib/chunk_client/chunk_info.pb.h>
#include <yt/ytlib/chunk_client/chunk_service_proxy.h>

#include <yt/client/object_client/helpers.h>

#include <yt/client/node_tracker_client/node_directory.h>

#include <yt/client/chunk_client/chunk_replica.h>

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/scheduler.h>
#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/erasure/codec.h>

#include <yt/core/misc/numeric_helpers.h>

namespace NYT {
namespace NChunkClient {

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
        while (currentSize * groupCount >= totalSize * groups.size() &&
               groups.size() < groupCount)
        {
            groups.push_back(std::vector<TBlock>());
        }
    }

    YCHECK(groups.size() == groupCount);

    return groups;
}

std::vector<i64> BlocksToSizes(const std::vector<TBlock>& blocks)
{
    std::vector<i64> sizes;
    for (const auto& block : blocks) {
        sizes.push_back(block.Size());
    }
    return sizes;
}

class TInMemoryBlocksReader
    : public IBlocksReader
{
public:
    TInMemoryBlocksReader(const std::vector<TBlock>& blocks)
        : Blocks_(blocks)
    { }

    virtual TFuture<std::vector<TBlock>> ReadBlocks(const std::vector<int>& blockIndexes) override
    {
        std::vector<TBlock> blocks;
        for (int index = 0; index < blockIndexes.size(); ++index) {
            int blockIndex = blockIndexes[index];
            YCHECK(0 <= blockIndex && blockIndex < Blocks_.size());
            blocks.push_back(Blocks_[blockIndex]);
        }
        return MakeFuture(blocks);
    }

private:
    const std::vector<TBlock>& Blocks_;
};

DEFINE_REFCOUNTED_TYPE(TInMemoryBlocksReader)

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TErasureWriter
    : public IChunkWriter
{
public:
    TErasureWriter(
        TErasureWriterConfigPtr config,
        const TSessionId& sessionId,
        ECodec codecId,
        ICodec* codec,
        const std::vector<IChunkWriterPtr>& writers,
        const TWorkloadDescriptor& workloadDescriptor)
        : Config_(config)
        , SessionId_(sessionId)
        , CodecId_(codecId)
        , Codec_(codec)
        , WorkloadDescriptor_(workloadDescriptor)
        , Writers_(writers)
    {
        YCHECK(writers.size() == codec->GetTotalPartCount());
        VERIFY_INVOKER_THREAD_AFFINITY(TDispatcher::Get()->GetWriterInvoker(), WriterThread);

        ChunkInfo_.set_disk_space(0);
    }

    virtual TFuture<void> Open() override
    {
        return BIND(&TErasureWriter::DoOpen, MakeStrong(this))
            .AsyncVia(TDispatcher::Get()->GetWriterInvoker())
            .Run();
    }

    virtual bool WriteBlock(const TBlock& block) override
    {
        Blocks_.push_back(block);
        return true;
    }

    virtual bool WriteBlocks(const std::vector<TBlock>& blocks) override
    {
        for (const auto& block : blocks) {
            WriteBlock(block);
        }
        return true;
    }

    virtual TFuture<void> GetReadyEvent() override
    {
        return MakeFuture(TError());
    }

    virtual const NProto::TChunkInfo& GetChunkInfo() const override
    {
        return ChunkInfo_;
    }

    virtual const NProto::TDataStatistics& GetDataStatistics() const override
    {
        Y_UNREACHABLE();
    }

    virtual ECodec GetErasureCodecId() const override
    {
        return CodecId_;
    }

    virtual TChunkReplicaList GetWrittenChunkReplicas() const override
    {
        TChunkReplicaList result;
        for (int i = 0; i < Writers_.size(); ++i) {
            auto replicas = Writers_[i]->GetWrittenChunkReplicas();
            YCHECK(replicas.size() == 1);
            auto replica = TChunkReplica(
                replicas.front().GetNodeId(),
                i,
                replicas.front().GetMediumIndex());

            result.push_back(replica);
        }
        return result;
    }

    virtual bool HasSickReplicas() const override
    {
        bool hasSickReplicas = false;
        for (const auto& writer : Writers_) {
            hasSickReplicas |= writer->HasSickReplicas();
        }
        return hasSickReplicas;
    }

    virtual TFuture<void> Close(const TRefCountedChunkMetaPtr& chunkMeta) override;

    virtual TChunkId GetChunkId() const override
    {
        return SessionId_.ChunkId;
    }

private:
    const TErasureWriterConfigPtr Config_;
    const TSessionId SessionId_;
    const ECodec CodecId_;
    ICodec* const Codec_;
    const TWorkloadDescriptor WorkloadDescriptor_;

    bool IsOpen_ = false;

    std::vector<IChunkWriterPtr> Writers_;
    std::vector<TBlock> Blocks_;

    // Information about blocks, necessary to write blocks
    // and encode parity parts
    std::vector<std::vector<TBlock>> Groups_;
    TParityPartSplitInfo ParityPartSplitInfo_;

    // Chunk meta with information about block placement
    const TRefCountedChunkMetaPtr ChunkMeta_ = New<TRefCountedChunkMeta>();
    NProto::TErasurePlacementExt PlacementExt_;
    NProto::TChunkInfo ChunkInfo_;

    DECLARE_THREAD_AFFINITY_SLOT(WriterThread);

    void PrepareBlocks();

    void PrepareChunkMeta(const NProto::TChunkMeta& chunkMeta);

    void DoOpen();

    TFuture<void> WriteDataBlocks();

    void WriteDataPart(int partIndex, IChunkWriterPtr writer, const std::vector<TBlock>& blocks);

    TFuture<void> EncodeAndWriteParityBlocks();

    void OnWritten();
};

////////////////////////////////////////////////////////////////////////////////

void TErasureWriter::PrepareBlocks()
{
    Groups_ = SplitBlocks(Blocks_, Codec_->GetDataPartCount());

    i64 partSize = 0;
    for (const auto& group : Groups_) {
        i64 size = 0;
        for (const auto& block : group) {
            size += block.Size();
        }
        partSize = std::max(partSize, size);
    }
    partSize = RoundUp(partSize, static_cast<i64>(Codec_->GetWordSize()));

    ParityPartSplitInfo_ = TParityPartSplitInfo::Build(Config_->ErasureWindowSize, partSize);
}

void TErasureWriter::PrepareChunkMeta(const NProto::TChunkMeta& chunkMeta)
{
    int start = 0;
    for (const auto& group : Groups_) {
        auto* info = PlacementExt_.add_part_infos();
        info->set_first_block_index(start);
        for (const auto& block : group) {
            info->add_block_sizes(block.Size());
        }
        start += group.size();
    }
    PlacementExt_.set_parity_part_count(Codec_->GetParityPartCount());
    PlacementExt_.set_parity_block_count(ParityPartSplitInfo_.BlockCount);
    PlacementExt_.set_parity_block_size(Config_->ErasureWindowSize);
    PlacementExt_.set_parity_last_block_size(ParityPartSplitInfo_.LastBlockSize);
    PlacementExt_.mutable_part_checksums()->Resize(Codec_->GetTotalPartCount(), NullChecksum);

    ChunkMeta_->CopyFrom(chunkMeta);
}

void TErasureWriter::DoOpen()
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    std::vector<TFuture<void>> asyncResults;
    for (auto writer : Writers_) {
        asyncResults.push_back(writer->Open());
    }
    WaitFor(Combine(asyncResults))
        .ThrowOnError();

    IsOpen_ = true;
}

TFuture<void> TErasureWriter::WriteDataBlocks()
{
    VERIFY_THREAD_AFFINITY(WriterThread);
    YCHECK(Groups_.size() <= Writers_.size());

    std::vector<TFuture<void>> asyncResults;
    for (int index = 0; index < Groups_.size(); ++index) {
        asyncResults.push_back(
            BIND(
                &TErasureWriter::WriteDataPart,
                MakeStrong(this),
                index,
                Writers_[index],
                Groups_[index])
            .AsyncVia(TDispatcher::Get()->GetWriterInvoker())
            .Run());
    }
    return Combine(asyncResults);
}

void TErasureWriter::WriteDataPart(int partIndex, IChunkWriterPtr writer, const std::vector<TBlock>& blocks)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    std::vector<TChecksum> blockChecksums;
    for (const auto& block : blocks) {
        TBlock blockWithChecksum{block};
        blockWithChecksum.Checksum = block.GetOrComputeChecksum();
        blockChecksums.push_back(blockWithChecksum.Checksum);

        if (!writer->WriteBlock(blockWithChecksum)) {
            WaitFor(writer->GetReadyEvent())
                .ThrowOnError();
        }
    }

    auto checksum = CombineChecksums(blockChecksums);
    YCHECK(checksum != NullChecksum || blockChecksums.empty() ||
        std::all_of(blockChecksums.begin(), blockChecksums.end(), [] (TChecksum value) {
            return value == NullChecksum;
        }));

    PlacementExt_.mutable_part_checksums()->Set(partIndex, checksum);
}

TFuture<void> TErasureWriter::EncodeAndWriteParityBlocks()
{
    VERIFY_INVOKER_AFFINITY(TDispatcher::Get()->GetCompressionPoolInvoker());

    TPartIndexList parityIndices;
    for (int index = Codec_->GetDataPartCount(); index < Codec_->GetTotalPartCount(); ++index) {
        parityIndices.push_back(index);
    }

    std::vector<IPartBlockProducerPtr> blockProducers;
    for (const auto& group : Groups_) {
        auto blocksReader = New<TInMemoryBlocksReader>(group);
        blockProducers.push_back(New<TPartReader>(blocksReader, BlocksToSizes(group)));
    }

    std::vector<TPartWriterPtr> writerConsumers;
    std::vector<IPartBlockConsumerPtr> blockConsumers;
    for (auto index : parityIndices) {
        writerConsumers.push_back(New<TPartWriter>(
            Writers_[index],
            ParityPartSplitInfo_.GetSizes(),
            /* computeChecksums */ true));
        blockConsumers.push_back(writerConsumers.back());
    }

    std::vector<TPartRange> ranges(1, TPartRange{0, ParityPartSplitInfo_.GetPartSize()});
    auto encoder = New<TPartEncoder>(
        Codec_,
        parityIndices,
        ParityPartSplitInfo_,
        ranges,
        blockProducers,
        blockConsumers);
    encoder->Run();

    std::vector<std::pair<int, TChecksum>> partChecksums;
    for (int index = 0; index < parityIndices.size(); ++index) {
        partChecksums.push_back(std::make_pair(parityIndices[index], writerConsumers[index]->GetPartChecksum()));
    }

    return BIND([=, this_ = MakeStrong(this)] () {
        // Access to PlacementExt_ must be from WriterInvoker only.
        for (const auto& pair : partChecksums) {
            PlacementExt_.mutable_part_checksums()->Set(pair.first, pair.second);
        }
    })
    .AsyncVia(TDispatcher::Get()->GetWriterInvoker())
    .Run();
}

TFuture<void> TErasureWriter::Close(const TRefCountedChunkMetaPtr& chunkMeta)
{
    YCHECK(IsOpen_);

    PrepareBlocks();
    PrepareChunkMeta(*chunkMeta);

    auto compressionInvoker = CreateFixedPriorityInvoker(
        TDispatcher::Get()->GetPrioritizedCompressionPoolInvoker(),
        WorkloadDescriptor_.GetPriority());

    std::vector<TFuture<void>> asyncResults {
        BIND(&TErasureWriter::WriteDataBlocks, MakeStrong(this))
            .AsyncVia(TDispatcher::Get()->GetWriterInvoker())
            .Run(),
        BIND(&TErasureWriter::EncodeAndWriteParityBlocks, MakeStrong(this))
            .AsyncVia(compressionInvoker)
            .Run()
    };

    return Combine(asyncResults).Apply(
        BIND(&TErasureWriter::OnWritten, MakeStrong(this))
            .AsyncVia(TDispatcher::Get()->GetWriterInvoker()));
}


void TErasureWriter::OnWritten()
{
    std::vector<TFuture<void>> asyncResults;

    SetProtoExtension(ChunkMeta_->mutable_extensions(), PlacementExt_);

    for (const auto& writer : Writers_) {
        asyncResults.push_back(writer->Close(ChunkMeta_));
    }

    WaitFor(Combine(asyncResults))
        .ThrowOnError();

    i64 diskSpace = 0;
    for (const auto& writer : Writers_) {
        diskSpace += writer->GetChunkInfo().disk_space();
    }
    ChunkInfo_.set_disk_space(diskSpace);

    Groups_.clear();
    Blocks_.clear();
}

////////////////////////////////////////////////////////////////////////////////

IChunkWriterPtr CreateErasureWriter(
    TErasureWriterConfigPtr config,
    const TSessionId& sessionId,
    ECodec codecId,
    ICodec* codec,
    const std::vector<IChunkWriterPtr>& writers,
    const TWorkloadDescriptor& workloadDescriptor)
{
    return New<TErasureWriter>(
        config,
        sessionId,
        codecId,
        codec,
        writers,
        workloadDescriptor);
}

////////////////////////////////////////////////////////////////////////////////

std::vector<IChunkWriterPtr> CreateErasurePartWriters(
    TReplicationWriterConfigPtr config,
    TRemoteWriterOptionsPtr options,
    const TSessionId& sessionId,
    ICodec* codec,
    TNodeDirectoryPtr nodeDirectory,
    NApi::NNative::IClientPtr client,
    TTrafficMeterPtr trafficMeter,
    IThroughputThrottlerPtr throttler,
    IBlockCachePtr blockCache)
{
    auto partConfig = NYTree::CloneYsonSerializable(config);
    // Ignore upload replication factor for erasure chunk parts.
    partConfig->UploadReplicationFactor = 1;
    // Don't prefer local host: if the latter is unhealthy and cannot start writing a chunk
    // we have no way of retrying the allocation.
    partConfig->PreferLocalHost = false;

    auto replicas = AllocateWriteTargets(
        client,
        sessionId,
        codec->GetTotalPartCount(),
        codec->GetTotalPartCount(),
        std::nullopt,
        partConfig->PreferLocalHost,
        std::vector<TString>(),
        nodeDirectory,
        ChunkClientLogger);

    YCHECK(replicas.size() == codec->GetTotalPartCount());

    std::vector<IChunkWriterPtr> writers;
    for (int index = 0; index < codec->GetTotalPartCount(); ++index) {
        auto partSessionId = TSessionId(
            ErasurePartIdFromChunkId(sessionId.ChunkId, index),
            sessionId.MediumIndex);
        writers.push_back(CreateReplicationWriter(
            partConfig,
            options,
            partSessionId,
            TChunkReplicaList(1, replicas[index]),
            nodeDirectory,
            client,
            blockCache,
            trafficMeter,
            throttler));
    }

    return writers;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
