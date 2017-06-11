#include "erasure_writer.h"
#include "public.h"
#include "chunk_meta_extensions.h"
#include "chunk_replica.h"
#include "chunk_writer.h"
#include "config.h"
#include "dispatcher.h"
#include "replication_writer.h"
#include "helpers.h"
#include "block.h"
#include "private.h"

#include <yt/ytlib/api/client.h>

#include <yt/ytlib/chunk_client/chunk_info.pb.h>
#include <yt/ytlib/chunk_client/chunk_service_proxy.h>

#include <yt/ytlib/object_client/helpers.h>

#include <yt/ytlib/node_tracker_client/node_directory.h>

#include <yt/core/concurrency/scheduler.h>
#include <yt/core/concurrency/thread_affinity.h>

#include <yt/core/erasure/codec.h>

namespace NYT {
namespace NChunkClient {

using namespace NConcurrency;
using namespace NNodeTrackerClient;
using namespace NObjectClient;
using namespace NChunkClient::NProto;

///////////////////////////////////////////////////////////////////////////////

namespace {

///////////////////////////////////////////////////////////////////////////////
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

i64 RoundUp(i64 num, i64 mod)
{
    if (num % mod == 0) {
        return num;
    }
    return num + mod - (num % mod);
}

class TSlicer
{
public:
    explicit TSlicer(const std::vector<TBlock>& blocks)
        : Blocks_(blocks)
    { }

    TSharedRef GetSlice(i64 start, i64 end) const
    {
        YCHECK(start >= 0);
        YCHECK(start <= end);

        TSharedMutableRef result;

        i64 pos = 0;
        i64 resultSize = end - start;

        // We use lazy initialization.
        auto initialize = [&] () {
            if (!result) {
                struct TErasureWriterSliceTag { };
                result = TSharedMutableRef::Allocate<TErasureWriterSliceTag>(resultSize);
            }
        };

        i64 currentStart = 0;

        for (const auto& block : Blocks_) {
            i64 innerStart = std::max(static_cast<i64>(0), start - currentStart);
            i64 innerEnd = std::min(static_cast<i64>(block.Size()), end - currentStart);

            if (innerStart < innerEnd) {
                if (resultSize == innerEnd - innerStart) {
                    return block.Data.Slice(innerStart, innerEnd);
                }

                initialize();
                std::copy(block.Data.Begin() + innerStart, block.Data.Begin() + innerEnd, result.Begin() + pos);

                pos += (innerEnd - innerStart);
            }
            currentStart += block.Size();

            if (pos == resultSize || currentStart >= end) {
                break;
            }
        }

        initialize();
        return result;
    }

private:
    // Mutable since we want to return subref of blocks.
    mutable std::vector<TBlock> Blocks_;
};

} // namespace

///////////////////////////////////////////////////////////////////////////////

class TErasureWriter
    : public IChunkWriter
{
public:
    TErasureWriter(
        TErasureWriterConfigPtr config,
        const TChunkId& chunkId,
        NErasure::ECodec codecId,
        NErasure::ICodec* codec,
        const std::vector<IChunkWriterPtr>& writers)
        : Config_(config)
        , ChunkId_(chunkId)
        , CodecId_(codecId)
        , Codec_(codec)
        , Writers_(writers)
    {
        YCHECK(writers.size() == codec->GetTotalPartCount());
        VERIFY_INVOKER_THREAD_AFFINITY(TDispatcher::Get()->GetWriterInvoker(), WriterThread);

        PartwiseBlockChecksums_.resize(codec->GetTotalPartCount());
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
        bool result = true;
        for (const auto& block : blocks) {
            result = WriteBlock(block);
        }
        return result;
    }

    virtual TFuture<void> GetReadyEvent() override
    {
        auto error = TPromise<void>();
        error.Set(TError());
        return error;
    }

    virtual const TChunkInfo& GetChunkInfo() const override
    {
        return ChunkInfo_;
    }

    virtual const TDataStatistics& GetDataStatistics() const override
    {
        Y_UNREACHABLE();
    }

    virtual NErasure::ECodec GetErasureCodecId() const override
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

    virtual TFuture<void> Close(const TChunkMeta& chunkMeta) override;

    virtual TChunkId GetChunkId() const override
    {
        return ChunkId_;
    }

private:
    void PrepareBlocks();

    void DoOpen();

    TFuture<void> WriteDataBlocks();

    void EncodeAndWriteParityBlocks();

    void WriteDataPart(IChunkWriterPtr writer, const std::vector<TBlock>& blocks);

    TFuture<void> WriteParityBlocks(const std::vector<TBlock>& blocks);

    TFuture<void> CloseWriters();

    void OnBlocksWritten();


    const TErasureWriterConfigPtr Config_;
    const TChunkId ChunkId_;
    const NErasure::ECodec CodecId_;
    NErasure::ICodec* const Codec_;

    bool IsOpen_ = false;

    std::vector<IChunkWriterPtr> Writers_;
    std::vector<TBlock> Blocks_;

    // Information about blocks, necessary to write blocks
    // and encode parity parts
    std::vector<std::vector<TBlock>> Groups_;
    std::vector<TSlicer> Slicers_;
    i64 ParityDataSize_;
    int WindowCount_;

    // Chunk meta with information about block placement
    TChunkMeta ChunkMeta_;
    TChunkInfo ChunkInfo_;

    std::vector<std::vector<TChecksum>> PartwiseBlockChecksums_;

    DECLARE_THREAD_AFFINITY_SLOT(WriterThread);
};

///////////////////////////////////////////////////////////////////////////////

void TErasureWriter::PrepareBlocks()
{
    Groups_ = SplitBlocks(Blocks_, Codec_->GetDataPartCount());

    YCHECK(Slicers_.empty());

    // Calculate size of parity blocks and form slicers
    ParityDataSize_ = 0;
    int partIndex = 0;
    for (const auto& group : Groups_) {
        auto& blockChecksums = PartwiseBlockChecksums_[partIndex++];

        i64 size = 0;
        i64 maxBlockSize = 0;
        for (const auto& block : group) {
            size += block.Size();
            maxBlockSize = std::max(maxBlockSize, (i64)block.Size());
            blockChecksums.push_back(block.Checksum);
        }
        ParityDataSize_ = std::max(ParityDataSize_, size);

        Slicers_.push_back(TSlicer(group));
    }

    // Calculate number of windows
    ParityDataSize_ = RoundUp(ParityDataSize_, Codec_->GetWordSize());

    WindowCount_ = ParityDataSize_ / Config_->ErasureWindowSize;
    if (ParityDataSize_ % Config_->ErasureWindowSize != 0) {
        WindowCount_ += 1;
    }
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
                Writers_[index],
                Groups_[index])
            .AsyncVia(TDispatcher::Get()->GetWriterInvoker())
            .Run());
    }
    return Combine(asyncResults);
}

void TErasureWriter::WriteDataPart(IChunkWriterPtr writer, const std::vector<TBlock>& blocks)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    for (const auto& block : blocks) {
        if (!writer->WriteBlock(block)) {
            WaitFor(writer->GetReadyEvent())
                .ThrowOnError();
        }
    }
}

void TErasureWriter::EncodeAndWriteParityBlocks()
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    for (i64 begin = 0; begin < ParityDataSize_; begin += Config_->ErasureWindowSize) {
        i64 end = std::min(begin + Config_->ErasureWindowSize, ParityDataSize_);
        auto asyncParityBlocks =
            BIND([=, this_ = MakeStrong(this)] () {
                // Generate bytes from [begin, end) for parity blocks.
                std::vector<TSharedRef> slices;
                for (const auto& slicer : Slicers_) {
                    slices.push_back(slicer.GetSlice(begin, end));
                }
                auto blocks = TBlock::Wrap(Codec_->Encode(slices));
                for (auto& block : blocks) {
                    block.Checksum = GetChecksum(block.Data);
                }
                return blocks;
            })
            .AsyncVia(TDispatcher::Get()->GetErasurePoolInvoker())
            .Run();
        auto parityBlocksOrError = WaitFor(asyncParityBlocks);
        const auto& parityBlocks = parityBlocksOrError.ValueOrThrow();
        WaitFor(WriteParityBlocks(parityBlocks))
            .ThrowOnError();
    }
}

TFuture<void> TErasureWriter::WriteParityBlocks(const std::vector<TBlock>& blocks)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    // Write blocks of current window in parallel manner.
    std::vector<TFuture<void>> asyncResults;
    for (int index = 0; index < Codec_->GetParityPartCount(); ++index) {
        auto& blockChecksums = PartwiseBlockChecksums_[Codec_->GetDataPartCount() + index];
        blockChecksums.push_back(blocks[index].Checksum);

        const auto& writer = Writers_[Codec_->GetDataPartCount() + index];
        writer->WriteBlock(blocks[index]);
        asyncResults.push_back(writer->GetReadyEvent());
    }
    return Combine(asyncResults);
}

TFuture<void> TErasureWriter::CloseWriters()
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    std::vector<TFuture<void>> asyncResults;

    // Close writer for each non-empty group.
    for (int index = 0; index < Groups_.size(); ++index) {
        auto& writer = Writers_[index];
        asyncResults.push_back(writer->Close(ChunkMeta_));
    }

    // Close writer for each parity part.
    for (int index = 0; index < Codec_->GetParityPartCount(); ++index) {
        const auto& writer = Writers_[Codec_->GetDataPartCount() + index];
        asyncResults.push_back(writer->Close(ChunkMeta_));
    }

    return Combine(asyncResults);
}

TFuture<void> TErasureWriter::Close(const TChunkMeta& chunkMeta)
{
    YCHECK(IsOpen_);

    PrepareBlocks();
    ChunkMeta_ = chunkMeta;

    auto invoker = TDispatcher::Get()->GetWriterInvoker();

    std::vector<TFuture<void>> asyncResults {
        BIND(&TErasureWriter::WriteDataBlocks, MakeStrong(this))
            .AsyncVia(invoker)
            .Run(),
        BIND(&TErasureWriter::EncodeAndWriteParityBlocks, MakeStrong(this))
            .AsyncVia(invoker)
            .Run()
    };

    return Combine(asyncResults).Apply(
        BIND(&TErasureWriter::OnBlocksWritten, MakeStrong(this)));
}

void TErasureWriter::OnBlocksWritten()
{
    int start = 0;
    TErasurePlacementExt placementExt;
    for (const auto& group : Groups_) {
        auto* info = placementExt.add_part_infos();
        info->set_first_block_index(start);
        for (const auto& block : group) {
            info->add_block_sizes(block.Size());
        }
        start += group.size();
    }
    placementExt.set_parity_part_count(Codec_->GetParityPartCount());
    placementExt.set_parity_block_count(WindowCount_);
    placementExt.set_parity_block_size(Config_->ErasureWindowSize);
    placementExt.set_parity_last_block_size(ParityDataSize_ - (Config_->ErasureWindowSize * (WindowCount_ - 1)));

    for (const auto& blockChecksums : PartwiseBlockChecksums_) {
        auto checksum = CombineChecksums(blockChecksums);
        YCHECK(checksum != NullChecksum ||
            blockChecksums.empty() ||
            std::all_of(blockChecksums.begin(), blockChecksums.end(), [] (TChecksum value) {
                return value == NullChecksum;
            }));
        placementExt.add_part_checksums(checksum);
    }

    SetProtoExtension(ChunkMeta_.mutable_extensions(), placementExt);

    WaitFor(CloseWriters())
        .ThrowOnError();

    i64 diskSpace = 0;
    for (auto writer : Writers_) {
        diskSpace += writer->GetChunkInfo().disk_space();
    }
    ChunkInfo_.set_disk_space(diskSpace);

    Slicers_.clear();
    Groups_.clear();
    Blocks_.clear();
}

///////////////////////////////////////////////////////////////////////////////

IChunkWriterPtr CreateErasureWriter(
    TErasureWriterConfigPtr config,
    const TChunkId& chunkId,
    NErasure::ECodec codecId,
    NErasure::ICodec* codec,
    const std::vector<IChunkWriterPtr>& writers)
{
    return New<TErasureWriter>(
        config,
        chunkId,
        codecId,
        codec,
        writers);
}

///////////////////////////////////////////////////////////////////////////////

std::vector<IChunkWriterPtr> CreateErasurePartWriters(
    TReplicationWriterConfigPtr config,
    TRemoteWriterOptionsPtr options,
    const TChunkId& chunkId,
    NErasure::ICodec* codec,
    TNodeDirectoryPtr nodeDirectory,
    NApi::INativeClientPtr client,
    IThroughputThrottlerPtr throttler,
    IBlockCachePtr blockCache)
{
    // Patch writer configs to ignore upload replication factor for erasure chunk parts.
    auto partConfig = NYTree::CloneYsonSerializable(config);
    partConfig->UploadReplicationFactor = 1;

    auto replicas = AllocateWriteTargets(
        client,
        chunkId,
        codec->GetTotalPartCount(),
        codec->GetTotalPartCount(),
        Null,
        options->MediumName,
        partConfig->PreferLocalHost,
        std::vector<Stroka>(),
        nodeDirectory,
        ChunkClientLogger);

    YCHECK(replicas.size() == codec->GetTotalPartCount());

    std::vector<IChunkWriterPtr> writers;
    for (int index = 0; index < codec->GetTotalPartCount(); ++index) {
        auto partId = ErasurePartIdFromChunkId(chunkId, index);
        writers.push_back(CreateReplicationWriter(
            partConfig,
            options,
            partId,
            TChunkReplicaList(1, replicas[index]),
            nodeDirectory,
            client,
            blockCache,
            throttler));
    }

    return writers;
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
