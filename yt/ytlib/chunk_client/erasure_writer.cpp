#include "public.h"
#include "config.h"
#include "dispatcher.h"
#include "writer.h"
#include "chunk_replica.h"
#include "chunk_meta_extensions.h"
#include "replication_writer.h"

#include <core/concurrency/scheduler.h>
#include <core/concurrency/parallel_awaiter.h>
#include <core/concurrency/parallel_collector.h>

#include <core/erasure/codec.h>

#include <core/misc/address.h>

#include <core/ytree/yson_serializable.h>

#include <ytlib/chunk_client/chunk_service_proxy.h>

#include <ytlib/node_tracker_client/node_directory.h>

namespace NYT {
namespace NChunkClient {

using namespace NConcurrency;
using namespace NNodeTrackerClient;

///////////////////////////////////////////////////////////////////////////////

namespace {

///////////////////////////////////////////////////////////////////////////////
// Helpers

// Split blocks into continuous groups of approximately equal sizes.
std::vector<std::vector<TSharedRef>> SplitBlocks(
    const std::vector<TSharedRef>& blocks,
    int groupCount)
{
    i64 totalSize = 0;
    for (const auto& block : blocks) {
        totalSize += block.Size();
    }

    std::vector<std::vector<TSharedRef>> groups(1);
    i64 currentSize = 0;
    for (const auto& block : blocks) {
        groups.back().push_back(block);
        currentSize += block.Size();
        // Current group is fulfilled if currentSize / currentGroupCount >= totalSize / groupCount
        while (currentSize * groupCount >= totalSize * groups.size() &&
               groups.size() < groupCount)
        {
            groups.push_back(std::vector<TSharedRef>());
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
    explicit TSlicer(const std::vector<TSharedRef>& blocks)
        : Blocks_(blocks)
    { }

    TSharedRef GetSlice(i64 start, i64 end) const
    {
        YCHECK(start >= 0);
        YCHECK(start <= end);

        TSharedRef result;

        i64 pos = 0;
        i64 resultSize = end - start;

        // We use lazy initialization.
        bool initialized = false;
        auto initialize = [&] () {
            if (!initialized) {
                struct TErasureWriterSliceTag { };
                result = TSharedRef::Allocate<TErasureWriterSliceTag>(resultSize);
                initialized = true;
            }
        };

        i64 currentStart = 0;

        for (auto block : Blocks_) {
            i64 innerStart = std::max((i64)0, start - currentStart);
            i64 innerEnd = std::min((i64)block.Size(), end - currentStart);

            if (innerStart < innerEnd) {
                auto slice = TRef(block.Begin() + innerStart, block.Begin() + innerEnd);

                if (resultSize == slice.Size()) {
                    return block.Slice(slice);
                }

                initialize();
                std::copy(slice.Begin(), slice.End(), result.Begin() + pos);

                pos += slice.Size();
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
    mutable std::vector<TSharedRef> Blocks_;
};

} // namespace

///////////////////////////////////////////////////////////////////////////////

class TErasureWriter
    : public IWriter
{
public:
    TErasureWriter(
        TErasureWriterConfigPtr config,
        NErasure::ICodec* codec,
        const std::vector<IWriterPtr>& writers)
        : Config_(config)
        , Codec_(codec)
        , IsOpen_(false)
        , Writers_(writers)
    {
        YCHECK(writers.size() == codec->GetTotalPartCount());
        VERIFY_INVOKER_AFFINITY(TDispatcher::Get()->GetWriterInvoker(), WriterThread);
        ChunkInfo_.set_disk_space(0);
    }

    virtual TAsyncError Open() override
    {
        return BIND(&TErasureWriter::DoOpen, MakeStrong(this))
            .Guarded()
            .AsyncVia(TDispatcher::Get()->GetWriterInvoker())
            .Run();
    }

    virtual bool WriteBlock(const TSharedRef& block) override
    {
        Blocks_.push_back(block);
        return true;
    }

    virtual bool WriteBlocks(const std::vector<TSharedRef>& blocks) override
    {
        bool result = true;
        for (const auto& block : blocks) {
            result = WriteBlock(block);
        }
        return result;
    }

    virtual TAsyncError GetReadyEvent() override
    {
        auto error = TAsyncErrorPromise();
        error.Set(TError());
        return error;
    }

    virtual const NProto::TChunkInfo& GetChunkInfo() const override
    {
        return ChunkInfo_;
    }

    virtual TChunkReplicaList GetWrittenChunkReplicas() const override
    {
        TChunkReplicaList result;
        for (int i = 0; i < Writers_.size(); ++i) {
            auto replicas = Writers_[i]->GetWrittenChunkReplicas();
            YCHECK(replicas.size() == 1);
            auto replica = TChunkReplica(replicas.front().GetNodeId(), i);

            result.push_back(replica);
        }
        return result;
    }

    virtual TAsyncError Close(const NProto::TChunkMeta& chunkMeta) override;

private:
    void PrepareBlocks();

    void PrepareChunkMeta(const NProto::TChunkMeta& chunkMeta);

    void DoOpen();

    TAsyncError WriteDataBlocks();

    TError EncodeAndWriteParityBlocks();

    TError WriteDataPart(IWriterPtr writer, const std::vector<TSharedRef>& blocks);

    TAsyncError WriteParityBlocks(const std::vector<TSharedRef>& blocks);

    TAsyncError CloseParityWriters();

    TAsyncError OnClosed(TError error);

    TErasureWriterConfigPtr Config_;
    NErasure::ICodec* Codec_;

    bool IsOpen_;

    std::vector<IWriterPtr> Writers_;
    std::vector<TSharedRef> Blocks_;

    // Information about blocks, necessary to write blocks
    // and encode parity parts
    std::vector<std::vector<TSharedRef>> Groups_;
    std::vector<TSlicer> Slicers_;
    i64 ParityDataSize_;
    int WindowCount_;

    // Chunk meta with information about block placement
    NProto::TChunkMeta ChunkMeta_;
    NProto::TChunkInfo ChunkInfo_;

    DECLARE_THREAD_AFFINITY_SLOT(WriterThread);

};

///////////////////////////////////////////////////////////////////////////////

void TErasureWriter::PrepareBlocks()
{
    Groups_ = SplitBlocks(Blocks_, Codec_->GetDataPartCount());

    YCHECK(Slicers_.empty());

    // Calculate size of parity blocks and form slicers
    ParityDataSize_ = 0;
    for (const auto& group : Groups_) {
        i64 size = 0;
        i64 maxBlockSize = 0;
        for (const auto& block : group) {
            size += block.Size();
            maxBlockSize = std::max(maxBlockSize, (i64)block.Size());
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

void TErasureWriter::PrepareChunkMeta(const NProto::TChunkMeta& chunkMeta)
{
    int start = 0;
    NProto::TErasurePlacementExt placementExt;
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

    ChunkMeta_ = chunkMeta;
    SetProtoExtension(ChunkMeta_.mutable_extensions(), placementExt);
}

void TErasureWriter::DoOpen()
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    auto collector = New<TParallelCollector<void>>();
    for (auto writer : Writers_) {
        collector->Collect(writer->Open());
    }

    auto error = WaitFor(collector->Complete());
    THROW_ERROR_EXCEPTION_IF_FAILED(error);
    IsOpen_ = true;
}

TAsyncError TErasureWriter::WriteDataBlocks()
{
    VERIFY_THREAD_AFFINITY(WriterThread);
    YCHECK(Groups_.size() <= Writers_.size());

    auto this_ = MakeStrong(this);
    auto parallelCollector = New<TParallelCollector<void>>();
    for (int index = 0; index < Groups_.size(); ++index) {
        parallelCollector->Collect(BIND(
                &TErasureWriter::WriteDataPart,
                this_,
                Writers_[index],
                Groups_[index])
            .AsyncVia(TDispatcher::Get()->GetWriterInvoker())
            .Run());
    }
    return parallelCollector->Complete();
}

TError TErasureWriter::WriteDataPart(IWriterPtr writer, const std::vector<TSharedRef>& blocks)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    for (const auto& block : blocks) {
        if (!writer->WriteBlock(block)) {
            auto error = WaitFor(writer->GetReadyEvent());
            if (!error.IsOK())
                return error;
        }
    }

    return WaitFor(writer->Close(ChunkMeta_));
}

TError TErasureWriter::EncodeAndWriteParityBlocks()
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    auto this_ = MakeStrong(this);
    for (i64 begin = 0; begin < ParityDataSize_; begin += Config_->ErasureWindowSize) {
        i64 end = std::min(begin + Config_->ErasureWindowSize, ParityDataSize_);

        auto parityBlocksPromise = NewPromise<std::vector<TSharedRef>>();

        TDispatcher::Get()->GetErasurePoolInvoker()->Invoke(BIND([this, this_, begin, end, &parityBlocksPromise] () {
            // Generate bytes from [begin, end) for parity blocks.
            std::vector<TSharedRef> slices;
            for (const auto& slicer : Slicers_) {
                slices.push_back(slicer.GetSlice(begin, end));
            }

            parityBlocksPromise.Set(Codec_->Encode(slices));
        }));
        
        auto error = WaitFor(WriteParityBlocks(WaitFor(parityBlocksPromise.ToFuture())));
        if (!error.IsOK()) {
            return error;
        }
    }

    return WaitFor(CloseParityWriters());
}

TAsyncError TErasureWriter::WriteParityBlocks(const std::vector<TSharedRef>& blocks)
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    // Write blocks of current window in parallel manner
    auto collector = New<TParallelCollector<void>>();
    for (int i = 0; i < Codec_->GetParityPartCount(); ++i) {
        auto& writer = Writers_[Codec_->GetDataPartCount() + i];
        writer->WriteBlock(blocks[i]);
        collector->Collect(writer->GetReadyEvent());
    }
    return collector->Complete();
}

TAsyncError TErasureWriter::CloseParityWriters()
{
    VERIFY_THREAD_AFFINITY(WriterThread);

    auto collector = New<TParallelCollector<void>>();
    for (int i = 0; i < Codec_->GetParityPartCount(); ++i) {
        auto& writer = Writers_[Codec_->GetDataPartCount() + i];
        collector->Collect(writer->Close(ChunkMeta_));
    }
    return collector->Complete();
}

TAsyncError TErasureWriter::Close(const NProto::TChunkMeta& chunkMeta)
{
    YCHECK(IsOpen_);

    PrepareBlocks();
    PrepareChunkMeta(chunkMeta);

    auto this_ = MakeStrong(this);
    auto invoker = TDispatcher::Get()->GetWriterInvoker();
    auto collector = New<TParallelCollector<void>>();
    collector->Collect(
        BIND(&TErasureWriter::WriteDataBlocks, MakeStrong(this))
        .AsyncVia(invoker)
        .Run());
    collector->Collect(
        BIND(&TErasureWriter::EncodeAndWriteParityBlocks, MakeStrong(this))
        .AsyncVia(invoker)
        .Run());
    return collector->Complete().Apply(BIND(&TErasureWriter::OnClosed, MakeStrong(this)));
}


TAsyncError TErasureWriter::OnClosed(TError error)
{
    if (!error.IsOK()) {
        return MakeFuture(error);
    }

    i64 diskSpace = 0;
    for (auto writer : Writers_) {
        diskSpace += writer->GetChunkInfo().disk_space();
    }
    ChunkInfo_.set_disk_space(diskSpace);

    Slicers_.clear();
    Groups_.clear();
    Blocks_.clear();

    return OKFuture;
}

///////////////////////////////////////////////////////////////////////////////

IWriterPtr CreateErasureWriter(
    TErasureWriterConfigPtr config,
    NErasure::ICodec* codec,
    const std::vector<IWriterPtr>& writers)
{
    return New<TErasureWriter>(config, codec, writers);
}

///////////////////////////////////////////////////////////////////////////////

std::vector<IWriterPtr> CreateErasurePartWriters(
    TReplicationWriterConfigPtr config,
    const TChunkId& chunkId,
    NErasure::ICodec* codec,
    TNodeDirectoryPtr nodeDirectory,
    NRpc::IChannelPtr masterChannel,
    EWriteSessionType sessionType)
{
    // Patch writer configs to ignore upload replication factor for erasure chunk parts.
    auto config_ = NYTree::CloneYsonSerializable(config);
    config_->UploadReplicationFactor = 1;

    TChunkServiceProxy proxy(masterChannel);
    auto req = proxy.AllocateWriteTargets();

    req->set_target_count(codec->GetTotalPartCount());
    if (config_->PreferLocalHost) {
        req->set_preferred_host_name(TAddressResolver::Get()->GetLocalHostName());
    }

    ToProto(req->mutable_chunk_id(), chunkId);

    auto rsp = WaitFor(req->Invoke());
    THROW_ERROR_EXCEPTION_IF_FAILED(
        *rsp,
        EErrorCode::MasterCommunicationFailed, 
        "Failed to allocate write targets for chunk %v", 
        chunkId);

    nodeDirectory->MergeFrom(rsp->node_directory());
    auto replicas = NYT::FromProto<TChunkReplica, TChunkReplicaList>(rsp->replicas());

    YCHECK(replicas.size() == codec->GetTotalPartCount());

    std::vector<IWriterPtr> writers;
    for (int index = 0; index < codec->GetTotalPartCount(); ++index) {
        auto partId = ErasurePartIdFromChunkId(chunkId, index);
        writers.push_back(CreateReplicationWriter(
            config_,
            partId,
            TChunkReplicaList(1, replicas[index]),
            nodeDirectory,
            nullptr,
            sessionType));
    }

    return writers;
}

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
