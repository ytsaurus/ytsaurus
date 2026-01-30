#include "chunk_physical_layout_writer.h"
#include "chunk_file_writer.h"

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/deferred_chunk_meta.h>
#include <yt/yt/ytlib/chunk_client/format.h>
#include <yt/yt/ytlib/chunk_client/block.h>

#include <yt/yt/client/chunk_client/chunk_replica.h>

#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/checksum.h>

namespace NYT::NIO {

using namespace NConcurrency;
using namespace NChunkClient;
using namespace NChunkClient::NProto;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EPhysicalLayerChunkWriterAdapterState,
    (Created)
    (Opening)
    (Ready)
    (WritingBlocks)
    (Closing)
    (Closed)
    (Aborting)
    (Aborted)
    (Failed)
);

////////////////////////////////////////////////////////////////////////////////

// TODO(cherepashka): rename + move to different file
// this adapter is the level where write requests (in shared ref) are formed and passed to underlying writer.
class TWrapperChunkWriter
    : public IWrapperFairShareChunkWriter
{
public:
    TWrapperChunkWriter(
        IPhysicalLayerWriterPtr underlyingWriter, 
        IInvokerPtr invoker,
        bool syncOnClose)
        : UnderlyingWriter_(std::move(underlyingWriter))
        , Invoker_(std::move(invoker))
    {
        BlocksExt_.set_sync_on_close(syncOnClose);
    }

    TFuture<void> Open() override
    {
        if (auto error = TryChangeState(EState::Created, EState::Opening); !error.IsOK()) {
            return MakeFuture<void>(std::move(error));
        }

        return UnderlyingWriter_->Open()
            .Apply(BIND([
                this,
                this_ = MakeStrong(this)
            ] (const TError& error) {
                YT_VERIFY(State_.load() == EState::Opening);

                if (!error.IsOK()) {
                    State_.store(EState::Failed);
                    THROW_ERROR_EXCEPTION("Failed to open chunk data writer")
                        << error;
                }

                State_.store(EState::Ready);
            }).AsyncVia(Invoker_));
    }

    bool WriteBlock(
        const IChunkWriter::TWriteBlocksOptions& options,
        const TWorkloadDescriptor& workloadDescriptor,
        const TBlock& block) override
    {
        return WriteBlocks(options, workloadDescriptor, {block});
    }

    bool WriteBlocks(
        const IChunkWriter::TWriteBlocksOptions& options,
        const TWorkloadDescriptor& workloadDescriptor,
        const std::vector<TBlock>& blocks) override
    {
        return WriteBlocks(options, workloadDescriptor, blocks, {});
    }

    bool WriteBlocks(
        const IChunkWriter::TWriteBlocksOptions& options,
        const TWorkloadDescriptor& workloadDescriptor,
        const std::vector<TBlock>& blocks,
        TFairShareSlotId fairShareSlotId) override
    {
        IPhysicalLayerWriter::TWriteRequest request;

        request.StartOffset = DataSize_;
        request.EndOffset = request.StartOffset;
        request.BlockCount = blocks.size();

        request.Buffers.reserve(blocks.size());

        for (const auto& block : blocks) {
            auto error = block.CheckChecksum();
            YT_LOG_FATAL_UNLESS(
                error.IsOK(),
                error,
                "Block checksum mismatch during file writing");

            auto* blockInfo = BlocksExt_.add_blocks();
            blockInfo->set_offset(request.EndOffset);
            blockInfo->set_size(ToProto<i64>(block.Size()));
            blockInfo->set_checksum(block.GetOrComputeChecksum());

            request.EndOffset += block.Size();
            request.Buffers.push_back(block.Data);
        }

        DataSize_ = request.EndOffset;
        return UnderlyingWriter_->WriteBlocks(options, workloadDescriptor, request, fairShareSlotId);
    }

    TFuture<void> GetReadyEvent() override
    {
        return UnderlyingWriter_->GetReadyEvent();
    }

    TFuture<void> Close(
        const IChunkWriter::TWriteBlocksOptions& options,
        const TWorkloadDescriptor& workloadDescriptor,
        const TDeferredChunkMetaPtr& chunkMeta,
        std::optional<int> truncateBlockCount) override
    {
        return Close(options, workloadDescriptor, chunkMeta, {}, truncateBlockCount);
    }

    TFuture<void> Close(
        const IChunkWriter::TWriteBlocksOptions& options,
        const TWorkloadDescriptor& workloadDescriptor,
        const TDeferredChunkMetaPtr& chunkMeta,
        TFairShareSlotId fairShareSlotId,
        std::optional<int> truncateBlockCount) override
    {
        // Journal chunks are not supported (for s3).
        // YT_VERIFY(chunkMeta);

        if (auto error = TryChangeState(EState::Ready, EState::Closing); !error.IsOK()) {
            return MakeFuture<void>(std::move(error));
        }

        if (truncateBlockCount.has_value()) {
            auto& blocksExt = BlocksExt_;
            YT_LOG_FATAL_IF(
                *truncateBlockCount > blocksExt.blocks_size() || *truncateBlockCount < 0,
                "Invalid truncate block count (TruncateBlockCount: %v, BlockCount: %v)",
                *truncateBlockCount,
                blocksExt.blocks_size());

            i64 truncateDataSize = 0;
            for (int index = *truncateBlockCount; index < blocksExt.blocks_size(); ++index) {
                truncateDataSize += blocksExt.blocks(index).size();
            }
            blocksExt.mutable_blocks()->Truncate(*truncateBlockCount);
            YT_VERIFY(truncateDataSize <= GetDataSize());
            DataSize_ -= truncateDataSize;
        }

        // Some uploads may still be running, but no more blocks can be added, so we can safely
        // finalize the meta in parallel with the completion of the chunk upload itself.
        FinalizeChunkMeta(std::move(chunkMeta));

        auto chunkMetaBlob = PrepareChunkMetaBlob();
        UpdateChunkInfoDiskSpace();

        return UnderlyingWriter_->Close(options, workloadDescriptor, chunkMetaBlob, fairShareSlotId, DataSize_, MetaDataSize_)
            .Apply(BIND([
                this,
                this_ = MakeStrong(this)
            ] (const TError& error) {
                YT_VERIFY(State_.load() == EState::Closing);

                if (!error.IsOK()) {
                    // SetFailed(error);
                    THROW_ERROR_EXCEPTION("Failed to close chunk writer")
                        << error;
                }

                UpdateChunkInfoDiskSpace();
                State_.store(EState::Closed);
            }).AsyncVia(Invoker_));
    }

    const NChunkClient::NProto::TChunkInfo& GetChunkInfo() const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();
        // YT_VERIFY(State_.load() == EState::Closed);

        return ChunkInfo_;
    }

    const NChunkClient::NProto::TDataStatistics& GetDataStatistics() const override
    {
        // YT_VERIFY(State_.load() == EState::Closed);

        return UnderlyingWriter_->GetDataStatistics();
    }

    TWrittenChunkReplicasInfo GetWrittenChunkReplicasInfo() const override
    {
        return UnderlyingWriter_->GetWrittenChunkReplicasInfo();
    }

    const TRefCountedChunkMetaPtr& GetChunkMeta() const override
    {
        // TODO: check the state
        return ChunkMeta_;
    }

    TChunkId GetChunkId() const override
    {
        return UnderlyingWriter_->GetChunkId();
    }

    NErasure::ECodec GetErasureCodecId() const override
    {
        return UnderlyingWriter_->GetErasureCodecId();
    }

    const TString& GetFileName() const override
    {
        return UnderlyingWriter_->GetFileName();
    }

    i64 GetDataSize() const override
    {
        return DataSize_;
    }

    bool IsCloseDemanded() const override
    {
        return UnderlyingWriter_->IsCloseDemanded();
    }

    TFuture<void> Cancel() override
    {
        auto state = State_.exchange(EState::Aborting);
        YT_VERIFY(
            state != EState::Opening &&
            state != EState::WritingBlocks &&
            state != EState::Closing);

        return UnderlyingWriter_->Cancel();
    }

    TFuture<void> PreallocateDiskSpace(
        const TWorkloadDescriptor& workloadDescriptor,
        i64 spaceSize) override
    {
        return UnderlyingWriter_->PreallocateDiskSpace(workloadDescriptor, spaceSize);
    }

private:
    const IPhysicalLayerWriterPtr UnderlyingWriter_;

    const TRefCountedChunkMetaPtr ChunkMeta_ = New<TRefCountedChunkMeta>();
    const NLogging::TLogger Logger;
    const IInvokerPtr Invoker_;

    using EState = EPhysicalLayerChunkWriterAdapterState;
    std::atomic<EState> State_ = EPhysicalLayerChunkWriterAdapterState::Created;

    NChunkClient::NProto::TChunkInfo ChunkInfo_;
    NChunkClient::NProto::TBlocksExt BlocksExt_;

    // todo: what about thread safety?
    i64 DataSize_ = 0;
    i64 MetaDataSize_ = 0;

    TError TryChangeState(EState oldState, EState newState)
    {
        if (State_.compare_exchange_strong(oldState, newState)) {
            return {};
        }

        auto error = TError(
            "Invalid chunk writer state: expected %Qlv, actual %Qlv",
            oldState,
            newState);
        if (oldState == EState::Failed) {
            // error.MutableInnerErrors()->push_back(Error_.Load());
        }
        return error;
    }

    void UpdateChunkInfoDiskSpace()
    {
        ChunkInfo_.set_disk_space(DataSize_ + MetaDataSize_);
    }

    void FinalizeChunkMeta(TDeferredChunkMetaPtr chunkMeta)
    {
        if (!chunkMeta->IsFinalized()) {
            auto& mapping = chunkMeta->BlockIndexMapping();
            mapping = std::vector<int>(BlocksExt_.blocks().size());
            std::iota(mapping->begin(), mapping->end(), 0);
            chunkMeta->Finalize();
        }

        ChunkMeta_->CopyFrom(*chunkMeta);
        SetProtoExtension(ChunkMeta_->mutable_extensions(), BlocksExt_);
    }

    TSharedMutableRef PrepareChunkMetaBlob()
    {
        auto metaData = SerializeProtoToRefWithEnvelope(*ChunkMeta_);

        TChunkMetaHeader_2 header;
        header.Signature = header.ExpectedSignature;
        header.Checksum = GetChecksum(metaData);
        header.ChunkId = GetChunkId();

        MetaDataSize_ = metaData.Size() + sizeof(header);

        struct TMetaBufferTag
        { };

        auto buffer = TSharedMutableRef::Allocate<TMetaBufferTag>(MetaDataSize_, {.InitializeStorage = false});
        ::memcpy(buffer.Begin(), &header, sizeof(header));
        ::memcpy(buffer.Begin() + sizeof(header), metaData.Begin(), metaData.Size());

        return buffer;
    }
};

DECLARE_REFCOUNTED_CLASS(TWrapperChunkWriter)
DEFINE_REFCOUNTED_TYPE(TWrapperChunkWriter)

////////////////////////////////////////////////////////////////////////////////

IWrapperFairShareChunkWriterPtr CreateChunkLayoutWriterAdapter(
    IPhysicalLayerWriterPtr underlying,
    IInvokerPtr invoker,
    bool syncOnClose)
{
    return New<TWrapperChunkWriter>(std::move(underlying), std::move(invoker), syncOnClose);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO

