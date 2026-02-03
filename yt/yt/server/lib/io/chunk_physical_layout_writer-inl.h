#ifndef CONVERT_INL_H_
#error "Direct inclusion of this file is not allowed, include chunk_physical_layout_writer.h"
// For the sake of sane code completion.
#include "chunk_physical_layout_writer.h"
#endif

// #include "chunk_file_writer.h"

#include <yt/yt/ytlib/chunk_client/chunk_meta_extensions.h>
#include <yt/yt/ytlib/chunk_client/deferred_chunk_meta.h>
#include <yt/yt/ytlib/chunk_client/format.h>
#include <yt/yt/ytlib/chunk_client/block.h>

#include <yt/yt/client/chunk_client/chunk_replica.h>

#include <yt/yt/core/misc/fs.h>
#include <yt/yt/core/misc/checksum.h>

namespace NYT::NIO {

// using namespace NConcurrency;
// using namespace NChunkClient;
// using namespace NChunkClient::NProto;

////////////////////////////////////////////////////////////////////////////////

// DEFINE_ENUM(EPhysicalLayerChunkWriterAdapterState,
//     (Created)
//     (Opening)
//     (Ready)
//     (WritingBlocks)
//     (Closing)
//     (Closed)
//     (Aborting)
//     (Aborted)
//     (Failed)
// );

////////////////////////////////////////////////////////////////////////////////

// TODO(cherepashka): rename + move to different file
// this adapter is the level where write requests (in shared ref) are formed and passed to underlying writer.

template<class TBaseChunkWriter>
    TChunkLayoutWriterAdapter<TBaseChunkWriter>::TChunkLayoutWriterAdapter(
        typename TBaseChunkWriter::TOptions options)
        : TBaseChunkWriter(options)
        , Invoker_(std::move(options.Invoker))
    {
        BlocksExt_.set_sync_on_close(options.SyncOnClose);
    }

template<class TBaseChunkWriter>
    TFuture<void> TChunkLayoutWriterAdapter<TBaseChunkWriter>::Open()
    {
        if (auto error = TryChangeState(EState::Created, EState::Opening); !error.IsOK()) {
            return MakeFuture<void>(std::move(error));
        }

        return TBaseChunkWriter::Open()
            .Apply(BIND([
                this,
                this_ = MakeStrong(this)
            ] (const TError& error) {
                YT_VERIFY(PState_.load() == EState::Opening);

                if (!error.IsOK()) {
                    PState_.store(EState::Failed);
                    THROW_ERROR_EXCEPTION("Failed to open chunk data writer")
                        << error;
                }

                PState_.store(EState::Ready);
            }).AsyncVia(Invoker_));
    }

template<class TBaseChunkWriter>
    bool TChunkLayoutWriterAdapter<TBaseChunkWriter>::WriteBlock(
        const NChunkClient::IChunkWriter::TWriteBlocksOptions& options,
        const TWorkloadDescriptor& workloadDescriptor,
        const NChunkClient::TBlock& block)
    {
        return WriteBlocks(options, workloadDescriptor, {block});
    }

    // bool WriteBlocks(
    //     const IChunkWriter::TWriteBlocksOptions& options,
    //     const TWorkloadDescriptor& workloadDescriptor,
    //     const std::vector<TBlock>& blocks) override
    // {
    //     return WriteBlocks(options, workloadDescriptor, blocks, {});
    // }

template<class TBaseChunkWriter>
    bool TChunkLayoutWriterAdapter<TBaseChunkWriter>::WriteBlocks(
        const NChunkClient::IChunkWriter::TWriteBlocksOptions& options,
        const TWorkloadDescriptor& workloadDescriptor,
        const std::vector<NChunkClient::TBlock>& blocks,
        TFairShareSlotId fairShareSlotId)
    {
        TPhysicalWriteRequest request;

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
        return TBaseChunkWriter::WriteBlocks(options, workloadDescriptor, request, fairShareSlotId);
    }

    // TFuture<void> GetReadyEvent() override
    // {
    //     return UnderlyingWriter_->GetReadyEvent();
    // }

    // TFuture<void> Close(
    //     const IChunkWriter::TWriteBlocksOptions& options,
    //     const TWorkloadDescriptor& workloadDescriptor,
    //     const TDeferredChunkMetaPtr& chunkMeta,
    //     std::optional<int> truncateBlockCount) override
    // {
    //     return Close(options, workloadDescriptor, chunkMeta, {}, truncateBlockCount);
    // }

template<class TBaseChunkWriter>
    TFuture<void> TChunkLayoutWriterAdapter<TBaseChunkWriter>::Close(
        const NChunkClient::IChunkWriter::TWriteBlocksOptions& options,
        const TWorkloadDescriptor& workloadDescriptor,
        const NChunkClient::TDeferredChunkMetaPtr& chunkMeta,
        std::optional<int> truncateBlockCount,
        TFairShareSlotId fairShareSlotId)
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
            YT_VERIFY(truncateDataSize <= DataSize_);
            DataSize_ -= truncateDataSize;
        }

        // Some uploads may still be running, but no more blocks can be added, so we can safely
        // finalize the meta in parallel with the completion of the chunk upload itself.
        FinalizeChunkMeta(std::move(chunkMeta));

        auto chunkMetaBlob = PrepareChunkMetaBlob();
        UpdateChunkInfoDiskSpace();   

        return TBaseChunkWriter::Close(options, workloadDescriptor, chunkMetaBlob, fairShareSlotId, DataSize_, MetaDataSize_)
            .Apply(BIND([
                this,
                this_ = MakeStrong(this)
            ] (const TError& error) {
                YT_VERIFY(PState_.load() == EState::Closing);

                if (!error.IsOK()) {
                    // SetFailed(error);
                    THROW_ERROR_EXCEPTION("Failed to close chunk writer")
                        << error;
                }

                UpdateChunkInfoDiskSpace();
                PState_.store(EState::Closed);
            }).AsyncVia(Invoker_));
    }
template<class TBaseChunkWriter>
const NChunkClient::NProto::TChunkInfo& TChunkLayoutWriterAdapter<TBaseChunkWriter>::GetChunkInfo() const
{
    YT_ASSERT_THREAD_AFFINITY_ANY();
    // YT_VERIFY(PState_.load() == EState::Closed);

    return ChunkInfo_;
}

    // const NChunkClient::NProto::TDataStatistics& GetDataStatistics() const override
    // {
    //     // YT_VERIFY(PState_.load() == EState::Closed);

    //     return UnderlyingWriter_->GetDataStatistics();
    // }

    // TWrittenChunkReplicasInfo GetWrittenChunkReplicasInfo() const override
    // {
    //     return UnderlyingWriter_->GetWrittenChunkReplicasInfo();
    // }
template<class TBaseChunkWriter>
const NChunkClient::TRefCountedChunkMetaPtr& TChunkLayoutWriterAdapter<TBaseChunkWriter>::GetChunkMeta() const
{
    // TODO: check the state
    return ChunkMeta_;
}

    // NErasure::ECodec GetErasureCodecId() const override
    // {
    //     return UnderlyingWriter_->GetErasureCodecId();
    // }

template<class TBaseChunkWriter>
    i64 TChunkLayoutWriterAdapter<TBaseChunkWriter>::GetDataSize() const
    {
        return DataSize_;
    }


    // TFuture<void> Cancel() override
    // {
    //     auto state = PState_.exchange(EState::Aborting);
    //     YT_VERIFY(
    //         state != EState::Opening &&
    //         state != EState::WritingBlocks &&
    //         state != EState::Closing);

    //     return UnderlyingWriter_->Cancel();
    // }


template<class TBaseChunkWriter>
TError TChunkLayoutWriterAdapter<TBaseChunkWriter>::TryChangeState(EState oldState, EState newState)
{
    if (PState_.compare_exchange_strong(oldState, newState)) {
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

template<class TBaseChunkWriter>
void TChunkLayoutWriterAdapter<TBaseChunkWriter>::UpdateChunkInfoDiskSpace()
{
    ChunkInfo_.set_disk_space(DataSize_ + MetaDataSize_);
}

template<class TBaseChunkWriter>
void TChunkLayoutWriterAdapter<TBaseChunkWriter>::FinalizeChunkMeta(NChunkClient::TDeferredChunkMetaPtr chunkMeta)
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

template<class TBaseChunkWriter>
TSharedMutableRef TChunkLayoutWriterAdapter<TBaseChunkWriter>::PrepareChunkMetaBlob()
{
    auto metaData = SerializeProtoToRefWithEnvelope(*ChunkMeta_);

    NChunkClient::TChunkMetaHeader_2 header;
    header.Signature = header.ExpectedSignature;
    header.Checksum = GetChecksum(metaData);
    header.ChunkId = TBaseChunkWriter::GetChunkId();

    MetaDataSize_ = metaData.Size() + sizeof(header);

    struct TMetaBufferTag
    { };

    auto buffer = TSharedMutableRef::Allocate<TMetaBufferTag>(MetaDataSize_, {.InitializeStorage = false});
    ::memcpy(buffer.Begin(), &header, sizeof(header));
    ::memcpy(buffer.Begin() + sizeof(header), metaData.Begin(), metaData.Size());

    return buffer;
}

////////////////////////////////////////////////////////////////////////////////


template<class TBaseChunkWriter>
TChunkLayoutWriterAdapterPtr<TBaseChunkWriter> CreateChunkLayoutWriterAdapter(typename TBaseChunkWriter::TOptions options)
{
    return New<TChunkLayoutWriterAdapter<TBaseChunkWriter>>(options);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIO

