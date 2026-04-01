#include "chunk_writer.h"

#include "public.h"
#include "config.h"
#include "upload_session.h"

#include <yt/yt/server/lib/io/chunk_file_writer.h>

#include <yt/yt/ytlib/chunk_client/block.h>
#include <yt/yt/ytlib/chunk_client/medium_descriptor.h>
#include <yt/yt/ytlib/chunk_client/dispatcher.h>
#include <yt/yt/ytlib/chunk_client/session_id.h>

#include <yt/yt/library/s3/client.h>

namespace NYT::NS3 {

using namespace NChunkClient;
using namespace NNodeTrackerClient;
using namespace NIO;

////////////////////////////////////////////////////////////////////////////

class TS3Writer
    : public IChunkWriter
{
public:
    TS3Writer(
        IClientPtr client,
        const TS3MediumDescriptorPtr& mediumDescriptor,
        TS3WriterConfigPtr config,
        TSessionId sessionId)
        : Client_(std::move(client))
        , SessionId_(sessionId)
        , Logger(ChunkClientLogger().WithTag("ChunkId: %v", SessionId_.ChunkId))
        , ChunkUploadSession_(New<TS3MultiPartUploadSession>(
            Client_,
            mediumDescriptor->GetS3ObjectPlacementForChunk(SessionId_.ChunkId),
            TS3MultiPartUploadSession::TOptions{
                .PartSize = config->UploadPartSize,
                .UploadWindowSize = config->UploadWindowSize,
            },
            TDispatcher::Get()->GetWriterInvoker(),
            Logger))
        , ChunkMetaUploadSession_(New<TS3SimpleUploadSession>(
            Client_,
            mediumDescriptor->GetS3ObjectPlacementForChunkMeta(SessionId_.ChunkId),
            TDispatcher::Get()->GetWriterInvoker(),
            Logger))
    { }

    TFuture<void> Open() override
    {
        YT_LOG_DEBUG("Offshore S3 writer opened");

        return ChunkUploadSession_->Start();
    }

    bool WriteBlock(
        const IChunkWriter::TWriteBlocksOptions& options,
        const TWorkloadDescriptor& workloadDescriptor,
        const TBlock& block) override
    {
        return WriteBlocks(options, workloadDescriptor, {block});
    }

    bool WriteBlocks(
        const IChunkWriter::TWriteBlocksOptions& /*options*/,
        const TWorkloadDescriptor& /*workloadDescriptor*/,
        const std::vector<TBlock>& blocks) override
    {

        auto writeRequest = SerializeBlocks(DataSize_, blocks, BlocksExt_);
        DataSize_ = writeRequest.EndOffset;
        return ChunkUploadSession_->Add(std::move(writeRequest.Buffers));
    }

    TFuture<void> GetReadyEvent() override
    {
        return ChunkUploadSession_->GetReadyEvent();
    }

    TFuture<void> Close(
        const IChunkWriter::TWriteBlocksOptions& /*options*/,
        const TWorkloadDescriptor& /*workloadDescriptor*/,
        const TDeferredChunkMetaPtr& chunkMeta) override
    {
        // Journal chunks are not supported.
        YT_VERIFY(chunkMeta);

        // Some uploads may still be running, but no more blocks can be added, so we can safely
        // finalize the meta in parallel with the completion of the chunk upload itself.
        ChunkMeta_->CopyFrom(*FinalizeChunkMeta(std::move(chunkMeta), BlocksExt_));
        auto chunkMetaBlob = SerializeChunkMeta(GetChunkId(), ChunkMeta_);
        auto closeFutures = std::vector{
            ChunkUploadSession_->Complete(),
            ChunkMetaUploadSession_->Upload(std::move(chunkMetaBlob)),
        };

        return AllSucceeded(std::move(closeFutures));
    }

    const NChunkClient::NProto::TChunkInfo& GetChunkInfo() const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return ChunkInfo_;
    }

    const NChunkClient::NProto::TDataStatistics& GetDataStatistics() const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        YT_UNIMPLEMENTED();
    }

    TWrittenChunkReplicasInfo GetWrittenChunkReplicasInfo() const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        // This method may only be called if the chunk was closed successfully,
        // so we can assume that the upload session was completed.
        YT_VERIFY(ChunkUploadSession_->IsUploadCompleted());

        TChunkReplicaWithLocation replica(
            OffshoreNodeId,
            GenericChunkReplicaIndex,
            SessionId_.MediumIndex,
            InvalidChunkLocationUuid,
            InvalidChunkLocationIndex);

        return {
            .Replicas = {std::move(replica)},
        };
    }

    TChunkId GetChunkId() const override
    {
        return SessionId_.ChunkId;
    }

    NErasure::ECodec GetErasureCodecId() const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return NErasure::ECodec::None;
    }

    bool IsCloseDemanded() const override
    {
        return false;
    }

    TFuture<void> Cancel() override
    {
        YT_UNIMPLEMENTED();
    }

private:
    const IClientPtr Client_;
    const TSessionId SessionId_;

    const NLogging::TLogger Logger;

    const TS3MultiPartUploadSessionPtr ChunkUploadSession_;
    const TS3SimpleUploadSessionPtr ChunkMetaUploadSession_;

    const NChunkClient::TRefCountedChunkMetaPtr ChunkMeta_ = New<NChunkClient::TRefCountedChunkMeta>();

    NChunkClient::NProto::TChunkInfo ChunkInfo_;
    NChunkClient::NProto::TBlocksExt BlocksExt_;

    i64 DataSize_ = 0;
};

////////////////////////////////////////////////////////////////////////////

IChunkWriterPtr CreateS3RegularChunkWriter(
    IClientPtr client,
    TS3MediumDescriptorPtr mediumDescriptor,
    TS3WriterConfigPtr config,
    TSessionId sessionId)
{
    YT_VERIFY(IsRegularChunkId(sessionId.ChunkId));
    YT_VERIFY(sessionId.MediumIndex == mediumDescriptor->GetIndex());

    return New<TS3Writer>(
        std::move(client),
        std::move(mediumDescriptor),
        std::move(config),
        sessionId);
}

////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NS3
