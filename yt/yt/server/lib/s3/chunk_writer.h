#pragma once

#include "public.h"

#include <yt/yt/ytlib/chunk_client/chunk_writer.h>
#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/library/s3/public.h>
#include <yt/ytlib/chunk_client/proto/chunk_info.pb.h>
#include <yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>

namespace NYT::NS3 {

////////////////////////////////////////////////////////////////////////////

NChunkClient::IChunkWriterPtr CreateS3RegularChunkWriter(
    NS3::IClientPtr client,
    NChunkClient::TS3MediumDescriptorPtr mediumDescriptor,
    TS3WriterConfigPtr config,
    NChunkClient::TSessionId sessionId);


struct IPhysicalChunkWriter
    : public virtual TRefCounted
{
    virtual TFuture<void> Open() = 0;

    struct TWriteRequest
    {
        i64 StartOffset = 0;
        i64 EndOffset = 0;
        std::vector<TSharedRef> Buffers;
        i64 BlockCount = 0;
    };

    virtual bool WriteBlocks(
        const NChunkClient::IChunkWriter::TWriteBlocksOptions& /*options*/,
        const TWorkloadDescriptor& /*workloadDescriptor*/,
        TWriteRequest request) = 0;

    virtual TFuture<void> GetReadyEvent() = 0;


    virtual TFuture<void> Close(
        const NChunkClient::IChunkWriter::TWriteBlocksOptions& options,
        const TWorkloadDescriptor& workloadDescriptor,
        const TSharedMutableRef& chunkMetaBlob,
        std::optional<int> truncateBlockCount) = 0;

    // virtual NChunkClient::TChunkId GetChunkId() const = 0;

    virtual const NChunkClient::NProto::TDataStatistics& GetDataStatistics() const = 0;
    virtual NChunkClient::TWrittenChunkReplicasInfo GetWrittenChunkReplicasInfo() const = 0;


    virtual NChunkClient::TChunkId GetChunkId() const = 0;
    virtual NErasure::ECodec GetErasureCodecId() const = 0;

    virtual bool IsCloseDemanded() const = 0;

    virtual TFuture<void> Cancel() = 0;
};

DEFINE_REFCOUNTED_TYPE(IPhysicalChunkWriter)

// TODO(cherepashka): rename + move to different file
// this adapter is the level where write requests (in shared ref) are formed and passed to underlying writer.
class TWrapperChunkWriter
    : public NChunkClient::IChunkWriter
{
public:
    TWrapperChunkWriter(IPhysicalChunkWriterPtr underlyingWriter);

    // IChunkWriter implementation.
    TFuture<void> Open() override;

    bool WriteBlock(
        const NChunkClient::IChunkWriter::TWriteBlocksOptions& options,
        const TWorkloadDescriptor& workloadDescriptor,
        const NChunkClient::TBlock& block) override;

    bool WriteBlocks(
        const IChunkWriter::TWriteBlocksOptions& options,
        const TWorkloadDescriptor& workloadDescriptor,
        const std::vector<NChunkClient::TBlock>& blocks) override;

    TFuture<void> GetReadyEvent() override;

    TFuture<void> Close(
        const NChunkClient::IChunkWriter::TWriteBlocksOptions& options,
        const TWorkloadDescriptor& workloadDescriptor,
        const NChunkClient::TDeferredChunkMetaPtr& chunkMeta,
        std::optional<int> truncateBlocks) override;

    // TFuture<void> Close(
    //     const NChunkClient::IChunkWriter::TWriteBlocksOptions& options,
    //     const TWorkloadDescriptor& workloadDescriptor,
    //     const NChunkClient::TDeferredChunkMetaPtr& chunkMeta,
    //     TFairShareSlotId fairShareSlotId,
    //     std::optional<int> truncateBlocks);

    const NChunkClient::NProto::TChunkInfo& GetChunkInfo() const override;
    const NChunkClient::NProto::TDataStatistics& GetDataStatistics() const override;
    NChunkClient::TWrittenChunkReplicasInfo GetWrittenChunkReplicasInfo() const override;

    NChunkClient::TChunkId GetChunkId() const override;

    NErasure::ECodec GetErasureCodecId() const override;

    bool IsCloseDemanded() const override;

    //! Aborts the writer and removes temporary files.
    TFuture<void> Cancel() override;
private:
    const IPhysicalChunkWriterPtr UnderlyingWriter_;

    const NChunkClient::TRefCountedChunkMetaPtr ChunkMeta_ = New<NChunkClient::TRefCountedChunkMeta>();
    const NLogging::TLogger Logger;

    NChunkClient::NProto::TChunkInfo ChunkInfo_;
    NChunkClient::NProto::TBlocksExt BlocksExt_;

    i64 DataSize_ = 0;
    i64 MetaDataSize_ = 0;
    // TODO: add state


void UpdateChunkInfoDiskSpace();

void FinalizeChunkMeta(NChunkClient::TDeferredChunkMetaPtr chunkMeta);

    TSharedMutableRef PrepareChunkMetaBlob();
};

DECLARE_REFCOUNTED_CLASS(TWrapperChunkWriter)

NChunkClient::IChunkWriterPtr CreateChunkLayoutWriterAdapter(IPhysicalChunkWriterPtr underlying);

////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NS3
