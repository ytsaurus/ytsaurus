#pragma once

#include "public.h"

#include "block.h"
#include "chunk_writer.h"

#include <yt/yt/ytlib/chunk_client/proto/chunk_info.pb.h>

#include <yt/yt/client/chunk_client/data_statistics.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

class TMemoryWriter
    : public IChunkWriter
{
public:
    explicit TMemoryWriter(TChunkId chunkId = NullChunkId);

    // IChunkWriter implementation.
    TFuture<void> Open() override;
    bool WriteBlock(
        const IChunkWriter::TWriteBlocksOptions& options,
        const TWorkloadDescriptor& workloadDescriptor,
        const TBlock& block) override;

    bool WriteBlocks(
        const IChunkWriter::TWriteBlocksOptions& options,
        const TWorkloadDescriptor& workloadDescriptor,
        const std::vector<TBlock>& blocks) override;

    TFuture<void> GetReadyEvent() override;

    TFuture<void> Close(
        const IChunkWriter::TWriteBlocksOptions& options,
        const TWorkloadDescriptor& workloadDescriptor,
        const TDeferredChunkMetaPtr& chunkMeta) override;

    const NProto::TChunkInfo& GetChunkInfo() const override;
    const NProto::TDataStatistics& GetDataStatistics() const override;
    TWrittenChunkReplicasInfo GetWrittenChunkReplicasInfo() const override;
    TChunkId GetChunkId() const override;
    NErasure::ECodec GetErasureCodecId() const override;

    bool IsCloseDemanded() const override;

    TFuture<void> Cancel() override;

    //! Can only be called after the writer is closed.
    std::vector<TBlock>& GetBlocks();
    //! Can only be called after the writer is closed.
    TRefCountedChunkMetaPtr GetChunkMeta();

private:
    const TChunkId ChunkId_;

    bool Open_ = false;
    bool Closed_ = false;

    std::vector<TBlock> Blocks_;
    TRefCountedChunkMetaPtr ChunkMeta_;

    NProto::TChunkInfo ChunkInfo_;
    NProto::TDataStatistics Statistics_;

    void FinalizeStatistics(const TDeferredChunkMetaPtr& chunkMeta);
};

DEFINE_REFCOUNTED_TYPE(TMemoryWriter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient

