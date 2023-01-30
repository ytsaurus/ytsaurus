#pragma once

#include "public.h"
#include "chunk_writer.h"
#include "block.h"

#include <yt/yt/ytlib/chunk_client/proto/chunk_info.pb.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>

namespace NYT::NChunkClient {

////////////////////////////////////////////////////////////////////////////////

class TMemoryWriter
    : public IChunkWriter
{
public:
    // IChunkWriter implementation.
    TFuture<void> Open() override;
    bool WriteBlock(
        const TWorkloadDescriptor& workloadDescriptor,
        const TBlock& block) override;

    bool WriteBlocks(
        const TWorkloadDescriptor& workloadDescriptor,
        const std::vector<TBlock>& blocks) override;

    TFuture<void> GetReadyEvent() override;

    TFuture<void> Close(
        const TWorkloadDescriptor& workloadDescriptor,
        const TDeferredChunkMetaPtr& chunkMeta) override;

    //! Unimplemented.
    const NProto::TChunkInfo& GetChunkInfo() const override;
    //! Unimplemented.
    const NProto::TDataStatistics& GetDataStatistics() const override;
    //! Unimplemented.
    TChunkReplicaWithLocationList GetWrittenChunkReplicas() const override;
    //! Returns #NullChunkId.
    TChunkId GetChunkId() const override;
    NErasure::ECodec GetErasureCodecId() const override;

    bool IsCloseDemanded() const override;

    TFuture<void> Cancel() override;

    //! Can only be called after the writer is closed.
    std::vector<TBlock>& GetBlocks();
    //! Can only be called after the writer is closed.
    TRefCountedChunkMetaPtr GetChunkMeta();

private:
    bool Open_ = false;
    bool Closed_ = false;

    std::vector<TBlock> Blocks_;
    TRefCountedChunkMetaPtr ChunkMeta_;

    NProto::TChunkInfo ChunkInfo_;
};

DEFINE_REFCOUNTED_TYPE(TMemoryWriter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient

