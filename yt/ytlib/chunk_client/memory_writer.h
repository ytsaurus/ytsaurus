#pragma once

#include "public.h"
#include "chunk_writer.h"
#include "block.h"

#include <yt/client/chunk_client/proto/chunk_meta.pb.h>

namespace NYT {
namespace NChunkClient {

////////////////////////////////////////////////////////////////////////////////

class TMemoryWriter
    : public IChunkWriter
{
public:
    // IChunkWriter implementation.
    virtual TFuture<void> Open() override;
    virtual bool WriteBlock(const TBlock& block) override;
    virtual bool WriteBlocks(const std::vector<TBlock>& blocks) override;
    virtual TFuture<void> GetReadyEvent() override;
    virtual TFuture<void> Close(const TRefCountedChunkMetaPtr& chunkMeta) override;

    //! Unimplemented.
    virtual const NProto::TChunkInfo& GetChunkInfo() const override;
    //! Unimplemented.
    virtual const NProto::TDataStatistics& GetDataStatistics() const override;
    //! Unimplemented.
    virtual TChunkReplicaList GetWrittenChunkReplicas() const override;
    //! Returns #NullChunkId.
    virtual TChunkId GetChunkId() const override;
    virtual NErasure::ECodec GetErasureCodecId() const override;

    virtual bool HasSickReplicas() const override;

    //! Can only be called after the writer is closed.
    std::vector<TBlock>& GetBlocks();
    //! Can only be called after the writer is closed.
    TRefCountedChunkMetaPtr GetChunkMeta();

private:
    bool Open_ = false;
    bool Closed_ = false;

    std::vector<TBlock> Blocks_;
    TRefCountedChunkMetaPtr ChunkMeta_;

};

DEFINE_REFCOUNTED_TYPE(TMemoryWriter)

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

