#pragma once

#include "public.h"
#include "writer.h"

#include <ytlib/chunk_client/chunk_meta.pb.h>

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

class TMemoryWriter
    : public IChunkWriter
{
public:
    virtual TAsyncError Open() override;
    virtual bool WriteBlock(const TSharedRef& block) override;
    virtual bool WriteBlocks(const std::vector<TSharedRef>& blocks) override;
    virtual TAsyncError GetReadyEvent() override;
    virtual TAsyncError Close(const NProto::TChunkMeta& chunkMeta) override;

    // Unimplemented.
    virtual const NProto::TChunkInfo& GetChunkInfo() const override;
    virtual TChunkReplicaList GetWrittenChunkReplicas() const override;

    //! Can only be called after the writer is closed.
    std::vector<TSharedRef>& GetBlocks();

    NProto::TChunkMeta& GetChunkMeta();

private:
    bool Open_ = false;
    bool Closed_ = false;

    std::vector<TSharedRef> Blocks_;
    NProto::TChunkMeta ChunkMeta_;

};

DEFINE_REFCOUNTED_TYPE(TMemoryWriter)

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

