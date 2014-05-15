#pragma once

#include "public.h"
#include "writer.h"

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

class TMemoryWriter
    : public IWriter
{
public:
    TMemoryWriter();

    virtual void Open() override;
    virtual bool WriteBlock(const TSharedRef& block) override;
    virtual TAsyncError GetReadyEvent() override;
    virtual TAsyncError Close(const NProto::TChunkMeta& chunkMeta) override;

    // Unimplemented.
    virtual const NProto::TChunkInfo& GetChunkInfo() const override;
    virtual const std::vector<int> GetWrittenIndexes() const override;

    // Possible to call after #AsyncClose.
    std::vector<TSharedRef>& GetBlocks();
    NProto::TChunkMeta& GetChunkMeta();

private:
    bool IsClosed_;
    std::vector<TSharedRef> Blocks_;
    NProto::TChunkMeta ChunkMeta_;

};

DEFINE_REFCOUNTED_TYPE(TMemoryWriter)

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

