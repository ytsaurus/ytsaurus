#pragma once

#include "public.h"
#include "async_writer.h"

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

class TMemoryWriter
    : public IAsyncWriter
{
public:
    TMemoryWriter();

    virtual void Open() override;
    virtual bool WriteBlock(const TSharedRef& block) override;
    virtual TAsyncError GetReadyEvent() override;
    virtual TAsyncError AsyncClose(const NProto::TChunkMeta& chunkMeta) override;

    // Unimplemented.
    virtual const NProto::TChunkInfo& GetChunkInfo() const override;
    virtual const std::vector<int> GetWrittenIndexes() const override;

    // Possible to call after #AsyncClose.
    std::vector<TSharedRef>& GetBlocks();
    NProto::TChunkMeta& GetMeta();

private:
    bool IsClosed;
    std::vector<TSharedRef> Blocks;
    NProto::TChunkMeta Meta;

};

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT

