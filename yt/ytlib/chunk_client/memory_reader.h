#pragma once

#include "public.h"
#include "async_reader.h"

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

class TMemoryReader
    : public IAsyncReader
{
public:
    TMemoryReader(
        NProto::TChunkMeta chunkMeta,
        std::vector<TSharedRef> blocks);

    virtual TAsyncReadResult AsyncReadBlocks(const std::vector<int>& blockIndexes) override;

    virtual TAsyncGetMetaResult AsyncGetChunkMeta(
        const TNullable<int>& partitionTag = Null,
        const std::vector<int>* tags = nullptr) override;

    // Unimplemented.
    virtual TChunkId GetChunkId() const override;

private:
    NProto::TChunkMeta ChunkMeta;
    std::vector<TSharedRef> Blocks;

};

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
