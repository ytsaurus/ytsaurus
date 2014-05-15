#pragma once

#include "public.h"
#include "reader.h"

namespace NYT {
namespace NChunkClient {

///////////////////////////////////////////////////////////////////////////////

class TMemoryReader
    : public IReader
{
public:
    TMemoryReader(
        NProto::TChunkMeta chunkMeta,
        std::vector<TSharedRef> blocks);

    virtual TAsyncReadResult ReadBlocks(const std::vector<int>& blockIndexes) override;

    virtual TAsyncGetMetaResult GetChunkMeta(
        const TNullable<int>& partitionTag = Null,
        const std::vector<int>* extensionTags = nullptr) override;

    // Unimplemented.
    virtual TChunkId GetChunkId() const override;

private:
    NProto::TChunkMeta ChunkMeta;
    std::vector<TSharedRef> Blocks;

};

DEFINE_REFCOUNTED_TYPE(TMemoryReader)

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
