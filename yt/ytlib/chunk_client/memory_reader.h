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
        std::vector<TSharedRef>&& blocks,
        NProto::TChunkMeta&& meta);

    virtual TAsyncReadResult AsyncReadBlocks(const std::vector<int>& blockIndexes) override;

    virtual TAsyncGetMetaResult AsyncGetChunkMeta(
        const TNullable<int>& partitionTag = Null,
        const std::vector<int>* tags = nullptr) override;

    // Unimplemented.
    virtual TChunkId GetChunkId() const override;

private:
    std::vector<TSharedRef> Blocks;
    NProto::TChunkMeta Meta;

};

///////////////////////////////////////////////////////////////////////////////

} // namespace NChunkClient
} // namespace NYT
