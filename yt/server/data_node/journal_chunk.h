#pragma once

#include "public.h"
#include "chunk_detail.h"

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

class TJournalChunk
    : public TChunk
{
public:
    TJournalChunk(
        TLocationPtr location,
        const TChunkId& chunkId,
        const NChunkClient::NProto::TChunkMeta& meta,
        const NChunkClient::NProto::TChunkInfo& info,
        NCellNode::TNodeMemoryTracker* memoryUsageTracker);

    virtual TAsyncGetMetaResult GetMeta(
        i64 priority,
        const std::vector<int>* tags = nullptr) override;

    virtual TAsyncError ReadBlocks(
        int firstBlockIndex,
        int blockCount,
        i64 priority,
        std::vector<TSharedRef>* blocks) override;

    virtual TRefCountedChunkMetaPtr GetCachedMeta() const override;

private:
    virtual void EvictFromCache() override;
    virtual TFuture<void> RemoveFiles() override;

    void DoReadBlocks(
        int firstBlockIndex,
        int blockCount,
        TPromise<TError> promise,
        std::vector<TSharedRef>* blocks);

};

DEFINE_REFCOUNTED_TYPE(TJournalChunk)

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT

