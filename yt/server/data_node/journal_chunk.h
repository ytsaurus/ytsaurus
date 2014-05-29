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
        NCellNode::TBootstrap* bootstrap,
        TLocationPtr location,
        const TChunkId& id,
        const NChunkClient::NProto::TChunkInfo& info);

    virtual TAsyncGetMetaResult GetMeta(
        i64 priority,
        const std::vector<int>* tags = nullptr) override;

    virtual TAsyncError ReadBlocks(
        int firstBlockIndex,
        int blockCount,
        i64 priority,
        std::vector<TSharedRef>* blocks) override;


    void SetRecordCount(int recordCount);
    void SetSealed(bool value);

    static TNullable<TChunkDescriptor> TryGetDescriptor(
        const TChunkId& id,
        const Stroka& fileName);

private:
    std::atomic<int> RecordCount_;
    std::atomic<bool> Sealed_;

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

