#pragma once

#include "public.h"
#include "chunk_detail.h"

#include <server/hydra/public.h>

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

class TJournalChunk
    : public TChunkBase
{
public:
    TJournalChunk(
        NCellNode::TBootstrap* bootstrap,
        TLocationPtr location,
        const TChunkId& id,
        const NChunkClient::NProto::TChunkInfo& info);

    virtual bool IsActive() const override;

    virtual TAsyncGetMetaResult GetMeta(
        i64 priority,
        const std::vector<int>* tags = nullptr) override;

    virtual TAsyncReadBlocksResult ReadBlocks(
        int firstBlockIndex,
        int blockCount,
        i64 priority) override;

    void AttachChangelog(NHydra::IChangelogPtr changelog);
    void DetachChangelog();

private:
    NHydra::IChangelogPtr Changelog_;

    void UpdateInfo();

    virtual void EvictFromCache() override;
    virtual TFuture<void> RemoveFiles() override;

    void DoReadBlocks(
        int firstBlockIndex,
        int blockCount,
        TPromise<TReadBlocksResult> promise);

};

DEFINE_REFCOUNTED_TYPE(TJournalChunk)

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT

