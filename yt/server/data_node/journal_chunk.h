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

    virtual TAsyncGetMetaResult GetMeta(
        i64 priority,
        const std::vector<int>* tags = nullptr) override;

    virtual TAsyncError ReadBlocks(
        int firstBlockIndex,
        int blockCount,
        i64 priority,
        std::vector<TSharedRef>* blocks) override;

    void SetChangelog(NHydra::IChangelogPtr changelog);
    void ResetChangelog();

private:
    NHydra::IChangelogPtr Changelog_;

    void UpdateInfo();

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

