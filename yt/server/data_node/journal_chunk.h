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
        const TChunkDescriptor& descriptor);

    virtual void SyncRemove() override;

    void SetActive(bool value);
    virtual bool IsActive() const override;

    virtual NChunkClient::NProto::TChunkInfo GetInfo() const override;
    virtual TAsyncGetMetaResult GetMeta(
        i64 priority,
        const std::vector<int>* tags = nullptr) override;

    virtual TAsyncReadBlocksResult ReadBlocks(
        int firstBlockIndex,
        int blockCount,
        i64 priority) override;

    void AttachChangelog(NHydra::IChangelogPtr changelog);
    void DetachChangelog();
    bool HasAttachedChangelog() const;

    i64 GetRowCount() const;

private:
    bool Active_ = false;
    NHydra::IChangelogPtr Changelog_;
    
    mutable i64 CachedRowCount_ = 0;
    mutable i64 CachedDataSize_ = 0;
    mutable bool CachedSealed_ = false;

    void UpdateCachedParams() const;

    virtual void EvictFromCache() override;
    virtual TFuture<void> AsyncRemove() override;

    void DoReadBlocks(
        int firstBlockIndex,
        int blockCount,
        TPromise<TReadBlocksResult> promise);

};

DEFINE_REFCOUNTED_TYPE(TJournalChunk)

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT

