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

    virtual void SyncRemove(bool force) override;

    void SetActive(bool value);
    virtual bool IsActive() const override;

    virtual NChunkClient::NProto::TChunkInfo GetInfo() const override;
    virtual TFuture<TRefCountedChunkMetaPtr> GetMeta(
        i64 priority,
        const std::vector<int>* tags = nullptr) override;

    virtual TFuture<std::vector<TSharedRef>> ReadBlocks(
        int firstBlockIndex,
        int blockCount,
        i64 priority) override;

    void AttachChangelog(NHydra::IChangelogPtr changelog);
    void DetachChangelog();
    bool HasAttachedChangelog() const;
    NHydra::IChangelogPtr GetAttachedChangelog() const;

    i64 GetRowCount() const;
    i64 GetDataSize() const;
    bool IsSealed() const;

private:
    bool Active_ = false;
    NHydra::IChangelogPtr Changelog_;
    
    mutable i64 CachedRowCount_ = 0;
    mutable i64 CachedDataSize_ = 0;
    mutable bool CachedSealed_ = false;

    void UpdateCachedParams() const;

    virtual TFuture<void> AsyncRemove() override;

    void DoReadBlocks(
        int firstBlockIndex,
        int blockCount,
        TPromise<std::vector<TSharedRef>> promise);

    void DoRemove();
    void DoMoveToTrash();

};

DEFINE_REFCOUNTED_TYPE(TJournalChunk)

////////////////////////////////////////////////////////////////////////////////

class TJournalChunkChangelogGuard
{
public:
    TJournalChunkChangelogGuard() = default;
    TJournalChunkChangelogGuard(TJournalChunkPtr chunk, NHydra::IChangelogPtr changelog);
    TJournalChunkChangelogGuard(TJournalChunkChangelogGuard&& other) = default;
    ~TJournalChunkChangelogGuard();

    TJournalChunkChangelogGuard& operator = (TJournalChunkChangelogGuard&& other);

    friend void swap(TJournalChunkChangelogGuard& lhs, TJournalChunkChangelogGuard& rhs);

private:
    TJournalChunkPtr Chunk_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT

