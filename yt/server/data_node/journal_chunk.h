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

    virtual TFuture<TRefCountedChunkMetaPtr> ReadMeta(
        i64 priority,
        const TNullable<std::vector<int>>& extensionTags) override;

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
    const TRefCountedChunkMetaPtr Meta_ = New<TRefCountedChunkMeta>();

    bool Active_ = false;
    NHydra::IChangelogPtr Changelog_;
    
    mutable i64 CachedRowCount_ = 0;
    mutable i64 CachedDataSize_ = 0;
    mutable bool CachedSealed_ = false;


    TRefCountedChunkMetaPtr DoReadMeta(const TNullable<std::vector<int>>& extensionTags);

    void UpdateCachedParams() const;

    virtual TFuture<void> AsyncRemove() override;

    void DoReadBlocks(
        int firstBlockIndex,
        int blockCount,
        TPromise<std::vector<TSharedRef>> promise);

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

