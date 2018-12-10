#pragma once

#include "public.h"
#include "chunk_detail.h"

#include <yt/server/hydra/public.h>

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

class TJournalChunk
    : public TChunkBase
{
public:
    TJournalChunk(
        NCellNode::TBootstrap* bootstrap,
        TStoreLocationPtr location,
        const TChunkDescriptor& descriptor);

    TStoreLocationPtr GetStoreLocation() const;

    virtual void SyncRemove(bool force) override;

    void SetActive(bool value);
    virtual bool IsActive() const override;

    virtual NChunkClient::NProto::TChunkInfo GetInfo() const override;

    virtual TFuture<NChunkClient::TRefCountedChunkMetaPtr> ReadMeta(
        const TBlockReadOptions& options,
        const std::optional<std::vector<int>>& extensionTags) override;

    virtual TFuture<std::vector<NChunkClient::TBlock>> ReadBlockSet(
        const std::vector<int>& blockIndexes,
        const TBlockReadOptions& options) override;

    virtual TFuture<std::vector<NChunkClient::TBlock>> ReadBlockRange(
        int firstBlockIndex,
        int blockCount,
        const TBlockReadOptions& options) override;

    void AttachChangelog(NHydra::IChangelogPtr changelog);
    void DetachChangelog();
    bool HasAttachedChangelog() const;
    NHydra::IChangelogPtr GetAttachedChangelog() const;

    i64 GetRowCount() const;
    i64 GetDataSize() const;

    TFuture<void> Seal();
    bool IsSealed() const;

private:
    const TStoreLocationPtr StoreLocation_;

    const NChunkClient::TRefCountedChunkMetaPtr Meta_;

    bool Active_ = false;
    NHydra::IChangelogPtr Changelog_;

    mutable i64 CachedRowCount_ = 0;
    mutable i64 CachedDataSize_ = 0;

    mutable bool Sealed_ = false;


    NChunkClient::TRefCountedChunkMetaPtr DoReadMeta(const std::optional<std::vector<int>>& extensionTags);

    void UpdateCachedParams() const;

    virtual TFuture<void> AsyncRemove() override;

    void DoReadBlockRange(
        int firstBlockIndex,
        int blockCount,
        NChunkClient::TChunkReaderStatisticsPtr chunkReaderStatistics,
        TPromise<std::vector<NChunkClient::TBlock>> promise);

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

