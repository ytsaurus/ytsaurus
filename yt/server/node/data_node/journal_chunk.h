#pragma once

#include "chunk_detail.h"

#include <yt/server/lib/hydra/public.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

class TJournalChunk
    : public TChunkBase
{
public:
    TJournalChunk(
        NClusterNode::TBootstrap* bootstrap,
        TStoreLocationPtr location,
        const TChunkDescriptor& descriptor);

    const TStoreLocationPtr& GetStoreLocation() const;

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

    void UpdateCachedParams(const NHydra::IChangelogPtr& changelog);
    i64 GetCachedRowCount() const;
    i64 GetCachedDataSize() const;

    TFuture<void> Seal();
    bool IsSealed() const;

private:
    const TStoreLocationPtr StoreLocation_;

    std::atomic<bool> Active_ = false;

    std::atomic<i64> CachedRowCount_ = 0;
    std::atomic<i64> CachedDataSize_ = 0;
    
    std::atomic<bool> Sealed_ = false;

    struct TReadBlockRangeSession
        : public TReadSessionBase
    {
        int FirstBlockIndex;
        int BlockCount;
        TPromise<std::vector<NChunkClient::TBlock>> Promise;
    };

    using TReadBlockRangeSessionPtr = TIntrusivePtr<TReadBlockRangeSession>;

    virtual TFuture<void> AsyncRemove() override;

    void DoReadBlockRange(const TReadBlockRangeSessionPtr& session);
};

DEFINE_REFCOUNTED_TYPE(TJournalChunk)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode

