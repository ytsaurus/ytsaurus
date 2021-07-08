#pragma once

#include "chunk_detail.h"

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

class TJournalChunk
    : public TChunkBase
{
public:
    TJournalChunk(
        IBootstrap* bootstrap,
        TStoreLocationPtr location,
        const TChunkDescriptor& descriptor);

    const TStoreLocationPtr& GetStoreLocation() const;

    virtual void SyncRemove(bool force) override;

    void SetActive(bool value);
    virtual bool IsActive() const override;

    virtual NChunkClient::NProto::TChunkInfo GetInfo() const override;

    virtual TFuture<NChunkClient::TRefCountedChunkMetaPtr> ReadMeta(
        const TChunkReadOptions& options,
        const std::optional<std::vector<int>>& extensionTags) override;

    virtual TFuture<std::vector<NChunkClient::TBlock>> ReadBlockSet(
        const std::vector<int>& blockIndexes,
        const TChunkReadOptions& options) override;

    virtual TFuture<std::vector<NChunkClient::TBlock>> ReadBlockRange(
        int firstBlockIndex,
        int blockCount,
        const TChunkReadOptions& options) override;

    virtual NIO::IIOEngine::TReadRequest MakeChunkFragmentReadRequest(
        const NIO::TChunkFragmentDescriptor& fragmentDescriptor) override;

    i64 GetFlushedRowCount() const;
    void UpdateFlushedRowCount(i64 rowCount);

    i64 GetDataSize() const;
    void UpdateDataSize(i64 dataSize);

    TFuture<void> Seal();
    bool IsSealed() const;

private:
    IBootstrap* const Bootstrap_;

    const TStoreLocationPtr StoreLocation_;

    std::atomic<bool> Active_ = false;

    std::atomic<i64> FlushedRowCount_ = 0;
    std::atomic<i64> DataSize_ = 0;

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

