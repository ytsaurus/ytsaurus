#pragma once

#include "chunk_detail.h"

#include <yt/yt/server/lib/hydra_common/public.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

class TJournalChunk
    : public TChunkBase
{
public:
    TJournalChunk(
        TChunkContextPtr context,
        TStoreLocationPtr location,
        const TChunkDescriptor& descriptor);

    const TStoreLocationPtr& GetStoreLocation() const;

    void SyncRemove(bool force) override;

    void SetActive(bool value);
    bool IsActive() const override;

    NChunkClient::NProto::TChunkInfo GetInfo() const override;

    TFuture<NChunkClient::TRefCountedChunkMetaPtr> ReadMeta(
        const TChunkReadOptions& options,
        const std::optional<std::vector<int>>& extensionTags) override;

    TFuture<std::vector<NChunkClient::TBlock>> ReadBlockSet(
        const std::vector<int>& blockIndexes,
        const TChunkReadOptions& options) override;

    TFuture<std::vector<NChunkClient::TBlock>> ReadBlockRange(
        int firstBlockIndex,
        int blockCount,
        const TChunkReadOptions& options) override;

    TFuture<void> PrepareToReadChunkFragments(
        const NChunkClient::TClientChunkReadOptions& options,
        bool useDirectIO) override;

    NIO::IIOEngine::TReadRequest MakeChunkFragmentReadRequest(
        const NIO::TChunkFragmentDescriptor& fragmentDescriptor,
        bool useDirectIO) override;

    i64 GetFlushedRowCount() const;
    void UpdateFlushedRowCount(i64 rowCount);

    i64 GetDataSize() const;
    void UpdateDataSize(i64 dataSize);

    TFuture<void> Seal();
    bool IsSealed() const;

private:
    const TStoreLocationPtr StoreLocation_;

    std::atomic<bool> Active_ = false;

    std::atomic<i64> FlushedRowCount_ = 0;
    std::atomic<i64> DataSize_ = 0;

    std::atomic<bool> Sealed_ = false;

    TWeakPtr<NHydra::IFileChangelog> WeakChangelog_;
    NHydra::IFileChangelogPtr Changelog_;
    TPromise<void> OpenChangelogPromise_;

    struct TReadBlockRangeSession
        : public TReadSessionBase
    {
        int FirstBlockIndex;
        int BlockCount;
        TPromise<std::vector<NChunkClient::TBlock>> Promise;
    };

    using TReadBlockRangeSessionPtr = TIntrusivePtr<TReadBlockRangeSession>;

    TFuture<void> AsyncRemove() override;

    void DoReadBlockRange(const TReadBlockRangeSessionPtr& session);

    NHydra::IFileChangelogPtr GetChangelog();
    void ReleaseReader(NThreading::TWriterGuard<NThreading::TReaderWriterSpinLock>& writerGuard) override;
};

DEFINE_REFCOUNTED_TYPE(TJournalChunk)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode

