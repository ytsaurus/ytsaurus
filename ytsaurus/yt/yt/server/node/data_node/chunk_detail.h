#pragma once

#include "public.h"
#include "chunk.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/core/profiling/timing.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

//! Chunk properties that can be obtained during the filesystem scan.
struct TChunkDescriptor
{
    TChunkDescriptor() = default;

    explicit TChunkDescriptor(TChunkId id, i64 diskSpace = 0)
        : Id(id)
        , DiskSpace(diskSpace)
    { }

    TChunkId Id;
    i64 DiskSpace = 0;

    // For journal chunks only.
    i64 RowCount = 0;
    bool Sealed = false;
};

////////////////////////////////////////////////////////////////////////////////

struct TChunkContext final
{
    IChunkMetaManagerPtr ChunkMetaManager;

    IPrioritizedInvokerPtr StorageHeavyInvoker;
    IInvokerPtr StorageLightInvoker;
    TDataNodeConfigPtr DataNodeConfig;

    TChunkReaderSweeperPtr ChunkReaderSweeper;
    // NB: might be null for exec node
    IJournalDispatcherPtr JournalDispatcher;
    IBlobReaderCachePtr BlobReaderCache;

    static TChunkContextPtr Create(NClusterNode::IBootstrapBase* bootstrap);
};

DEFINE_REFCOUNTED_TYPE(TChunkContext)

////////////////////////////////////////////////////////////////////////////////

//! A base for any IChunk implementation.
class TChunkBase
    : public IChunk
{
public:
    TChunkId GetId() const override;
    const TChunkLocationPtr& GetLocation() const override;
    TString GetFileName() const override;

    int GetVersion() const override;
    int IncrementVersion() override;

    TFuture<void> PrepareToReadChunkFragments(
        const NChunkClient::TClientChunkReadOptions& options,
        bool useDirectIO) override;
    NIO::IIOEngine::TReadRequest MakeChunkFragmentReadRequest(
        const NIO::TChunkFragmentDescriptor& fragmentDescriptor,
        bool useDirectIO) override;

    void AcquireReadLock() override;
    void ReleaseReadLock() override;

    void AcquireUpdateLock() override;
    void ReleaseUpdateLock() override;

    TFuture<void> ScheduleRemove() override;
    bool IsRemoveScheduled() const override;

    void TrySweepReader() override;

protected:
    const TChunkContextPtr Context_;
    const TChunkLocationPtr Location_;
    const TChunkId Id_;

    std::atomic<int> Version_ = 0;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, LifetimeLock_);
    std::atomic<int> ReadLockCounter_ = 0;
    int UpdateLockCounter_ = 0;
    TFuture<void> RemovedFuture_;
    TPromise<void> RemovedPromise_;
    std::atomic<bool> RemoveScheduled_ = false;
    std::atomic<bool> Removing_ = false;
    // Incremented by 2 on each read lock acquisition since last sweep scheduling.
    // The lowest bit indicates if sweep has been scheduled.
    std::atomic<ui64> ReaderSweepLatch_ = 0;


    struct TReadSessionBase
        : public TRefCounted
    {
        NProfiling::TWallTimer SessionTimer;
        std::optional<TChunkReadGuard> ChunkReadGuard;
        TChunkReadOptions Options;
    };

    using TReadSessionBasePtr = TIntrusivePtr<TReadSessionBase>;


    struct TReadMetaSession
        : public TReadSessionBase
    { };

    using TReadMetaSessionPtr = TIntrusivePtr<TReadMetaSession>;

    TChunkBase(
        TChunkContextPtr context,
        TChunkLocationPtr location,
        TChunkId id);
    ~TChunkBase();

    void StartAsyncRemove();
    virtual TFuture<void> AsyncRemove() = 0;

    virtual void ReleaseReader(NThreading::TWriterGuard<NThreading::TReaderWriterSpinLock>& writerGuard);

    static NChunkClient::TRefCountedChunkMetaPtr FilterMeta(
        NChunkClient::TRefCountedChunkMetaPtr meta,
        const std::optional<std::vector<int>>& extensionTags);

    void StartReadSession(
        const TReadSessionBasePtr& session,
        const TChunkReadOptions& options);
    void ProfileReadBlockSetLatency(const TReadSessionBasePtr& session);
    void ProfileReadMetaLatency(const TReadSessionBasePtr& session);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
