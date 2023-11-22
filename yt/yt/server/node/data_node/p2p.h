#pragma once

#include "public.h"

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/ytree/yson_serializable.h>

#include <yt/yt/core/misc/sync_cache.h>

#include <yt/yt/ytlib/chunk_client/block.h>
#include <yt/yt/ytlib/chunk_client/block_id.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

struct TP2PBlock
    : public TSyncCacheValueBase<NChunkClient::TBlockId, TP2PBlock>
{
    TP2PBlock(NChunkClient::TBlockId blockId, NChunkClient::TBlock block);

    NChunkClient::TBlock Block;
    std::atomic<i64> AccessCount = 0;
};

DEFINE_REFCOUNTED_TYPE(TP2PBlock)

////////////////////////////////////////////////////////////////////////////////

struct TP2PSession
{
    THashSet<TChunkId> ActiveChunks;
    TInstant LastUpdateTime = TInstant::Now();
    i64 LastIteration = 0;

    struct TWaiter final
    {
        TPromise<void> Promise;
        std::atomic<int> WaiterCount = 0;
    };

    THashMap<i64, TIntrusivePtr<TWaiter>> Waiters;
};

////////////////////////////////////////////////////////////////////////////////

class TP2PBlockCache
    : public virtual TRefCounted
    , public TMemoryTrackingSyncSlruCacheBase<NChunkClient::TBlockId, TP2PBlock>
{
public:
    TP2PBlockCache(
        const TP2PConfigPtr& config,
        const IInvokerPtr& invoker,
        const IMemoryUsageTrackerPtr& memoryTracker);

    void UpdateConfig(const TP2PConfigPtr& config);

    void FinishSessionIteration(TGuid sessionId, i64 iteration);
    TFuture<void> WaitSessionIteration(TGuid sessionId, i64 iteration);

    void HoldBlocks(
        TChunkId chunkId,
        const std::vector<int>& blockIndices,
        const std::vector<NChunkClient::TBlock>& blocks);

    std::vector<NChunkClient::TBlock> LookupBlocks(
        TChunkId chunkId,
        const std::vector<int>& blockIndices);

    TP2PConfigPtr Config();

private:
    TP2PConfigPtr Config_;
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, ConfigLock_);
    TP2PConfigPtr DynamicConfig_;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Lock_);
    THashMap<TGuid, TP2PSession> ActiveSessions_;
    std::atomic<int> ActiveWaiters_ = 0;

    NConcurrency::TPeriodicExecutorPtr StaleSessionCleaup_;

    i64 GetWeight(const TP2PBlockPtr& value) const override;
    void OnRemoved(const TP2PBlockPtr& value) override;

    NProfiling::TCounter WastedBytes_;
    NProfiling::TCounter DuplicateBytes_;
    NProfiling::TCounter MissedBlocks_;

    void CleanupOldSessions();
};

DEFINE_REFCOUNTED_TYPE(TP2PBlockCache)

////////////////////////////////////////////////////////////////////////////////

using TPeerList = TCompactVector<TNodeId, 8>;

struct TP2PSuggestion
{
    int BlockIndex;
    TPeerList Peers;

    TGuid P2PSessionId;
    i64 P2PIteration;
};

////////////////////////////////////////////////////////////////////////////////

struct TBlockAccessCounter
{
    std::atomic<i64> LastAccessTick{};
    std::atomic<int> AccessCount{};

    int Increment(i64 tick);

    TPeerList Peers;
    i64 DistributedAt = 0;

    TBlockAccessCounter() = default;
    TBlockAccessCounter(const TBlockAccessCounter& other);
};

struct TP2PChunk
    : public TSyncCacheValueBase<NChunkClient::TChunkId, TP2PChunk>
{
    TP2PChunk(NChunkClient::TChunkId blockId);

    std::atomic<NProfiling::TCpuInstant> LastAccessTime;
    std::atomic<bool> Hot = false;

    YT_DECLARE_SPIN_LOCK(NThreading::TReaderWriterSpinLock, BlocksLock);
    std::vector<TBlockAccessCounter> Blocks;
    i64 Weight = 0;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, PeersLock);
    i64 DistributedSize = 0;
    i64 PeersAllocatedAt = 0;
    TPeerList Peers;

    void Reserve(
        size_t size);
};

DEFINE_REFCOUNTED_TYPE(TP2PChunk)

////////////////////////////////////////////////////////////////////////////////

class TP2PSnooper
    : public virtual TRefCounted
    , public TMemoryTrackingSyncSlruCacheBase<NChunkClient::TChunkId, TP2PChunk>
{
public:
    TP2PSnooper(
        TP2PConfigPtr config,
        IMemoryUsageTrackerPtr memoryTracker);

    std::vector<TP2PSuggestion> OnBlockRead(
        TChunkId chunkId,
        const std::vector<int>& blockIndices,
        std::vector<NChunkClient::TBlock>* blocks,
        bool* throttledLargeBlock = nullptr,
        bool readFromP2P = false);

    std::vector<TP2PSuggestion> OnBlockProbe(
        TChunkId chunkId,
        const std::vector<int>& blockIndices);

    void UpdateConfig(const TP2PConfigPtr& config);
    void SetEligiblePeers(const std::vector<TNodeId>& peers);

    struct TQueuedBlock
    {
        TP2PChunkPtr Chunk;
        NChunkClient::TBlock Block;
        int BlockIndex;
        TPeerList Peers;
    };

    void FinishTick(
        i64 *currentTick,
        std::vector<TQueuedBlock>* hotBlocks);

    TGuid GetSessionId() const;

    std::vector<TP2PChunkPtr> GetHotChunks() const;
    void CoolChunk(const TP2PChunkPtr& chunk);

    i64 GetWeight(const TP2PChunkPtr& value) const override;

    TP2PConfigPtr GetConfig();

private:
    const TP2PConfigPtr Config_;
    const IMemoryUsageTrackerPtr MemoryUsageTracker_;

    TAtomicIntrusivePtr<TP2PConfig> DynamicConfig_;

    const TGuid SessionId_ = TGuid::Create();

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, NodesLock_);
    std::vector<TNodeId> EligibleNodes_;

    std::atomic<i64> CounterIteration_ = 0;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, QueueLock_);
    std::vector<TQueuedBlock> BlockQueue_;
    i64 CurrentTick_ = 1;

    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, ChunkLock_);
    THashSet<TP2PChunkPtr> HotChunks_;

    NProfiling::TCounter ThrottledBytes_;
    NProfiling::TCounter ThrottledLargeBlockBytes_;
    NProfiling::TCounter DistributedBytes_;
    NProfiling::TCounter HitBytes_;

    TPeerList AllocatePeers();
};

DEFINE_REFCOUNTED_TYPE(TP2PSnooper)

////////////////////////////////////////////////////////////////////////////////

class TP2PDistributor
    : public virtual TRefCounted
{
public:
    TP2PDistributor(
        TP2PConfigPtr config,
        IInvokerPtr invoker,
        IBootstrap* bootstrap);

    void Start();
    void UpdateConfig(const TP2PConfigPtr& config);

private:
    const TP2PConfigPtr Config_;
    const IInvokerPtr Invoker_;
    IBootstrap* const Bootstrap_;

    const TP2PSnooperPtr Snooper_;

    const NConcurrency::TPeriodicExecutorPtr DistributorExecutor_;
    const NConcurrency::TPeriodicExecutorPtr AllocatorExecutor_;
    const NConcurrency::TPeriodicExecutorPtr CoolerExecutor_;

    TAtomicIntrusivePtr<TP2PConfig> DynamicConfig_;

    NProfiling::TCounter DistributionErrorsCounter_;


    void DistributeBlocks();
    void AllocateNodes();
    void CoolHotChunks();

    TP2PConfigPtr GetConfig();
};

DEFINE_REFCOUNTED_TYPE(TP2PDistributor)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
