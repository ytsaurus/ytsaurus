#include "p2p.h"

#include "private.h"
#include "config.h"
#include "bootstrap.h"

#include <yt/yt/ytlib/chunk_client/block_id.h>
#include <yt/yt/ytlib/chunk_client/helpers.h>

#include <yt/yt/ytlib/api/native/client.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/chunk_client/data_node_service_proxy.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <util/random/random.h>

#include <algorithm>

namespace NYT::NDataNode {

using namespace NChunkClient;
using namespace NConcurrency;
using namespace NRpc;
using namespace NProfiling;

static const auto& Logger = P2PLogger;

////////////////////////////////////////////////////////////////////////////////

TP2PBlock::TP2PBlock(NChunkClient::TBlockId blockId, NChunkClient::TBlock block)
    : TSyncCacheValueBase<NChunkClient::TBlockId, TP2PBlock>(blockId)
    , Block(std::move(block))
{ }

////////////////////////////////////////////////////////////////////////////////

TP2PBlockCache::TP2PBlockCache(
    const TP2PConfigPtr& config,
    const IInvokerPtr& invoker,
    const IMemoryUsageTrackerPtr& memoryTracker)
    : TMemoryTrackingSyncSlruCacheBase<TBlockId, TP2PBlock>(
        config->BlockCache,
        memoryTracker,
        P2PProfiler.WithPrefix("/cache"))
    , Config_(config)
    , StaleSessionCleaup_(New<TPeriodicExecutor>(
        invoker,
        BIND(&TP2PBlockCache::CleanupOldSessions, MakeWeak(this)),
        Config_->SessionCleaupPeriod))
    , WastedBytes_(P2PProfiler.Counter("/wasted_bytes"))
    , DuplicateBytes_(P2PProfiler.Counter("/duplicate_bytes"))
    , MissedBlocks_(P2PProfiler.Counter("/missed_blocks"))
{
    P2PProfiler.AddFuncGauge("/active_sessions", MakeStrong(this), [this] {
        auto guard = Guard(Lock_);
        return ActiveSessions_.size();
    });

    P2PProfiler.AddFuncGauge("/active_waiters", MakeStrong(this), [this] {
        return ActiveWaiters_.load();
    });
}

void TP2PBlockCache::UpdateConfig(const TP2PConfigPtr& config)
{
    if (config) {
        TMemoryTrackingSyncSlruCacheBase<TBlockId, TP2PBlock>::Reconfigure(config->BlockCacheOverride);
    } else {
        TMemoryTrackingSyncSlruCacheBase<TBlockId, TP2PBlock>::Reconfigure(New<TSlruCacheDynamicConfig>());
    }

    {
        auto guard = Guard(ConfigLock_);
        DynamicConfig_ = config;
    }

    StaleSessionCleaup_->SetPeriod(Config()->SessionCleaupPeriod);
}

TP2PConfigPtr TP2PBlockCache::Config()
{
    auto guard = Guard(ConfigLock_);
    if (DynamicConfig_) {
        return DynamicConfig_;
    }
    return Config_;
}

void TP2PBlockCache::FinishSessionIteration(TGuid sessionId, i64 iteration)
{
    auto guard = Guard(Lock_);
    auto& session = ActiveSessions_[sessionId];

    session.LastIteration = iteration;
    session.LastUpdateTime = TInstant::Now();

    std::vector<i64> toRemove;
    for (const auto& [waiterIteration, promise] : session.Waiters) {
        if (waiterIteration <= iteration) {
            toRemove.push_back(waiterIteration);
            promise->Promise.TrySet();
        }
    }
    for (auto iteration : toRemove) {
        session.Waiters.erase(iteration);
    }
}

TFuture<void> TP2PBlockCache::WaitSessionIteration(TGuid sessionId, i64 iteration)
{
    auto config = Config();
    if (!config->Enabled) {
        return VoidFuture;
    }

    if (ActiveWaiters_.load() > config->MaxWaitingRequests) {
        return VoidFuture;
    }

    auto guard = Guard(Lock_);

    auto& session = ActiveSessions_[sessionId];
    if (iteration <= session.LastIteration) {
        return VoidFuture;
    }

    auto it = session.Waiters.find(iteration);
    if (it != session.Waiters.end()) {
        if (it->second->WaiterCount++ > 0) {
            ActiveWaiters_++;
        }

        return it->second->Promise;
    }

    auto waiter = New<TP2PSession::TWaiter>();
    waiter->Promise = NewPromise<void>();
    waiter->WaiterCount = 1;
    ActiveWaiters_++;

    waiter->Promise.ToFuture().Subscribe(BIND([this, this_=MakeStrong(this), waiter] (const TError& /*error*/) {
        ActiveWaiters_ -= waiter->WaiterCount.exchange(Min<int>());
    }));

    TDelayedExecutor::Submit(
        BIND([waiter] {
            waiter->Promise.TrySet();
        }),
        config->IterationWaitTimeout);

    session.Waiters[iteration] = waiter;
    return waiter->Promise.ToFuture();
}

void TP2PBlockCache::HoldBlocks(
    TChunkId chunkId,
    const std::vector<int>& blockIndices,
    const std::vector<NChunkClient::TBlock>& blocks)
{
    YT_VERIFY(blockIndices.size() == blocks.size());

    for (int i = 0; i < std::ssize(blockIndices); i++) {
        auto p2pBlock = New<TP2PBlock>(
            TBlockId{chunkId, blockIndices[i]},
            blocks[i]);

        if (!TryInsert(p2pBlock)) {
            DuplicateBytes_.Increment(blocks[i].Size());
        }
    }
}

std::vector<NChunkClient::TBlock> TP2PBlockCache::LookupBlocks(TChunkId chunkId, const std::vector<int>& blockIndices)
{
    std::vector<NChunkClient::TBlock> blocks;
    blocks.resize(blockIndices.size());

    for (int i = 0; i < std::ssize(blockIndices); i++) {
        if (auto p2pBlock = Find({chunkId, blockIndices[i]})) {
            p2pBlock->AccessCount++;
            blocks[i] = p2pBlock->Block;
        } else {
            MissedBlocks_.Increment();
        }
    }

    return blocks;
}

i64 TP2PBlockCache::GetWeight(const TP2PBlockPtr& value) const
{
    return value->Block.Size();
}

void TP2PBlockCache::OnRemoved(const TP2PBlockPtr& value)
{
    if (value->AccessCount == 0) {
        WastedBytes_.Increment(value->Block.Size());
    }

    TMemoryTrackingSyncSlruCacheBase<TBlockId, TP2PBlock>::OnRemoved(value);
}

void TP2PBlockCache::CleanupOldSessions()
{
    auto sessionCutoff = TInstant::Now() - Config()->SessionTTL;

    auto guard = Guard(Lock_);

    std::vector<TGuid> deadSessions;
    for (const auto& [sessionId, session] : ActiveSessions_) {
        if (session.LastUpdateTime < sessionCutoff) {
            deadSessions.push_back(sessionId);
        }
    }

    for (auto sessionId : deadSessions) {
        ActiveSessions_.erase(sessionId);
    }
}

////////////////////////////////////////////////////////////////////////////////

int TBlockAccessCounter::Increment(i64 tick)
{
    auto lastTick = LastAccessTick.load();
    if (tick != lastTick) {
        LastAccessTick = tick;
        AccessCount.store(0);
    }

    return AccessCount.fetch_add(1) + 1;
}

TBlockAccessCounter::TBlockAccessCounter(const TBlockAccessCounter& other)
    : LastAccessTick{other.LastAccessTick.load()}
    , AccessCount{other.AccessCount.load()}
{ }

TP2PChunk::TP2PChunk(NChunkClient::TChunkId blockId)
    : TSyncCacheValueBase<NChunkClient::TChunkId, TP2PChunk>(blockId)
{ }

void TP2PChunk::Reserve(size_t size)
{
    auto guard = WriterGuard(BlocksLock);
    if (Blocks.size() < size) {
        Blocks.resize(size);
    }
}

////////////////////////////////////////////////////////////////////////////////

TP2PSnooper::TP2PSnooper(TP2PConfigPtr config)
    : TSyncSlruCacheBase<NChunkClient::TChunkId, TP2PChunk>(
        config->RequestCache,
        P2PProfiler.WithPrefix("/request_cache"))
    , Config_(std::move(config))
    , ThrottledBytes_(P2PProfiler.Counter("/throttled_bytes"))
    , ThrottledLargeBlockBytes_(P2PProfiler.Counter("/throttled_large_block_bytes"))
    , DistributedBytes_(P2PProfiler.Counter("/distributed_bytes"))
    , HitBytes_(P2PProfiler.Counter("/hit_bytes"))
{
    P2PProfiler.AddFuncGauge("/hot_chunks", MakeStrong(this), [this] {
        auto guard = Guard(ChunkLock_);
        return HotChunks_.size();
    });

    P2PProfiler.AddFuncGauge("/eligible_nodes", MakeStrong(this), [this] {
        auto guard = Guard(NodesLock_);
        return EligibleNodes_.size();
    });
}

void TP2PSnooper::UpdateConfig(const TP2PConfigPtr& config)
{
    auto cacheConfig = config  ? config->RequestCacheOverride : New<TSlruCacheDynamicConfig>();
    TSyncSlruCacheBase<NChunkClient::TChunkId, TP2PChunk>::Reconfigure(cacheConfig);

    DynamicConfig_.Store(config);
}

TP2PConfigPtr TP2PSnooper::GetConfig()
{
    auto dynamicConfig = DynamicConfig_.Acquire();
    return dynamicConfig ? dynamicConfig : Config_;
}

std::vector<TP2PSuggestion> TP2PSnooper::OnBlockRead(
    TChunkId chunkId,
    const std::vector<int>& blockIndices,
    std::vector<NChunkClient::TBlock>* blocks,
    bool* throttledLargeBlock,
    bool readFromP2P)
{
    auto config = GetConfig();
    if (!config->Enabled) {
        return {};
    }

    YT_VERIFY(!blockIndices.empty());

    auto chunk = Find(chunkId);
    if (!chunk) {
        chunk = New<TP2PChunk>(chunkId);
        TryInsert(chunk, &chunk);
    }

    chunk->LastAccessTime = NProfiling::GetCpuInstant();
    chunk->Reserve(*std::max_element(blockIndices.begin(), blockIndices.end()) + 1);

    std::vector<TP2PSuggestion> suggestions;

    auto queueLock = Guard(QueueLock_);
    auto currentTick = CurrentTick_;
    queueLock.Release();

    auto guard = ReaderGuard(chunk->BlocksLock);
    for (int i = 0; i < std::ssize(*blocks); i++) {
        if (!(*blocks)[i]) {
            continue;
        }

        auto blockIndex = blockIndices[i];

        auto& blockCounter = chunk->Blocks[blockIndex];
        auto accessCount = blockCounter.Increment(CounterIteration_.load());

        bool blockIsHot = false;
        if (!chunk->Hot) {
            if (accessCount > config->HotBlockThreshold) {
                blockIsHot = true;

                if (!chunk->Hot.exchange(true)) {
                    YT_LOG_DEBUG("Chunk is hot (ChunkId: %v)", chunk->GetKey());

                    auto guard = Guard(ChunkLock_);
                    HotChunks_.insert(chunk);
                }
            }
        } else {
            if (accessCount > config->SecondHotBlockThreshold) {
                blockIsHot = true;
            }
        }

        if (blockIsHot && (*blocks)[i].Size() >= static_cast<size_t>(config->MaxBlockSize)) {
            ThrottledBytes_.Increment((*blocks)[i].Size());
            ThrottledLargeBlockBytes_.Increment((*blocks)[i].Size());
            (*blocks)[i] = {};

            if (throttledLargeBlock) {
                *throttledLargeBlock = true;
            }

            continue;
        }

        TPeerList blockPeers;
        i64 distributedAt;

        {
            auto guard = Guard(chunk->PeersLock);
            if (blockCounter.DistributedAt != 0 &&
                currentTick - blockCounter.DistributedAt >= config->BlockRedistributionTicks
            ) {
                if (chunk->PeersAllocatedAt == blockCounter.DistributedAt) {
                    YT_LOG_DEBUG("Resetting chunk peers (ChunkId: %v, Reason: Period)", chunk->GetKey());
                    chunk->Peers = {};
                    chunk->DistributedSize = 0;
                }

                blockCounter.Peers = {};
                blockCounter.DistributedAt = 0;
            }

            blockPeers = blockCounter.Peers;
            distributedAt = blockCounter.DistributedAt;
        }

        if (blockIsHot) {
            if (blockPeers.empty()) {
                auto peersGuard = Guard(chunk->PeersLock);
                auto guard = Guard(QueueLock_);

                if (blockCounter.DistributedAt == 0) {
                    if (chunk->DistributedSize > config->MaxDistributedBytes) {
                        chunk->Peers = {};
                        chunk->DistributedSize = 0;

                        YT_LOG_DEBUG("Resetting chunk peers by size (ChunkId: %v, Reason: Size)", chunk->GetKey());
                    }

                    if (chunk->Peers.empty()) {
                        chunk->Peers = AllocatePeers();
                        chunk->PeersAllocatedAt = CurrentTick_;
                    }

                    blockCounter.Peers = chunk->Peers;
                    if (!blockCounter.Peers.empty()) {
                        blockCounter.DistributedAt = CurrentTick_;
                        chunk->DistributedSize += (*blocks)[i].Size();

                        BlockQueue_.push_back(TQueuedBlock{
                            .Chunk = chunk,
                            .Block = (*blocks)[i],
                            .BlockIndex = blockIndex,
                            .Peers = blockCounter.Peers,
                        });
                        DistributedBytes_.Increment((*blocks)[i].Size() * blockCounter.Peers.size());
                    }
                }

                blockPeers = blockCounter.Peers;
                distributedAt = blockCounter.DistributedAt;
            }

            if (blockPeers.empty()) {
                YT_LOG_DEBUG("Failed to allocate block peers (ChunkId: %v, BlockIndex: %v)", chunk->GetKey(), blockIndex);
                continue;
            }

            ThrottledBytes_.Increment((*blocks)[i].Size());
            (*blocks)[i] = {};
        }

        if (!blockPeers.empty()) {
            suggestions.push_back(TP2PSuggestion{
                .BlockIndex = blockIndex,
                .Peers = blockPeers,
                .P2PSessionId = SessionId_,
                .P2PIteration = distributedAt,
            });
        }
    }

    if (readFromP2P) {
        for (const auto& block : *blocks) {
            HitBytes_.Increment(block.Size());
        }
    }

    return suggestions;
}

std::vector<TP2PSuggestion> TP2PSnooper::OnBlockProbe(TChunkId chunkId, const std::vector<int>& blockIndices)
{
    if (!GetConfig()->Enabled) {
        return {};
    }

    auto chunk = Find(chunkId);
    if (!chunk || !chunk->Hot) {
        return {};
    }

    std::vector<TP2PSuggestion> suggestions;
    auto guard = ReaderGuard(chunk->BlocksLock);
    auto peerLock = Guard(chunk->PeersLock);

    for (int blockIndex : blockIndices) {
        if (blockIndex >= std::ssize(chunk->Blocks)) {
            continue;
        }

        const auto& blockCounter = chunk->Blocks[blockIndex];
        if (blockCounter.DistributedAt != 0 && !blockCounter.Peers.empty()) {
            suggestions.push_back(TP2PSuggestion{
                .BlockIndex = blockIndex,
                .Peers = blockCounter.Peers,
                .P2PSessionId = SessionId_,
                .P2PIteration = blockCounter.DistributedAt,
            });
        }
    }

    return suggestions;
}

void TP2PSnooper::SetEligiblePeers(const std::vector<TNodeId>& peers)
{
    auto guard = Guard(NodesLock_);
    EligibleNodes_ = peers;
}

void TP2PSnooper::FinishTick(
    i64 *currentTick,
    std::vector<TQueuedBlock>* hotBlocks)
{
    auto config = GetConfig();
    auto guard = Guard(QueueLock_);

    *currentTick = CurrentTick_++;
    *hotBlocks = std::move(BlockQueue_);

    if (*currentTick % config->BlockCounterResetTicks == 0) {
        CounterIteration_++;
    }
}

TGuid TP2PSnooper::GetSessionId() const
{
    return SessionId_;
}

std::vector<TP2PChunkPtr> TP2PSnooper::GetHotChunks() const
{
    auto guard = Guard(ChunkLock_);
    return {HotChunks_.begin(), HotChunks_.end()};
}

void TP2PSnooper::CoolChunk(const TP2PChunkPtr& chunk)
{
    {
        auto guard = WriterGuard(chunk->BlocksLock);
        chunk->Blocks.clear();
    }

    {
        auto guard = Guard(chunk->PeersLock);
        chunk->Peers = {};
    }

    auto guard = Guard(ChunkLock_);
    HotChunks_.erase(chunk);
    chunk->Hot = false;
}

TPeerList TP2PSnooper::AllocatePeers()
{
    auto config = GetConfig();

    TPeerList peerList;

    {
        auto guard = Guard(NodesLock_);
        if (EligibleNodes_.empty()) {
            return {};
        }

        for (int i = 0; i < config->HotBlockReplicaCount; i++) {
            peerList.push_back(EligibleNodes_[RandomNumber(EligibleNodes_.size())]);
        }
    }

    std::sort(peerList.begin(), peerList.end());
    peerList.erase(std::unique(peerList.begin(), peerList.end()), peerList.end());
    return peerList;
}

////////////////////////////////////////////////////////////////////////////////

TP2PDistributor::TP2PDistributor(
    TP2PConfigPtr config,
    IInvokerPtr invoker,
    IBootstrap* bootstrap)
    : Config_(std::move(config))
    , Invoker_(invoker)
    , Bootstrap_(bootstrap)
    , Snooper_(Bootstrap_->GetP2PSnooper())
    , DistributorExecutor_(New<TPeriodicExecutor>(
        Invoker_,
        BIND(&TP2PDistributor::DistributeBlocks, MakeWeak(this)),
        Config_->TickPeriod))
    , AllocatorExecutor_(New<TPeriodicExecutor>(
        Invoker_,
        BIND(&TP2PDistributor::AllocateNodes, MakeWeak(this)),
        Config_->NodeRefreshPeriod))
    , CoolerExecutor_(New<TPeriodicExecutor>(
        Invoker_,
        BIND(&TP2PDistributor::CoolHotChunks, MakeWeak(this)),
        Config_->ChunkCooldownTimeout))
    , DistributionErrorsCounter_(P2PProfiler.Counter("/distribution_errors"))
{ }

void TP2PDistributor::Start()
{
    DistributorExecutor_->Start();
    AllocatorExecutor_->Start();
    CoolerExecutor_->Start();
}

void TP2PDistributor::UpdateConfig(const TP2PConfigPtr& config)
{
    DynamicConfig_.Store(config);

    auto realConfig = GetConfig();
    DistributorExecutor_->SetPeriod(realConfig->TickPeriod);
    AllocatorExecutor_->SetPeriod(realConfig->NodeRefreshPeriod);
    CoolerExecutor_->SetPeriod(realConfig->ChunkCooldownTimeout);
}

TP2PConfigPtr TP2PDistributor::GetConfig()
{
    auto dynamicConfig = DynamicConfig_.Acquire();
    return dynamicConfig ? dynamicConfig : Config_;
}

void TP2PDistributor::DistributeBlocks()
{
    auto config = GetConfig();

    i64 currentTick;
    std::vector<TP2PSnooper::TQueuedBlock> blocks;

    Snooper_->FinishTick(&currentTick, &blocks);
    if (blocks.empty()) {
        return;
    }
    if (!config->Enabled) {
        return;
    }

    YT_LOG_DEBUG("Started block distribution (Iteration: %v, HotBlockCount: %v)",
        currentTick,
        blocks.size());

    THashMap<TNodeId, THashMap<TChunkId, std::vector<TP2PSnooper::TQueuedBlock>>> peerHotBlocks;
    for (const auto& block : blocks) {
        auto guard = Guard(block.Chunk->PeersLock);
        for (const auto& peerId : block.Peers) {
            peerHotBlocks[peerId][block.Chunk->GetKey()].push_back(block);
        }
    }

    std::vector<TFuture<void>> futures;
    for (const auto& [peerId, hotBlocks] : peerHotBlocks) {
        auto nodeDescriptor = Bootstrap_->GetNodeDirectory()->FindDescriptor(peerId);
        if (!nodeDescriptor) {
            continue;
        }

        auto destinationAddress = nodeDescriptor->GetAddressOrThrow(Bootstrap_->GetLocalNetworks());
        auto future = BIND([
            hotBlocks = std::move(hotBlocks),
            destinationAddress,
            currentTick,
            config,
            this,
            this_ = MakeStrong(this)
        ] () mutable {
            const auto& channelFactory = Bootstrap_
                ->GetClient()
                ->GetNativeConnection()
                ->GetChannelFactory();

            auto selfNodeId = Bootstrap_->GetNodeId();

            auto channel = channelFactory->CreateChannel(destinationAddress);

            TDataNodeServiceProxy proxy(std::move(channel));
            proxy.SetDefaultTimeout(config->RequestTimeout);

            auto req = proxy.UpdateP2PBlocks();
            req->SetMultiplexingBand(EMultiplexingBand::Heavy);

            req->set_source_node_id(selfNodeId);
            req->set_iteration(currentTick);
            ToProto(req->mutable_session_id(), Snooper_->GetSessionId());

            std::vector<TBlock> rawBlocks;
            for (const auto& [chunkId, blocks] : hotBlocks) {
                ToProto(req->add_chunk_ids(), chunkId);
                req->add_chunk_block_count(blocks.size());

                std::vector<int> blockIndexes;
                i64 blockSize = 0;
                for (const auto& block : blocks) {
                    blockSize += block.Block.Size();

                    rawBlocks.push_back(block.Block);
                    blockIndexes.push_back(block.BlockIndex);
                    req->add_block_indexes(block.BlockIndex);
                }

                YT_LOG_DEBUG("Sending blocks (BlockIds: %v:%v, Size: %v, Destination: %v)",
                    chunkId,
                    MakeShrunkFormattableView(blockIndexes, TDefaultFormatter(), 3),
                    blockSize,
                    destinationAddress);
            }

            SetRpcAttachedBlocks(req, rawBlocks);
            return req->Invoke().AsVoid();
        })
            .AsyncVia(Invoker_)
            .Run();

        future.Subscribe(BIND([this, this_ = MakeStrong(this)] (const TError& error) {
            if (!error.IsOK()) {
                DistributionErrorsCounter_.Increment();
                YT_LOG_DEBUG(error, "Failed to distribute blocks");
            }
        }));

        futures.push_back(future);
    }

    Y_UNUSED(WaitFor(AllSet(std::move(futures))));

    YT_LOG_DEBUG("Finished block distribution (Iteration: %v)", currentTick);
}

void TP2PDistributor::AllocateNodes()
{
    auto config = GetConfig();
    std::vector<TNodeId> eligiblePeerIds;

    auto now = TInstant::Now();

    auto nodes = Bootstrap_->GetNodeDirectory()->GetAllDescriptors();
    for (const auto& [nodeId, nodeDescriptor] : nodes) {
        if (nodeId == Bootstrap_->GetNodeId()) {
            continue;
        }

        if (!config->NodeTagFilter.IsSatisfiedBy(nodeDescriptor.GetTags())) {
            continue;
        }

        if (auto lastSeenTime = nodeDescriptor.GetLastSeenTime(); lastSeenTime && (*lastSeenTime + config->NodeStalenessTimeout <= now)) {
            continue;
        }

        eligiblePeerIds.push_back(nodeId);
    }

    YT_LOG_DEBUG("Updated peer list (Size: %v)", eligiblePeerIds.size());
    Snooper_->SetEligiblePeers(eligiblePeerIds);
}

void TP2PDistributor::CoolHotChunks()
{
    auto config = GetConfig();
    auto cutoff = TInstant::Now() - config->ChunkCooldownTimeout;
    for (const auto& chunk : Snooper_->GetHotChunks()) {
        if (CpuInstantToInstant(chunk->LastAccessTime) < cutoff) {
            Snooper_->CoolChunk(chunk);

            YT_LOG_DEBUG("Chunk is cool (ChunkId: %v)", chunk->GetKey());
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
