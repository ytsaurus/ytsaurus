#include "chunk_scanner.h"

#include "chunk.h"
#include "private.h"

#include <yt/yt/server/master/object_server/object_manager.h>

namespace NYT::NChunkServer {

using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

TChunkScanner::TChunkScanner(
    IObjectManagerPtr objectManager,
    EChunkScanKind kind,
    bool journal)
    : ObjectManager_(std::move(objectManager))
    , Kind_(kind)
    , Journal_(journal)
    , Logger(ChunkServerLogger.WithTag("Kind: %v, Journal: %v",
        Kind_,
        Journal_))
{ }

void TChunkScanner::Start(int shardIndex, TGlobalChunkScanDescriptor descriptor)
{
    auto& globalScanShard = GlobalChunkScanShards_[shardIndex];
    YT_VERIFY(!globalScanShard.Iterator);
    YT_VERIFY(globalScanShard.ChunkCount == 0);

    YT_VERIFY(!ActiveShardIndices_[shardIndex]);
    ActiveShardIndices_.set(shardIndex);

    ScheduleGlobalScan(shardIndex, descriptor);

    YT_LOG_INFO("Chunk scanner started for shard (ShardIndex: %v)",
        shardIndex);
}

void TChunkScanner::ScheduleGlobalScan(int shardIndex, TGlobalChunkScanDescriptor descriptor)
{
    if (!ActiveShardIndices_.test(shardIndex)) {
        return;
    }

    auto& globalScanShard = GlobalChunkScanShards_[shardIndex];
    globalScanShard.Iterator = descriptor.FrontChunk;
    globalScanShard.ChunkCount = descriptor.ChunkCount;

    if (auto* chunk = globalScanShard.Iterator) {
        YT_VERIFY(chunk->IsJournal() == Journal_);
        ActiveGlobalChunkScanIndex_ = shardIndex;

        YT_LOG_INFO("Global chunk scan started (ShardIndex: %v, ChunkCount: %v)",
            shardIndex,
            globalScanShard.ChunkCount);
    }
}

void TChunkScanner::Stop(int shardIndex)
{
    if (!ActiveShardIndices_.test(shardIndex)) {
        return;
    }
    ActiveShardIndices_.reset(shardIndex);

    YT_LOG_DEBUG("Chunk scanner stopped for shard (ShardIndex: %v)",
        shardIndex);

    // Clear global chunk scan state.
    GlobalChunkScanShards_[shardIndex] = {};

    if (ActiveGlobalChunkScanIndex_ == shardIndex) {
        RecomputeActiveGlobalChunkScanIndex();
    }

    // If there are no more active shards, we clear the queue to drop all the
    // ephemeral references. Since queue may be huge, destruction is offloaded into
    // separate thread.
    // Otherwise, queue is not changed. Note that queue now may contain chunks from
    // non-active shards. We have to properly handle them during chunk dequeue.
    if (ActiveShardIndices_.none()) {
        std::queue<TQueueEntry> queue;
        std::swap(Queue_, queue);
        NRpc::TDispatcher::Get()->GetHeavyInvoker()->Invoke(
            BIND([queue = std::move(queue)] { Y_UNUSED(queue); }));
    }
}

void TChunkScanner::OnChunkDestroyed(TChunk* chunk)
{
    auto shardIndex = chunk->GetShardIndex();
    auto& globalScanShard = GlobalChunkScanShards_[shardIndex];
    if (chunk == globalScanShard.Iterator) {
        AdvanceGlobalIterator(shardIndex);
    }
}

bool TChunkScanner::EnqueueChunk(TChunk* chunk)
{
    auto shardIndex = chunk->GetShardIndex();
    if (!ActiveShardIndices_.test(shardIndex)) {
        return false;
    }

    if (chunk->GetScanFlag(Kind_)) {
        return false;
    }
    chunk->SetScanFlag(Kind_);
    Queue_.push({
        TEphemeralObjectPtr<TChunk>(chunk),
        NProfiling::GetCpuInstant()
    });
    return true;
}

TChunk* TChunkScanner::DequeueChunk()
{
    if (ActiveGlobalChunkScanIndex_ != -1) {
        auto& globalScanShard = GlobalChunkScanShards_[ActiveGlobalChunkScanIndex_];
        auto* chunk = globalScanShard.Iterator;
        YT_VERIFY(chunk);
        AdvanceGlobalIterator(ActiveGlobalChunkScanIndex_);
        return IsObjectAlive(chunk) ? chunk : nullptr;
    }

    if (Queue_.empty()) {
        return nullptr;
    }

    auto* chunk = Queue_.front().Chunk.Get();
    bool alive = IsObjectAlive(chunk);
    if (alive) {
        YT_ASSERT(chunk->GetScanFlag(Kind_));
        chunk->ClearScanFlag(Kind_);
    }
    Queue_.pop();
    if (alive && ActiveShardIndices_.test(chunk->GetShardIndex())) {
        return chunk;
    }

    return nullptr;
}

bool TChunkScanner::HasUnscannedChunk(NProfiling::TCpuInstant deadline) const
{
    if (ActiveGlobalChunkScanIndex_ != -1) {
        return true;
    }

    if (!Queue_.empty() && Queue_.front().Instant < deadline) {
        return true;
    }

    return false;
}

int TChunkScanner::GetQueueSize() const
{
    int queueSize = std::ssize(Queue_);
    for (int shardIndex = 0; shardIndex < ChunkShardCount; ++shardIndex) {
        queueSize += GlobalChunkScanShards_[shardIndex].ChunkCount;
    }

    return queueSize;
}

void TChunkScanner::AdvanceGlobalIterator(int shardIndex)
{
    auto& globalScanShard = GlobalChunkScanShards_[shardIndex];
    auto& chunkCount = globalScanShard.ChunkCount;
    auto& iterator = globalScanShard.Iterator;

    YT_VERIFY(chunkCount > 0);
    --chunkCount;

    iterator = iterator->GetNextScannedChunk();
    if (!iterator) {
        // NB: Some chunks could vanish during the scan so this is not
        // necessarily zero.
        YT_VERIFY(chunkCount >= 0);
        YT_LOG_INFO("Global chunk scan finished (ShardIndex: %v, VanishedChunkCount: %v)",
            shardIndex,
            chunkCount);
        chunkCount = 0;

        if (ActiveGlobalChunkScanIndex_ == shardIndex) {
            RecomputeActiveGlobalChunkScanIndex();
        }
    }
}

void TChunkScanner::RecomputeActiveGlobalChunkScanIndex()
{
    ActiveGlobalChunkScanIndex_ = -1;
    for (int shardIndex = 0; shardIndex < ChunkShardCount; ++shardIndex) {
        if (GlobalChunkScanShards_[shardIndex].Iterator) {
            ActiveGlobalChunkScanIndex_ = shardIndex;
            break;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
