#include "chunk_scanner.h"

#include "chunk.h"
#include "private.h"

#include <yt/yt/server/master/object_server/object_manager.h>

namespace NYT::NChunkServer {

using namespace NLogging;
using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

TGlobalChunkScanner::TGlobalChunkScanner(
    bool journal,
    TLogger logger)
    : Journal_(journal)
    , Logger(logger)
{ }

TGlobalChunkScanner::TGlobalChunkScanner(bool journal)
    : TGlobalChunkScanner(
        journal,
        ChunkServerLogger.WithTag("Journal: %v", journal))
{ }

void TGlobalChunkScanner::Start(TGlobalChunkScanDescriptor descriptor)
{
    auto& globalScanShard = GlobalChunkScanShards_[descriptor.ShardIndex];
    YT_VERIFY(!globalScanShard.Iterator);
    YT_VERIFY(globalScanShard.ChunkCount == 0);

    YT_VERIFY(!ActiveShardIndices_[descriptor.ShardIndex]);
    ActiveShardIndices_.set(descriptor.ShardIndex);

    ScheduleGlobalScan(descriptor);

    YT_LOG_INFO("Chunk scanner started for shard (ShardIndex: %v)",
        descriptor.ShardIndex);
}

void TGlobalChunkScanner::ScheduleGlobalScan(TGlobalChunkScanDescriptor descriptor)
{
    if (!ActiveShardIndices_.test(descriptor.ShardIndex)) {
        return;
    }

    auto& globalScanShard = GlobalChunkScanShards_[descriptor.ShardIndex];
    globalScanShard.Iterator = descriptor.FrontChunk;
    globalScanShard.ChunkCount = descriptor.ChunkCount;

    if (auto* chunk = globalScanShard.Iterator) {
        YT_VERIFY(chunk->IsJournal() == Journal_);
        ActiveGlobalChunkScanIndex_ = descriptor.ShardIndex;

        YT_LOG_INFO("Global chunk scan started (ShardIndex: %v, ChunkCount: %v)",
            descriptor.ShardIndex,
            globalScanShard.ChunkCount);
    }
}

void TGlobalChunkScanner::Stop(int shardIndex)
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
}

void TGlobalChunkScanner::OnChunkDestroyed(TChunk* chunk)
{
    auto shardIndex = chunk->GetShardIndex();
    auto& globalScanShard = GlobalChunkScanShards_[shardIndex];
    if (chunk == globalScanShard.Iterator) {
        AdvanceGlobalIterator(shardIndex);
    }
}

TChunk* TGlobalChunkScanner::DequeueChunk()
{
    if (ActiveGlobalChunkScanIndex_ != -1) {
        auto& globalScanShard = GlobalChunkScanShards_[ActiveGlobalChunkScanIndex_];
        auto* chunk = globalScanShard.Iterator;
        YT_VERIFY(chunk);
        AdvanceGlobalIterator(ActiveGlobalChunkScanIndex_);
        return IsObjectAlive(chunk) ? chunk : nullptr;
    }

    return nullptr;
}

bool TGlobalChunkScanner::HasUnscannedChunk() const
{
    return ActiveGlobalChunkScanIndex_ != -1;
}

int TGlobalChunkScanner::GetQueueSize() const
{
    return std::transform_reduce(
        GlobalChunkScanShards_.begin(),
        GlobalChunkScanShards_.end(),
        0,
        std::plus<int>{},
        [] (TGlobalChunkScanShard shard) {
            return shard.ChunkCount;
        });
}

void TGlobalChunkScanner::AdvanceGlobalIterator(int shardIndex)
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

void TGlobalChunkScanner::RecomputeActiveGlobalChunkScanIndex()
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

namespace NDetail {

TChunkScannerBase::TChunkScannerBase(
    EChunkScanKind kind,
    bool journal)
    : TGlobalChunkScanner(
        journal,
        ChunkServerLogger.WithTag("Kind: %v, Journal: %v",
            kind,
            journal)),
    Kind_(kind)
{
    YT_VERIFY(kind != EChunkScanKind::GlobalStatisticsCollector);
}

bool TChunkScannerBase::IsObjectAlive(TChunk* chunk)
{
    return NObjectServer::IsObjectAlive(chunk);
}

int TChunkScannerBase::GetShardIndex(TChunk* chunk)
{
    return chunk->GetShardIndex();
}

bool TChunkScannerBase::GetScanFlag(TChunk* chunk) const
{
    return chunk->GetScanFlag(Kind_);
}

void TChunkScannerBase::ClearScanFlag(TChunk* chunk)
{
    chunk->ClearScanFlag(Kind_);
}

void TChunkScannerBase::SetScanFlag(TChunk* chunk)
{
    chunk->SetScanFlag(Kind_);
}

bool TChunkScannerBase::IsRelevant(TChunk* chunk) const
{
    return ActiveShardIndices_.test(chunk->GetShardIndex());
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
