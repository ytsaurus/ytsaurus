#pragma once

#include "public.h"

#include <yt/server/cell_node/public.h>

#include <yt/ytlib/chunk_client/public.h>
#include <yt/ytlib/chunk_client/chunk_spec.pb.h>

#include <yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/ytlib/table_client/cached_versioned_chunk_meta.h>
#include <yt/ytlib/table_client/versioned_chunk_reader.h>

#include <yt/core/misc/ref.h>

namespace NYT {
namespace NTabletNode {

////////////////////////////////////////////////////////////////////////////////

NChunkClient::EBlockType MapInMemoryModeToBlockType(NTabletClient::EInMemoryMode mode);

////////////////////////////////////////////////////////////////////////////////

//! Contains all relevant data (e.g. blocks) for in-memory chunks.
struct TInMemoryChunkData
    : public TIntrinsicRefCounted
{
    NTabletClient::EInMemoryMode InMemoryMode = NTabletClient::EInMemoryMode::None;
    ui64 InMemoryConfigRevision = 0;

    std::vector<NChunkClient::TBlock> Blocks;
    NChunkClient::NProto::TChunkSpec ChunkSpec;
    NTableClient::TCachedVersionedChunkMetaPtr ChunkMeta;
    NTableClient::IChunkLookupHashTablePtr LookupHashTable;
    NCellNode::TNodeMemoryTrackerGuard MemoryTrackerGuard;
};

DEFINE_REFCOUNTED_TYPE(TInMemoryChunkData)

////////////////////////////////////////////////////////////////////////////////

//! Manages in-memory tables served by the node.
/*!
 *  Ensures that chunk stores of in-memory tables are preloaded when a node starts.
 *
 *  Provides means for intercepting data write-out during flushes and compactions
 *  and thus enables new chunk stores to be created with all blocks already resident.
 */
class TInMemoryManager
    : public TRefCounted
{
public:
    TInMemoryManager(
        TInMemoryManagerConfigPtr config,
        NCellNode::TBootstrap* bootstrap);
    ~TInMemoryManager();

    NChunkClient::IBlockCachePtr CreateInterceptingBlockCache(NTabletClient::EInMemoryMode mode, ui64 configRevision);
    TInMemoryChunkDataPtr EvictInterceptedChunkData(const NChunkClient::TChunkId& chunkId);
    void FinalizeChunk(
        const NChunkClient::TChunkId& chunkId,
        const NChunkClient::NProto::TChunkMeta& chunkMeta,
        const TTabletSnapshotPtr& tablet);

private:
    class TImpl;
    using TImplPtr = TIntrusivePtr<TImpl>;
    const TImplPtr Impl_;
};

DEFINE_REFCOUNTED_TYPE(TInMemoryManager)

////////////////////////////////////////////////////////////////////////////////

//! Preload specified store into memory.
TInMemoryChunkDataPtr PreloadInMemoryStore(
    const TTabletSnapshotPtr& tabletSnapshot,
    const IChunkStorePtr& store,
    const NChunkClient::TReadSessionId& readSessionId,
    NNodeTrackerClient::TNodeMemoryTracker* memoryUsageTracker,
    const IInvokerPtr& compressionInvoker,
    const NConcurrency::IThroughputThrottlerPtr& throttler,
    const NProfiling::TTagId& preloadTag = {});

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
