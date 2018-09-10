#pragma once

#include "public.h"

#include <yt/server/cell_node/public.h>

#include <yt/ytlib/api/native/public.h>

#include <yt/ytlib/chunk_client/public.h>
#include <yt/ytlib/chunk_client/block_cache.h>
#include <yt/client/chunk_client/proto/chunk_spec.pb.h>

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
struct IInMemoryManager
    : public TRefCounted
{
    virtual NChunkClient::IBlockCachePtr CreateInterceptingBlockCache(
            NTabletClient::EInMemoryMode mode) = 0;

    virtual TInMemoryChunkDataPtr EvictInterceptedChunkData(
        NChunkClient::TChunkId chunkId) = 0;

    virtual void FinalizeChunk(
        NChunkClient::TChunkId chunkId,
        const NChunkClient::NProto::TChunkMeta& chunkMeta,
        const TTabletSnapshotPtr& tablet) = 0;

    virtual const TInMemoryManagerConfigPtr& GetConfig() const = 0;

};

DEFINE_REFCOUNTED_TYPE(IInMemoryManager)

IInMemoryManagerPtr CreateInMemoryManager(
    TInMemoryManagerConfigPtr config,
    NCellNode::TBootstrap* bootstrap);

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

struct TChunkInfo
{
    TChunkInfo(
        NChunkClient::TChunkId chunkId,
        NChunkClient::NProto::TChunkMeta chunkMeta,
        TTabletId tabletId)
        : ChunkId(chunkId)
        , ChunkMeta(std::move(chunkMeta))
        , TabletId(tabletId)
    { }

    NChunkClient::TChunkId ChunkId;
    NChunkClient::NProto::TChunkMeta ChunkMeta;
    TTabletId TabletId;
};

struct IRemoteInMemoryBlockCache
    : public NChunkClient::IBlockCache
{
    virtual TFuture<void> Finish(const std::vector<TChunkInfo>& chunkInfos) = 0;
};

DEFINE_REFCOUNTED_TYPE(IRemoteInMemoryBlockCache)

TFuture<IRemoteInMemoryBlockCachePtr> CreateRemoteInMemoryBlockCache(
    NApi::NNative::IClientPtr client,
    const NHiveClient::TCellDescriptor& cellDescriptor,
    NTabletClient::EInMemoryMode inMemoryMode,
    TInMemoryManagerConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NTabletNode
} // namespace NYT
