#pragma once

#include "public.h"
#include "tablet_profiling.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/chunk_client/public.h>
#include <yt/yt/ytlib/chunk_client/block_cache.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/ytlib/table_client/cached_versioned_chunk_meta.h>
#include <yt/yt/ytlib/table_client/versioned_chunk_reader.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_spec.pb.h>

#include <yt/yt/core/misc/ref.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

NChunkClient::EBlockType MapInMemoryModeToBlockType(NTabletClient::EInMemoryMode mode);

////////////////////////////////////////////////////////////////////////////////

struct TPreloadedBlockTag { };

////////////////////////////////////////////////////////////////////////////////

//! Contains all relevant data (e.g. blocks) for in-memory chunks.
struct TInMemoryChunkData final
{
    const NTabletClient::EInMemoryMode InMemoryMode;
    const int StartBlockIndex;
    const std::vector<NChunkClient::TBlock> Blocks;
    const NTableClient::TCachedVersionedChunkMetaPtr ChunkMeta;
    const NTableClient::IChunkLookupHashTablePtr LookupHashTable;
    const TMemoryUsageTrackerGuard MemoryTrackerGuard;
};

DEFINE_REFCOUNTED_TYPE(TInMemoryChunkData)

TInMemoryChunkDataPtr CreateInMemoryChunkData(
    NChunkClient::TChunkId chunkId,
    NTabletClient::EInMemoryMode mode,
    int startBlockIndex,
    std::vector<NChunkClient::TBlock> blocks,
    const NTableClient::TCachedVersionedChunkMetaPtr& versionedChunkMeta,
    const TTabletSnapshotPtr& tabletSnapshot,
    TMemoryUsageTrackerGuard memoryTrackerGuard);

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
    virtual TInMemoryChunkDataPtr EvictInterceptedChunkData(NChunkClient::TChunkId chunkId) = 0;

    virtual void FinalizeChunk(NChunkClient::TChunkId chunkId, TInMemoryChunkDataPtr chunkData) = 0;

    virtual const TInMemoryManagerConfigPtr& GetConfig() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IInMemoryManager)

IInMemoryManagerPtr CreateInMemoryManager(
    TInMemoryManagerConfigPtr config,
    IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

//! Preload specified store into memory.
TInMemoryChunkDataPtr PreloadInMemoryStore(
    const TTabletSnapshotPtr& tabletSnapshot,
    const IChunkStorePtr& store,
    NChunkClient::TReadSessionId readSessionId,
    const NClusterNode::TNodeMemoryTrackerPtr& memoryTracker,
    const IInvokerPtr& compressionInvoker,
    const TReaderProfilerPtr& readerProfiler);

////////////////////////////////////////////////////////////////////////////////

struct TChunkInfo
{
    TChunkInfo(
        NChunkClient::TChunkId chunkId,
        NChunkClient::TRefCountedChunkMetaPtr chunkMeta,
        TTabletId tabletId,
        NHydra::TRevision mountRevision)
        : ChunkId(chunkId)
        , ChunkMeta(std::move(chunkMeta))
        , TabletId(tabletId)
        , MountRevision(mountRevision)
    { }

    NChunkClient::TChunkId ChunkId;
    NChunkClient::TRefCountedChunkMetaPtr ChunkMeta;
    TTabletId TabletId;
    NHydra::TRevision MountRevision;
};

struct IRemoteInMemoryBlockCache
    : public NChunkClient::IBlockCache
{
    virtual TFuture<void> Finish(const std::vector<TChunkInfo>& chunkInfos) = 0;
};

DEFINE_REFCOUNTED_TYPE(IRemoteInMemoryBlockCache)

TFuture<IRemoteInMemoryBlockCachePtr> CreateRemoteInMemoryBlockCache(
    NApi::NNative::IClientPtr client,
    const NNodeTrackerClient::TNodeDescriptor& localDescriptor,
    NRpc::IServerPtr localRpcServer,
    const NHiveClient::TCellDescriptor& cellDescriptor,
    NTabletClient::EInMemoryMode inMemoryMode,
    TInMemoryManagerConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
