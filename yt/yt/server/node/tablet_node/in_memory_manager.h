#pragma once

#include "public.h"
#include "tablet_profiling.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/chunk_client/public.h>
#include <yt/yt/ytlib/chunk_client/block_cache.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>
#include <yt/yt/ytlib/misc/public.h>

#include <yt/yt/ytlib/table_client/cached_versioned_chunk_meta.h>
#include <yt/yt/ytlib/table_client/versioned_chunk_reader.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_spec.pb.h>

#include <library/cpp/yt/memory/ref.h>

namespace NYT::NTabletNode {

////////////////////////////////////////////////////////////////////////////////

NChunkClient::EBlockType GetBlockTypeFromInMemoryMode(NTabletClient::EInMemoryMode mode);

////////////////////////////////////////////////////////////////////////////////

struct TPreloadedBlockTag { };

////////////////////////////////////////////////////////////////////////////////

//! Contains all relevant data (e.g. blocks) for in-memory chunks.
struct TInMemoryChunkData final
{
    const NTabletClient::EInMemoryMode InMemoryMode;
    const int StartBlockIndex;
    const NTableClient::TCachedVersionedChunkMetaPtr ChunkMeta;
    const NTableClient::TChunkLookupHashTablePtr LookupHashTable;

    //! Guard that trackers non blocks memory usage (ChunkMeta + LookupHashTable).
    const TMemoryUsageTrackerGuard BlockMetaMemoryTrackerGurard;

    //! Blocks to serve.
    const std::vector<NChunkClient::TBlock> Blocks;

    //! Blocks with block tracker memory category holders attached.
    const std::vector<NChunkClient::TBlock> BlockCategoryHolders;
};

DEFINE_REFCOUNTED_TYPE(TInMemoryChunkData)

TInMemoryChunkDataPtr CreateInMemoryChunkData(
    NChunkClient::TChunkId chunkId,
    NTabletClient::EInMemoryMode mode,
    int startBlockIndex,
    std::vector<NChunkClient::TBlock> blocksWithCategory,
    const NTableClient::TCachedVersionedChunkMetaPtr& versionedChunkMeta,
    const TTabletSnapshotPtr& tabletSnapshot,
    const INodeMemoryReferenceTrackerPtr& memoryReferenceTracker,
    const IMemoryUsageTrackerPtr& memoryTracker);

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

    virtual TInMemoryManagerConfigPtr GetConfig() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IInMemoryManager)

IInMemoryManagerPtr CreateInMemoryManager(IBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

//! Preload specified store into memory.
TInMemoryChunkDataPtr PreloadInMemoryStore(
    const TTabletSnapshotPtr& tabletSnapshot,
    const IChunkStorePtr& store,
    NChunkClient::TReadSessionId readSessionId,
    const INodeMemoryTrackerPtr& memoryTracker,
    const IInvokerPtr& compressionInvoker,
    const TReaderProfilerPtr& readerProfiler,
    const INodeMemoryReferenceTrackerPtr& memoryReferenceTracker,
    bool enablePreliminaryNetworkThrottling,
    const NConcurrency::IThroughputThrottlerPtr& networkThrottler);

////////////////////////////////////////////////////////////////////////////////

struct TChunkInfo
{
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
    IInvokerPtr controlInvoker,
    const NNodeTrackerClient::TNodeDescriptor& localDescriptor,
    NRpc::IServerPtr localRpcServer,
    NHiveClient::TCellDescriptorPtr cellDescriptor,
    NTabletClient::EInMemoryMode inMemoryMode,
    TInMemoryManagerConfigPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTabletNode
