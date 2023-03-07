#pragma once

#include "public.h"

#include <yt/server/node/cluster_node/public.h>

#include <yt/ytlib/chunk_client/public.h>

#include <yt/core/actions/future.h>

#include <yt/core/misc/optional.h>
#include <yt/core/misc/async_cache.h>
#include <yt/core/misc/property.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

//! Represents a cached chunk meta.
class TCachedChunkMeta
    : public TAsyncCacheValueBase<TChunkId, TCachedChunkMeta>
{
public:
    DEFINE_BYVAL_RO_PROPERTY(NChunkClient::TRefCountedChunkMetaPtr, Meta);

public:
    TCachedChunkMeta(
        TChunkId chunkId,
        NChunkClient::TRefCountedChunkMetaPtr meta,
        NClusterNode::TNodeMemoryTrackerPtr memoryTracker);

    i64 GetSize() const;

private:
    // NB: Avoid including TMemoryUsageTracker here.
    std::unique_ptr<NClusterNode::TNodeMemoryTrackerGuard> MemoryTrackerGuard_;

};

DEFINE_REFCOUNTED_TYPE(TCachedChunkMeta)

using TCachedChunkMetaCookie = TAsyncSlruCacheBase<TChunkId, TCachedChunkMeta>::TInsertCookie;

////////////////////////////////////////////////////////////////////////////////

//! Represents a cached blocks extension.
class TCachedBlocksExt
    : public TAsyncCacheValueBase<TChunkId, TCachedBlocksExt>
{
public:
    DEFINE_BYVAL_RO_PROPERTY(NChunkClient::TRefCountedBlocksExtPtr, BlocksExt);

public:
    TCachedBlocksExt(
        TChunkId chunkId,
        NChunkClient::TRefCountedBlocksExtPtr blocksExt,
        NClusterNode::TNodeMemoryTrackerPtr memoryTracker);

    i64 GetSize() const;

private:
    // NB: Avoid including TMemoryUsageTracker here.
    std::unique_ptr<NClusterNode::TNodeMemoryTrackerGuard> MemoryTrackerGuard_;

};

DEFINE_REFCOUNTED_TYPE(TCachedBlocksExt)

using TCachedBlocksExtCookie = TAsyncSlruCacheBase<TChunkId, TCachedBlocksExt>::TInsertCookie;

////////////////////////////////////////////////////////////////////////////////

//! Manages (in particular, caches) metas of chunks stored at Data Node.
/*!
 *  \note
 *  Thread affinity: any
 */
class TChunkMetaManager
    : public TRefCounted
{
public:
    TChunkMetaManager(
        TDataNodeConfigPtr config,
        NClusterNode::TBootstrap* bootstrap);

    ~TChunkMetaManager();

    //! Returns cached chunk meta if prevent in the cache; if not then returns null.
    NChunkClient::TRefCountedChunkMetaPtr FindCachedMeta(TChunkId chunkId);

    //! Puts chunk meta into the cache.
    void PutCachedMeta(
        TChunkId chunkId,
        NChunkClient::TRefCountedChunkMetaPtr meta);

    //! Starts an asynchronous chunk meta load.
    TCachedChunkMetaCookie BeginInsertCachedMeta(TChunkId chunkId);

    //! Completes an asynchronous chunk meta load.
    void EndInsertCachedMeta(
        TCachedChunkMetaCookie&& cookie,
        NChunkClient::TRefCountedChunkMetaPtr meta);
    
    //! Forcefully evicts cached chunk meta from the cache, if any.
    void RemoveCachedMeta(TChunkId chunkId);

    //! Looks for blocks ext in the cache.
    NChunkClient::TRefCountedBlocksExtPtr FindCachedBlocksExt(TChunkId chunkId);

    //! Puts blocks ext into the cache.
    void PutCachedBlocksExt(
        TChunkId chunkId,
        NChunkClient::TRefCountedBlocksExtPtr blocksExt);

    //! Starts an asynchronous blocks ext load.
    TCachedBlocksExtCookie BeginInsertCachedBlocksExt(TChunkId chunkId);

    //! Completes an asynchronous blocks ext load.
    void EndInsertCachedBlocksExt(
        TCachedBlocksExtCookie&& cookie,
        NChunkClient::TRefCountedBlocksExtPtr blocksExt);

    //! Forcefully evicts cached blocks ext from the cache, if any.
    void RemoveCachedBlocksExt(TChunkId chunkId);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TChunkMetaManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
