#pragma once

#include "public.h"

#include <yt/server/node/cluster_node/public.h>

#include <yt/ytlib/chunk_client/public.h>

#include <yt/ytlib/table_client/public.h>

#include <yt/core/actions/future.h>

#include <yt/core/misc/optional.h>
#include <yt/core/misc/async_slru_cache.h>
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
        NChunkClient::TRefCountedChunkMetaPtr meta);

    i64 GetWeight() const;

private:
    const i64 Weight_;
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
        NChunkClient::TRefCountedBlocksExtPtr blocksExt);

    i64 GetWeight() const;

private:
    const i64 Weight_;
};

DEFINE_REFCOUNTED_TYPE(TCachedBlocksExt)

using TCachedBlocksExtCookie = TAsyncSlruCacheBase<TChunkId, TCachedBlocksExt>::TInsertCookie;

////////////////////////////////////////////////////////////////////////////////

//! Manages (in particular, caches) metas of chunks stored at Data Node.
/*!
 *  \note
 *  Thread affinity: any
 */
struct IChunkMetaManager
    : public virtual TRefCounted
{
    //! Returns the block meta cache.
    virtual const NTableClient::TBlockMetaCachePtr& GetBlockMetaCache() = 0;

    //! Returns cached chunk meta if prevent in the cache; if not then returns null.
    virtual NChunkClient::TRefCountedChunkMetaPtr FindCachedMeta(TChunkId chunkId) = 0;

    //! Puts chunk meta into the cache.
    virtual void PutCachedMeta(
        TChunkId chunkId,
        NChunkClient::TRefCountedChunkMetaPtr meta) = 0;

    //! Starts an asynchronous chunk meta load.
    virtual TCachedChunkMetaCookie BeginInsertCachedMeta(TChunkId chunkId) = 0;

    //! Completes an asynchronous chunk meta load.
    virtual void EndInsertCachedMeta(
        TCachedChunkMetaCookie&& cookie,
        NChunkClient::TRefCountedChunkMetaPtr meta) = 0;

    //! Forcefully evicts cached chunk meta from the cache, if any.
    virtual void RemoveCachedMeta(TChunkId chunkId) = 0;

    //! Looks for blocks ext in the cache.
    virtual NChunkClient::TRefCountedBlocksExtPtr FindCachedBlocksExt(TChunkId chunkId) = 0;

    //! Puts blocks ext into the cache.
    virtual void PutCachedBlocksExt(
        TChunkId chunkId,
        NChunkClient::TRefCountedBlocksExtPtr blocksExt) = 0;

    //! Starts an asynchronous blocks ext load.
    virtual TCachedBlocksExtCookie BeginInsertCachedBlocksExt(TChunkId chunkId) = 0;

    //! Completes an asynchronous blocks ext load.
    virtual void EndInsertCachedBlocksExt(
        TCachedBlocksExtCookie&& cookie,
        NChunkClient::TRefCountedBlocksExtPtr blocksExt) = 0;

    //! Forcefully evicts cached blocks ext from the cache, if any.
    virtual void RemoveCachedBlocksExt(TChunkId chunkId) = 0;
};

DEFINE_REFCOUNTED_TYPE(IChunkMetaManager)

IChunkMetaManagerPtr CreateChunkMetaManager(NClusterNode::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode
