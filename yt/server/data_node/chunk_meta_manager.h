#pragma once

#include "public.h"

#include <yt/core/actions/future.h>

#include <yt/core/misc/nullable.h>
#include <yt/core/misc/async_cache.h>
#include <yt/core/misc/property.h>

#include <yt/server/cell_node/public.h>

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

//! Represents a cached chunk meta.
class TCachedChunkMeta
    : public TAsyncCacheValueBase<TChunkId, TCachedChunkMeta>
{
public:
    DEFINE_BYVAL_RO_PROPERTY(TRefCountedChunkMetaPtr, Meta);

public:
    TCachedChunkMeta(
        const TChunkId& chunkId,
        TRefCountedChunkMetaPtr meta,
        NCellNode::TNodeMemoryTracker* memoryTracker);

    i64 GetSize() const;

private:
    // NB: Avoid including TMemoryUsageTracker here.
    std::unique_ptr<NCellNode::TNodeMemoryTrackerGuard> MemoryTrackerGuard_;

};

DEFINE_REFCOUNTED_TYPE(TCachedChunkMeta)

using TCachedChunkMetaCookie = TAsyncSlruCacheBase<TChunkId, TCachedChunkMeta>::TInsertCookie;

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
        NCellNode::TBootstrap* bootstrap);

    ~TChunkMetaManager();

    //! Puts chunk meta into the cache.
    /*!
     *  Typically invoked when chunk session finishes and the meta is ready at hand.
     */
    void PutCachedMeta(
        const TChunkId& chunkId,
        TRefCountedChunkMetaPtr meta);

    //! Starts an asynchronous chunk meta load.
    /*!
     *  See TAsyncCacheValueBase for more details.
     */
    TCachedChunkMetaCookie BeginInsertCachedMeta(const TChunkId& chunkId);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TChunkMetaManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
