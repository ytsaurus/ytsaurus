#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/ytlib/chunk_client/public.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

//! Manages cached blob chunk readers.
/*!
 *  \note
 *  Thread affinity: any
 */
struct IBlobReaderCache
    : public virtual TRefCounted
{
    //! Returns a (cached) blob chunk reader.
    /*!
     *  The reader becomes open on first use.
     */
    virtual NChunkClient::TFileReaderPtr GetReader(const TBlobChunkBasePtr& chunk) = 0;

    //! Evicts the reader from the cache thus hopefully closing the files.
    /*!
     *  NB: Do not make #chunk a smartpointer since #EvictReader is called from TCachedBlobChunk dtor.
     */
    virtual void EvictReader(TBlobChunkBase* chunk) = 0;
};

DEFINE_REFCOUNTED_TYPE(IBlobReaderCache)

IBlobReaderCachePtr CreateBlobReaderCache(NClusterNode::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode

