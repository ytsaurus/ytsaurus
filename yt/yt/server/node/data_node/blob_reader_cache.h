#pragma once

#include "public.h"

#include <yt/server/node/cluster_node/public.h>

#include <yt/ytlib/chunk_client/public.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

//! Manages cached blob chunk readers.
struct IBlobReaderCache
    : public virtual TRefCounted
{
    //! Returns a (cached) blob chunk reader.
    /*!
     *  This call is thread-safe but may block since it actually opens the file.
     *  A rule of thumb is to invoke it from IO thread only.
     *
     *  The returned reader is already open.
     *
     *  This method throws on failure.
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

