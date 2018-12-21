#pragma once

#include "public.h"

#include <yt/server/cell_node/public.h>

#include <yt/ytlib/chunk_client/public.h>

#include <yt/core/misc/error.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

//! Manages cached blob chunk readers.
class TBlobReaderCache
    : public TRefCounted
{
public:
    TBlobReaderCache(
        TDataNodeConfigPtr config,
        NCellNode::TBootstrap* bootstrap);
    ~TBlobReaderCache();

    //! Returns a (cached) blob chunk reader.
    /*!
     *  This call is thread-safe but may block since it actually opens the file.
     *  A rule of thumb is to invoke it from IO thread only.
     *
     *  The returned reader is already open.
     *  
     *  This method throws on failure.
     */
    NChunkClient::TFileReaderPtr GetReader(const TBlobChunkBasePtr& chunk);

    //! Evicts the reader from the cache thus hopefully closing the files.
    /*!
     *  NB: Do not make #chunk a smartpointer since #EvictReader is called from TCachedBlobChunk dtor.
     */
    void EvictReader(TBlobChunkBase* chunk);

private:
    class TCachedReader;
    using TCachedReaderPtr = TIntrusivePtr<TCachedReader>;

    class TImpl;
    using TImplPtr = TIntrusivePtr<TImpl>;

    const TImplPtr Impl_;

};

DEFINE_REFCOUNTED_TYPE(TBlobReaderCache)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode

