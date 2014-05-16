#pragma once

#include "public.h"

#include <core/misc/property.h>
#include <core/misc/error.h>

#include <core/actions/signal.h>

#include <server/cell_node/public.h>

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

//! Manages cached chunks.
/*!
 *  \note
 *  Thread affinity: any.
 *
 *  Since methods may be called from arbitrary threads some of their results
 *  may only be regarded as a transient snapshot
 *  (applies to #GetChunks, #FindChunk, #GetChunkCount);
 */
class TChunkCache
    : public TRefCounted
{
public:
    TChunkCache(
        TDataNodeConfigPtr config,
        NCellNode::TBootstrap* bootstrap);

    void Initialize();

    ~TChunkCache();

    //! Finds chunk by id. Returns NULL if no chunk exists.
    IChunkPtr FindChunk(const TChunkId& chunkId);

    //! Returns the list of all registered chunks.
    std::vector<IChunkPtr> GetChunks();

    //! Returns the number of registered chunks.
    int GetChunkCount();

    bool IsEnabled() const;

    typedef TErrorOr<IChunkPtr> TDownloadResult;
    typedef TFuture<TDownloadResult> TAsyncDownloadResult;

    //! Downloads a chunk into the cache.
    /*!
     *  The download process is asynchronous.
     *  If the chunk is already cached, it returns a pre-set result.
     */
    TAsyncDownloadResult DownloadChunk(const TChunkId& chunkId);

    //! Raised when a chunk is added to the cache.
    DECLARE_SIGNAL(void(IChunkPtr), ChunkAdded);

    //! Raised when a chunk is removed from the cache.
    DECLARE_SIGNAL(void(IChunkPtr), ChunkRemoved);

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TChunkCache)

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT

