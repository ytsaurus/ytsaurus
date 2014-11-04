#pragma once

#include "public.h"

#include <core/misc/property.h>
#include <core/misc/error.h>

#include <core/actions/signal.h>

#include <ytlib/node_tracker_client/public.h>

#include <ytlib/chunk_client/chunk_replica.h>

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
    typedef std::vector<TCachedChunkPtr> TChunks;

    TChunkCache(
        TDataNodeConfigPtr config,
        NCellNode::TBootstrap* bootstrap);

    void Initialize();

    ~TChunkCache();

    //! Finds chunk by id. Returns NULL if no chunk exists.
    TCachedChunkPtr FindChunk(const TChunkId& chunkId);

    //! Returns the list of all registered chunks.
    TChunks GetChunks();

    //! Returns the number of registered chunks.
    int GetChunkCount();

    const TGuid& GetCellGuid() const;
    void UpdateCellGuid(const TGuid& cellGuid);

    bool IsEnabled() const;

    typedef TErrorOr<TCachedChunkPtr> TDownloadResult;
    typedef TFuture<TDownloadResult> TAsyncDownloadResult;

    //! Downloads a chunk into the cache.
    /*!
     *  The download process is asynchronous.
     *  If the chunk is already cached, it returns a pre-set result.
     */
    TAsyncDownloadResult DownloadChunk(
        const TChunkId& chunkId,
        NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory = nullptr,
        const NChunkClient::TChunkReplicaList& seedReplicas = NChunkClient::TChunkReplicaList());

    //! Raised when a chunk is added to the cache.
    DECLARE_SIGNAL(void(TChunkPtr), ChunkAdded);

    //! Raised when a chunk is removed from the cache.
    DECLARE_SIGNAL(void(TChunkPtr), ChunkRemoved);

private:
    class TImpl;
    TIntrusivePtr<TImpl> Impl;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT

