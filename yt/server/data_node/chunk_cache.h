#pragma once

#include "public.h"

#include <core/misc/error.h>

#include <core/actions/signal.h>

#include <ytlib/node_tracker_client/public.h>

#include <ytlib/chunk_client/chunk_replica.h>

#include <server/cell_node/public.h>

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

//! Manages chunks cached at Data Node.
/*!
 *  \note
 *  Thread affinity: ControlThread (unless indicated otherwise)
 */
class TChunkCache
    : public TRefCounted
{
public:
    TChunkCache(
        TDataNodeConfigPtr config,
        NCellNode::TBootstrap* bootstrap);
    ~TChunkCache();

    void Initialize();

    bool IsEnabled() const;

    //! Finds chunk by id. Returns |nullptr| if no chunk exists.
    /*!
     *  \note
     *  Thread affinity: any
     */
    IChunkPtr FindChunk(const TChunkId& chunkId);

    //! Returns the list of all registered chunks.
    /*!
     *  \note
     *  Thread affinity: any
     */
    std::vector<IChunkPtr> GetChunks();

    //! Returns the number of registered chunks.
    /*!
     *  \note
     *  Thread affinity: any
     */
    int GetChunkCount();

    //! Downloads a single- or multi-chunk artifact into the cache.
    /*!
     *  The download process is asynchronous.
     *  If the chunk is already cached, it returns a pre-set result.
     *
     *  Thread affinity: any
     */
    TFuture<IChunkPtr> PrepareArtifact(
        const TArtifactKey& key,
        NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory = nullptr);

    //! Raised when a chunk is added to the cache.
    DECLARE_SIGNAL(void(IChunkPtr), ChunkAdded);

    //! Raised when a chunk is removed from the cache.
    DECLARE_SIGNAL(void(IChunkPtr), ChunkRemoved);

private:
    class TImpl;
    const TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TChunkCache)

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT

