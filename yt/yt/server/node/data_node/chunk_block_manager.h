#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/ytlib/chunk_client/block.h>
#include <yt/yt/ytlib/chunk_client/block_id.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/core/misc/async_slru_cache.h>
#include <yt/yt/core/misc/ref.h>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

//! Manages chunk blocks stored at Data Node.
/*!
 *  \note
 *  Thread affinity: any
 */
struct IChunkBlockManager
    : public virtual TRefCounted
{
    //! Asynchronously reads a range of blocks from the store.
    /*!
     *  If some unrecoverable IO error happens during retrieval then the latter error is returned.
     *
     *  The resulting list may contain less blocks than requested.
     *  All returned blocks, however, are not null.
     *  The empty list indicates that the requested blocks are all out of range.
     *
     *  Note that blob chunks will indicate an error if an attempt is made to read a non-existing block.
     *  Journal chunks, however, will silently ignore it.
     */
    virtual TFuture<std::vector<NChunkClient::TBlock>> ReadBlockRange(
        TChunkId chunkId,
        int firstBlockIndex,
        int blockCount,
        const TChunkReadOptions& options) = 0;

    //! Asynchronously reads a set of blocks from the store.
    /*!
     *  If some unrecoverable IO error happens during retrieval then the latter error is returned.
     *
     *  The resulting list may contain less blocks than requested.
     *  If the whole chunk or some of its blocks does not exist then null block may be returned.
     */
    virtual TFuture<std::vector<NChunkClient::TBlock>> ReadBlockSet(
        TChunkId chunkId,
        const std::vector<int>& blockIndexes,
        const TChunkReadOptions& options) = 0;
};

DEFINE_REFCOUNTED_TYPE(IChunkBlockManager)

////////////////////////////////////////////////////////////////////////////////

IChunkBlockManagerPtr CreateChunkBlockManager(NClusterNode::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode

