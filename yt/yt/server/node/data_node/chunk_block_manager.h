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

//! Represents a cached block of chunk.
class TCachedBlock
    : public TAsyncCacheValueBase<TBlockId, TCachedBlock>
{
public:
    DEFINE_BYVAL_RO_PROPERTY(NChunkClient::TBlock, Data);
    DEFINE_BYREF_RO_PROPERTY(std::optional<NNodeTrackerClient::TNodeDescriptor>, Source);

public:
    //! Constructs a new block from id and data.
    TCachedBlock(
        const TBlockId& blockId,
        const NChunkClient::TBlock& data,
        const std::optional<NNodeTrackerClient::TNodeDescriptor>& source);
};

DEFINE_REFCOUNTED_TYPE(TCachedBlock)

using TCachedBlockCookie = TAsyncSlruCacheBase<TBlockId, TCachedBlock>::TInsertCookie;

////////////////////////////////////////////////////////////////////////////////

//! Manages chunk blocks stored at Data Node.
/*!
 *  \note
 *  Thread affinity: any
 */
struct IChunkBlockManager
    : public virtual TRefCounted
{
    //! Synchronously looks up a compressed block in the block cache.
    //! Returns |nullptr| if block was not found.
    virtual TCachedBlockPtr FindCachedBlock(const TBlockId& blockId) = 0;

    //! Puts a compressed block into the store's cache.
    /*!
     *  The store may already have another copy of the same block.
     *  In this case the block content is checked for identity.
     */
    virtual void PutCachedBlock(
        const TBlockId& blockId,
        const NChunkClient::TBlock& data,
        const std::optional<NNodeTrackerClient::TNodeDescriptor>& source) = 0;

    //! Starts an asynchronous block load.
    /*!
     *  See TAsyncCacheValueBase for more details.
     */
    virtual TCachedBlockCookie BeginInsertCachedBlock(const TBlockId& blockId) = 0;

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
        const TBlockReadOptions& options) = 0;

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
        const TBlockReadOptions& options) = 0;

    //! Gets a vector of all blocks with non-null source stored in the cache. Thread-safe.
    virtual std::vector<TCachedBlockPtr> GetAllBlocksWithSource() = 0;
};

DEFINE_REFCOUNTED_TYPE(IChunkBlockManager)

////////////////////////////////////////////////////////////////////////////////

IChunkBlockManagerPtr CreateChunkBlockManager(NClusterNode::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode

