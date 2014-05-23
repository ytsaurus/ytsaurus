#pragma once

#include "public.h"

#include <core/misc/error.h>
#include <core/misc/lease_manager.h>

#include <core/actions/signal.h>

#include <ytlib/node_tracker_client/node_directory.h>

#include <ytlib/chunk_client/chunk_meta.pb.h>

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

struct ISession
    : public virtual TRefCounted
{
    //! Returns the TChunkId being uploaded.
    virtual const TChunkId& GetChunkId() const = 0;

    //! Returns session type provided by the client during handshake.
    virtual EWriteSessionType GetType() const = 0;

    //! Returns target chunk location.
    virtual TLocationPtr GetLocation() const = 0;

    //! Returns the chunk info.
    virtual const NChunkClient::NProto::TChunkInfo& GetChunkInfo() const = 0;

    //! Initializes the session instance.
    virtual void Start(TLeaseManager::TLease lease) = 0;

    //! Cancels the session.
    virtual void Cancel(const TError& error) = 0;

    //! Finishes the session.
    virtual TFuture<TErrorOr<IChunkPtr>> Finish(
        const NChunkClient::NProto::TChunkMeta& chunkMeta) = 0;

    //! Puts a contiguous range of blocks into the window.
    virtual TAsyncError PutBlocks(
        int startBlockIndex,
        const std::vector<TSharedRef>& blocks,
        bool enableCaching) = 0;

    //! Sends a range of blocks (from the current window) to another data node.
    virtual TAsyncError SendBlocks(
        int startBlockIndex,
        int blockCount,
        const NNodeTrackerClient::TNodeDescriptor& target) = 0;

    //! Flushes a block and moves the window
    /*!
     * The operation is asynchronous. It returns a result that gets set
     * when the actual flush happens. Once a block is flushed, the next block becomes
     * the first one in the window.
     */
    virtual TAsyncError FlushBlock(int blockIndex) = 0;

    //! Renews the lease.
    virtual void Ping() = 0;


    DECLARE_INTERFACE_SIGNAL(void(const TError& error), Failed);
    DECLARE_INTERFACE_SIGNAL(void(IChunkPtr chunk), Completed);

};

DEFINE_REFCOUNTED_TYPE(ISession)

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT

