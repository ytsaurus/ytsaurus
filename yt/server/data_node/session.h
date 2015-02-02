#pragma once

#include "public.h"

#include <core/misc/error.h>
#include <core/misc/lease_manager.h>
#include <core/misc/nullable.h>

#include <core/actions/signal.h>

#include <ytlib/node_tracker_client/node_directory.h>

#include <ytlib/chunk_client/chunk_meta.pb.h>

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

struct TSessionOptions
{
    EWriteSessionType SessionType;
    bool SyncOnClose = false;
    bool OptimizeForLatency = false;
};

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
    virtual NChunkClient::NProto::TChunkInfo GetChunkInfo() const = 0;

    //! Starts the session.
    /*!
     *  Returns the flag indicating that the session is persistenly started.
     *  For blob chunks this happens immediately (and the actualy opening happens in backgound).
     *  For journal chunks this happens when append record is flushed into the multiplexed changelog.
     */
    virtual TFuture<void> Start() = 0;

    //! Cancels the session.
    virtual void Cancel(const TError& error) = 0;

    //! Finishes the session.
    virtual TFuture<IChunkPtr> Finish(
        const NChunkClient::NProto::TChunkMeta& chunkMeta,
        const TNullable<int>& blockCount) = 0;

    //! Puts a contiguous range of blocks into the window.
    virtual TFuture<void> PutBlocks(
        int startBlockIndex,
        const std::vector<TSharedRef>& blocks,
        bool enableCaching) = 0;

    //! Sends a range of blocks (from the current window) to another data node.
    virtual TFuture<void> SendBlocks(
        int startBlockIndex,
        int blockCount,
        const NNodeTrackerClient::TNodeDescriptor& target) = 0;

    //! Flushes blocks up to a given one.
    virtual TFuture<void> FlushBlocks(int blockIndex) = 0;

    //! Renews the lease.
    virtual void Ping() = 0;


    DECLARE_INTERFACE_SIGNAL(void(const TError& error), Finished);

};

DEFINE_REFCOUNTED_TYPE(ISession)

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT

