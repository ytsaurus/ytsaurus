#pragma once

#include "public.h"

#include <yt/yt/server/lib/io/io_tracker.h>

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/ytlib/chunk_client/session_id.h>
#include <yt/yt/ytlib/chunk_client/block.h>
#include <yt/yt/ytlib/chunk_client/data_node_service_proxy.h>

#include <yt/yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>

#include <yt/yt/client/misc/workload.h>

#include <yt/yt/client/node_tracker_client/node_directory.h>

#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/concurrency/lease_manager.h>

#include <optional>

namespace NYT::NDataNode {

////////////////////////////////////////////////////////////////////////////////

struct TSessionOptions
{
    TWorkloadDescriptor WorkloadDescriptor;
    bool SyncOnClose = false;
    bool EnableMultiplexing = false;
    NChunkClient::TPlacementId PlacementId;
    bool DisableSendBlocks = false;
};

////////////////////////////////////////////////////////////////////////////////

//! Represents a chunk upload session.
/*!
 *  \note
 *  Thread affinity: any
 */
struct ISession
    : public virtual TRefCounted
{
    //! Returns the TChunkId being uploaded.
    virtual TChunkId GetChunkId() const& = 0;

    //! Returns the session ID.
    virtual TSessionId GetId() const& = 0;

    //! Returns the session type.
    virtual ESessionType GetType() const = 0;

    //! Returns the master epoch of session.
    virtual NClusterNode::TMasterEpoch GetMasterEpoch() const = 0;

    //! Returns the workload descriptor provided by the client during handshake.
    virtual const TWorkloadDescriptor& GetWorkloadDescriptor() const = 0;

    virtual const TSessionOptions& GetSessionOptions() const = 0;

    //! Returns the target chunk location.
    virtual const TStoreLocationPtr& GetStoreLocation() const = 0;

    //! Starts the session.
    /*!
     *  Returns the flag indicating that the session is persistently started.
     *  For blob chunks this happens immediately (and the actually opening happens in background).
     *  For journal chunks this happens when append record is flushed into the multiplexed changelog.
     */
    virtual TFuture<void> Start() = 0;

    //! Cancels the session.
    virtual void Cancel(const TError& error) = 0;

    //! Finishes the session.
    virtual TFuture<NChunkClient::NProto::TChunkInfo> Finish(
        const NChunkClient::TRefCountedChunkMetaPtr& chunkMeta,
        std::optional<int> blockCount) = 0;

    //! Puts a contiguous range of blocks into the window.
    virtual TFuture<NIO::TIOCounters> PutBlocks(
        int startBlockIndex,
        const std::vector<NChunkClient::TBlock>& blocks,
        bool enableCaching) = 0;

    //! Sends a range of blocks (from the current window) to another data node.
    virtual TFuture<NChunkClient::TDataNodeServiceProxy::TRspPutBlocksPtr> SendBlocks(
        int startBlockIndex,
        int blockCount,
        const NNodeTrackerClient::TNodeDescriptor& target) = 0;

    //! Flushes blocks up to a given one.
    virtual TFuture<NIO::TIOCounters> FlushBlocks(int blockIndex) = 0;

    //! Renews the lease.
    virtual void Ping() = 0;

    //! Called by session manager. Indicates that the session was unregistered.
    virtual void OnUnregistered() = 0;

    virtual void UnlockChunk() = 0;

    virtual TFuture<void> GetUnregisteredEvent() = 0;

    DECLARE_INTERFACE_SIGNAL(void(const TError& error), Finished);
};

DEFINE_REFCOUNTED_TYPE(ISession)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode

