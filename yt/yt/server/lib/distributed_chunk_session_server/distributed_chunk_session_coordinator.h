#pragma once

#include "public.h"

#include <yt/yt/ytlib/distributed_chunk_session_client/proto/distributed_chunk_session_service.pb.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/core/actions/future.h>

#include <yt_proto/yt/client/chunk_client/proto/chunk_meta.pb.h>

namespace NYT::NDistributedChunkSessionServer {

////////////////////////////////////////////////////////////////////////////////

struct TCoordinatorStatus
{
    bool CloseDemanded = false;
    int WrittenBlockCount = 0;
    NChunkClient::NProto::TMiscExt ChunkMiscMeta;
    std::vector<NTableClient::NProto::TDataBlockMeta> DataBlockMetas;
};

void ToProto(NDistributedChunkSessionClient::NProto::TRspPingSession* proto, const TCoordinatorStatus& status);

/*!
 *  \note Thread affinity: any
 */
struct IDistributedChunkSessionCoordinator
    : virtual public TRefCounted
{
    // Returns future that is set when the session is finished.
    virtual TFuture<void> Run() = 0;

    virtual TFuture<void> SendBlocks(
        std::vector<NChunkClient::TBlock> blocks,
        std::vector<NTableClient::NProto::TDataBlockMeta> blockMetas,
        NChunkClient::NProto::TMiscExt blocksMiscMeta) = 0;

    virtual TFuture<TCoordinatorStatus> UpdateStatus(int acknowledgedBlockCount) = 0;

    // Each pending SendBlocks call can be in one of three states:
    // 1. WaitingDataNodesRequest - waiting to send blocks to data nodes.
    // 2. WaitingDataNodesResponse - waiting a response from the data nodes.
    // 3. WaitingControllerAcknowledge - waiting for acknowledgment from the controller.
    //
    // Close() cancels pending SendBlocks in the WaitingDataNodesRequest state.
    // Close(/*force*/ true) cancels all pending SendBlocks requests, regardless of their state.
    //
    // New SendBlocks calls attempted after Close will result in a SessionClosed error.
    virtual void Close(bool force = false) = 0;
};

DEFINE_REFCOUNTED_TYPE(IDistributedChunkSessionCoordinator)

////////////////////////////////////////////////////////////////////////////////

IDistributedChunkSessionCoordinatorPtr CreateDistributedChunkSessionCoordinator(
    TDistributedChunkSessionServiceConfigPtr config,
    NChunkClient::TSessionId sessionId,
    std::vector<NNodeTrackerClient::TNodeDescriptor> targets,
    IInvokerPtr invoker,
    NApi::NNative::IConnectionPtr connection);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDistributedChunkSessionServer
