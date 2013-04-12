#pragma once

#include "public.h"

#include <ytlib/rpc/channel.h>

#include <ytlib/misc/thread_affinity.h>

#include <ytlib/node_tracker_client/node_tracker_service_proxy.h>

#include <ytlib/job_tracker_client/job_tracker_service_proxy.h>

#include <server/cell_node/public.h>

namespace NYT {
namespace NChunkHolder {

////////////////////////////////////////////////////////////////////////////////

//! Mediates connection between a node and its master.
/*!
 *  This class is responsible for registering the node and sending
 *  heartbeats. In particular, it reports chunk deltas to the master
 *  and manages jobs.
 */
class TMasterConnector
    : public TRefCounted
{
public:
    //! Creates an instance.
    TMasterConnector(
        TDataNodeConfigPtr config,
        NCellNode::TBootstrap* bootstrap);

    //! Starts interaction with master.
    void Start();

    //! Forces a new registration round and a full heartbeat to be sent.
    /*!
     *  Thread affinity: any
     *
     *  Typically called when a location goes down.
     */
    void ForceRegister();

    //! Returns the node id assigned by master or |InvalidNodeId| if the node
    //! is not registered.
    TNodeId GetNodeId() const;

private:
    typedef NNodeTrackerClient::TNodeTrackerServiceProxy TNodeProxy;
    typedef NJobTrackerClient::TJobTrackerServiceProxy TJobProxy;

    typedef yhash_set<TChunkPtr> TChunkSet;

    TDataNodeConfigPtr Config;
    NCellNode::TBootstrap* Bootstrap;
    IInvokerPtr ControlInvoker;

    DECLARE_ENUM(EState,
        // Not registered.
        (Offline)
        // Registered but did not report the full heartbeat yet.
        (Registered)
        // Registered and reported the full heartbeat.
        (Online)
    );

    //! The current connection state.
    EState State;

    //! Node id assigned by master or |InvalidNodeId| is not registered.
    TNodeId NodeId;

    //! Node Tracker proxy.
    THolder<TNodeProxy> NodeProxy;

    //! Job Tracker proxy.
    THolder<TJobProxy> JobProxy;

    //! Chunks that were added since the last successful heartbeat.
    TChunkSet AddedSinceLastSuccess;

    //! Store chunks that were removed since the last successful heartbeat.
    TChunkSet RemovedSinceLastSuccess;

    //! Store chunks that were reported added at the last heartbeat (for which no reply is received yet).
    TChunkSet ReportedAdded;

    //! Store chunks that were reported removed at the last heartbeat (for which no reply is received yet).
    TChunkSet ReportedRemoved;

    //! Schedules a heartbeat via TDelayedInvoker.
    void ScheduleHeartbeat();

    //! Invoked when a heartbeat must be sent.
    void OnHeartbeat();

    //! Sends out a registration request.
    void SendRegister();

    //! Computes the current node statistics.
    NNodeTrackerClient::NProto::TNodeStatistics ComputeStatistics();

    //! Handles registration response.
    void OnRegisterResponse(TNodeProxy::TRspRegisterNodePtr rsp);

    //! Sends out a full heartbeat.
    void SendFullNodeHeartbeat();

    //! Sends out an incremental heartbeat to Node Tracker.
    void SendIncrementalNodeHeartbeat();

    //! Sends out a heartbeat to Job Tracker.
    void SendJobHeartbeat();

    //! Similar to #ForceRegister but handled in Control thread.
    void DoForceRegister();

    //! Constructs a protobuf info for an added chunk.
    static NNodeTrackerClient::NProto::TChunkAddInfo GetAddInfo(TChunkPtr chunk);

    //! Constructs a protobuf info for a removed chunk.
    static NNodeTrackerClient::NProto::TChunkRemoveInfo GetRemoveInfo(TChunkPtr chunk);

    //! Handles full heartbeat response from Node Tracker.
    void OnFullNodeHeartbeatResponse(TNodeProxy::TRspFullHeartbeatPtr rsp);

    //! Handles incremental heartbeat response from Node Tracker.
    void OnIncrementalNodeHeartbeatResponse(TNodeProxy::TRspIncrementalHeartbeatPtr rsp);

    //! Handles heartbeat response from Job Tracker.
    void OnJobHeartbeatResponse(TJobProxy::TRspHeartbeatPtr rsp);

    //! Handles errors occurring during heartbeats.
    void OnHeartbeatError(const TError& error);

    //! Handles error during a registration or a heartbeat.
    void Disconnect();

    //! Handles registration of new chunks.
    /*!
     *  Places the chunk into a list and reports its arrival
     *  to the master upon a next heartbeat.
     */
    void OnChunkAdded(TChunkPtr chunk);

    //! Handles removal of existing chunks.
    /*!
     *  Places the chunk into a list and reports its removal
     *  to the master upon a next heartbeat.
     */
    void OnChunkRemoved(TChunkPtr chunk);

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkHolder
} // namespace NYT
