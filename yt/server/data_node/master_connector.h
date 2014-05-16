#pragma once

#include "public.h"

#include <core/actions/signal.h>

#include <core/rpc/channel.h>

#include <core/concurrency/thread_affinity.h>

#include <core/actions/cancelable_context.h>

#include <ytlib/node_tracker_client/node_tracker_service_proxy.h>

#include <ytlib/job_tracker_client/job_tracker_service_proxy.h>

#include <server/cell_node/public.h>

namespace NYT {
namespace NDataNode {

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

    //! Returns |true| iff node is currently connected to master.
    bool IsConnected() const;

    //! Returns the node id assigned by master or |InvalidNodeId| if the node
    //! is not registered.
    TNodeId GetNodeId() const;

    //! Adds a given message to the list of alerts sent to master with each heartbeat.
    void RegisterAlert(const Stroka& alert);

private:
    typedef yhash_set<IChunkPtr> TChunkSet;

    TDataNodeConfigPtr Config;
    NCellNode::TBootstrap* Bootstrap;

    bool Started;
    IInvokerPtr ControlInvoker;

    DECLARE_ENUM(EState,
        // Not registered.
        (Offline)
        // Register request is in progress.
        (Registering)
        // Registered but did not report the full heartbeat yet.
        (Registered)
        // Registered and reported the full heartbeat.
        (Online)
    );

    //! Guards the current heartbeat session.
    TCancelableContextPtr HeartbeatContext;

    //! Corresponds to #HeartbeatContext and #ControlInvoker.
    IInvokerPtr HeartbeatInvoker;

    //! The current connection state.
    EState State;

    //! Node id assigned by master or |InvalidNodeId| is not registered.
    TNodeId NodeId;

    //! Chunks that were added since the last successful heartbeat.
    TChunkSet AddedSinceLastSuccess;

    //! Store chunks that were removed since the last successful heartbeat.
    TChunkSet RemovedSinceLastSuccess;

    //! Store chunks that were reported added at the last heartbeat (for which no reply is received yet).
    TChunkSet ReportedAdded;

    //! Store chunks that were reported removed at the last heartbeat (for which no reply is received yet).
    TChunkSet ReportedRemoved;

    //! Protects #Alerts.
    TSpinLock AlertsLock;
    //! A list of registered alerts.
    std::vector<Stroka> Alerts;

    
    //! Schedules a new node heartbeat via TDelayedExecutor.
    void ScheduleNodeHeartbeat();

    //! Schedules a new job heartbeat via TDelayedExecutor.
    void ScheduleJobHeartbeat();

    //! Calls #Reset and schedules a new registration request via TDelayedExecutor.
    void ResetAndScheduleRegister();

    //! Invoked when a node heartbeat must be sent.
    void OnNodeHeartbeat();

    //! Invoked when a job heartbeat must be sent.
    void OnJobHeartbeat();

    //! Sends out a registration request.
    void SendRegister();

    //! Computes the current node statistics.
    NNodeTrackerClient::NProto::TNodeStatistics ComputeStatistics();

    //! Handles registration response.
    void OnRegisterResponse(NNodeTrackerClient::TNodeTrackerServiceProxy::TRspRegisterNodePtr rsp);

    //! Sends out a full heartbeat.
    void SendFullNodeHeartbeat();

    //! Sends out an incremental heartbeat to Node Tracker.
    void SendIncrementalNodeHeartbeat();

    //! Sends out a heartbeat to Job Tracker.
    void SendJobHeartbeat();

    //! Similar to #ForceRegister but handled in Control thread.
    void StartHeartbeats();

    //! Constructs a protobuf info for an added chunk.
    static NNodeTrackerClient::NProto::TChunkAddInfo BuildAddChunkInfo(IChunkPtr chunk);

    //! Constructs a protobuf info for a removed chunk.
    static NNodeTrackerClient::NProto::TChunkRemoveInfo BuildRemoveChunkInfo(IChunkPtr chunk);

    //! Handles full heartbeat response from Node Tracker.
    void OnFullNodeHeartbeatResponse(NNodeTrackerClient::TNodeTrackerServiceProxy::TRspFullHeartbeatPtr rsp);

    //! Handles incremental heartbeat response from Node Tracker.
    void OnIncrementalNodeHeartbeatResponse(NNodeTrackerClient::TNodeTrackerServiceProxy::TRspIncrementalHeartbeatPtr rsp);

    //! Handles heartbeat response from Job Tracker.
    void OnJobHeartbeatResponse(NJobTrackerClient::TJobTrackerServiceProxy::TRspHeartbeatPtr rsp);

    //! Resets connection state.
    void Reset();

    //! Handles registration of new chunks.
    /*!
     *  Places the chunk into a list and reports its arrival
     *  to the master upon a next heartbeat.
     */
    void OnChunkAdded(IChunkPtr chunk);

    //! Handles removal of existing chunks.
    /*!
     *  Places the chunk into a list and reports its removal
     *  to the master upon a next heartbeat.
     */
    void OnChunkRemoved(IChunkPtr chunk);

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

};

DEFINE_REFCOUNTED_TYPE(TMasterConnector)

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
