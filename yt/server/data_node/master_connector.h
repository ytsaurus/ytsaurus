#pragma once

#include "public.h"

#include <core/actions/signal.h>

#include <core/rpc/channel.h>

#include <core/concurrency/thread_affinity.h>

#include <core/actions/cancelable_context.h>

#include <ytlib/node_tracker_client/node_tracker_service_proxy.h>
#include <ytlib/node_tracker_client/node_directory.h>

#include <ytlib/job_tracker_client/job_tracker_service_proxy.h>

#include <ytlib/transaction_client/public.h>

#include <server/cell_node/public.h>

namespace NYT {
namespace NDataNode {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EMasterConnectorState,
    // Not registered.
    (Offline)
    // Registered but did not report the full heartbeat yet.
    (Registered)
    // Registered and reported the full heartbeat.
    (Online)
);

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
    //! Raised with each heartbeat.
    //! Subscribers may provide additional dynamic alerts to be reported to master.
    DEFINE_SIGNAL(void(std::vector<TError>* alerts), PopulateAlerts);

public:
    //! Creates an instance.
    TMasterConnector(
        TDataNodeConfigPtr config,
        const NNodeTrackerClient::TAddressMap& localAddresses,
        NCellNode::TBootstrap* bootstrap);

    //! Starts interaction with master.
    void Start();

    //! Forces a new registration round and a full heartbeat to be sent.
    /*!
     *  Thread affinity: any
     *
     *  Typically called when a location goes down.
     */
    void ForceRegisterAtMaster();

    //! Returns |true| iff node is currently connected to master.
    bool IsConnected() const;

    //! Returns the node id assigned by master or |InvalidNodeId| if the node
    //! is not registered.
    TNodeId GetNodeId() const;

    //! Adds a given message to the list of alerts sent to master with each heartbeat.
    /*!
     *  Thread affinity: any
     */
    void RegisterAlert(const TError& alert);

    //! Returns a statically known map for the local addresses.
    /*!
     *  \note
     *  Thread affinity: any
     */
    const NNodeTrackerClient::TAddressMap& GetLocalAddresses() const;

    //! Returns a dynamically updated node descriptor.
    /*!
     *  \note
     *  Thread affinity: any
     */
    NNodeTrackerClient::TNodeDescriptor GetLocalDescriptor() const;

private:
    using EState = EMasterConnectorState;

    const TDataNodeConfigPtr Config_;
    const NNodeTrackerClient::TAddressMap LocalAddresses_;
    const NCellNode::TBootstrap* Bootstrap_;
    const IInvokerPtr ControlInvoker_;

    bool Started_ = false;

    //! Guards the current heartbeat session.
    TCancelableContextPtr HeartbeatContext_;

    //! Corresponds to #HeartbeatContext and #ControlInvoker.
    IInvokerPtr HeartbeatInvoker_;

    //! The lease transaction.
    NTransactionClient::TTransactionPtr LeaseTransaction_;

    //! Node id assigned by master or |InvalidNodeId| is not registered.
    TNodeId NodeId_ = NNodeTrackerClient::InvalidNodeId;

    struct TChunksDelta
    {
        //! Synchronization state.
        EState State = EState::Offline;

        //! Chunks that were added since the last successful heartbeat.
        yhash_set<IChunkPtr> AddedSinceLastSuccess;

        //! Chunks that were removed since the last successful heartbeat.
        yhash_set<IChunkPtr> RemovedSinceLastSuccess;

        //! Maps chunks that were reported added at the last heartbeat (for which no reply is received yet) to their versions.
        yhash_map<IChunkPtr, int> ReportedAdded;

        //! Chunks that were reported removed at the last heartbeat (for which no reply is received yet).
        yhash_set<IChunkPtr> ReportedRemoved;
    };

    //! Per-cell chunks delta.
    yhash_map<NObjectClient::TCellTag, TChunksDelta> ChunksDeltaMap_;

    //! All master cell tags (including primary).
    std::vector<NObjectClient::TCellTag> MasterCellTags_;

    //! Index in MasterCellTags_ indicating the current target for job heartbeat round-robin.
    int JobHeartbeatCellIndex_ = 0;

    //! Protects #Alerts.
    TSpinLock AlertsLock_;
    //! A list of statically registered alerts.
    std::vector<TError> StaticAlerts_;

    TSpinLock LocalDescriptorLock_;
    NNodeTrackerClient::TNodeDescriptor LocalDescriptor_;


    //! Returns the list of all active alerts, including those induced
    //! by |PopulateAlerts| subscribers.
    /*!
     *  Thread affinity: any
     */
    std::vector<TError> GetAlerts();

    //! Schedules a new node heartbeat via TDelayedExecutor.
    void ScheduleNodeHeartbeat(NObjectClient::TCellTag cellTag);

    //! Schedules a new job heartbeat via TDelayedExecutor.
    void ScheduleJobHeartbeat();

    //! Calls #Reset and schedules a new registration attempt.
    void ResetAndScheduleRegisterAtMaster();

    //! Sends an appropriate node heartbeat.
    void SendNodeHeartbeat(NObjectClient::TCellTag cellTag);

    //! Starts a lease transaction.
    //! Sends out a registration request to master.
    void RegisterAtMaster();

    //! Handles lease transaction abort.
    void OnLeaseTransactionAborted();

    //! Computes the current node statistics.
    NNodeTrackerClient::NProto::TNodeStatistics ComputeStatistics();

    //! Sends out a full heartbeat to Node Tracker.
    void SendFullNodeHeartbeat(NObjectClient::TCellTag cellTag);

    //! Sends out an incremental heartbeat to Node Tracker.
    void SendIncrementalNodeHeartbeat(NObjectClient::TCellTag cellTag);

    //! Sends out a heartbeat to Job Tracker.
    void SendJobHeartbeat();

    //! Similar to #ForceRegisterAtMaster but handled in Control thread.
    void StartHeartbeats();

    //! Constructs a protobuf info for an added chunk.
    NNodeTrackerClient::NProto::TChunkAddInfo BuildAddChunkInfo(IChunkPtr chunk);

    //! Constructs a protobuf info for a removed chunk.
    static NNodeTrackerClient::NProto::TChunkRemoveInfo BuildRemoveChunkInfo(IChunkPtr chunk);

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

    TChunksDelta* GetChunksDelta(NObjectClient::TCellTag cellTag);
    TChunksDelta* GetChunksDelta(const NObjectClient::TObjectId& id);

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

};

DEFINE_REFCOUNTED_TYPE(TMasterConnector)

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
