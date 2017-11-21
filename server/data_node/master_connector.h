#pragma once

#include "public.h"

#include <yt/server/cell_node/public.h>

#include <yt/ytlib/job_tracker_client/job_tracker_service_proxy.h>

#include <yt/ytlib/node_tracker_client/node_directory.h>
#include <yt/ytlib/node_tracker_client/node_tracker_service_proxy.h>

#include <yt/ytlib/api/public.h>

#include <yt/core/actions/cancelable_context.h>
#include <yt/core/actions/signal.h>

#include <yt/core/concurrency/thread_affinity.h>
#include <yt/core/concurrency/periodic_executor.h>

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

    //! Raised when node successfully connects and registers at the primary master.
    DEFINE_SIGNAL(void(), MasterConnected);

    //! Raised when node disconnects from masters.
    DEFINE_SIGNAL(void(), MasterDisconnected);

public:
    //! Creates an instance.
    TMasterConnector(
        TDataNodeConfigPtr config,
        const NNodeTrackerClient::TAddressMap& rpcAddresses,
        const NNodeTrackerClient::TAddressMap& skynetHttpAddresses,
        const std::vector<TString>& nodeTags,
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

    //! Returns future that is set when the next incremental heartbeat is successfully reported
    //! to cell #cellTag.
    TFuture<void> GetHeartbeatBarrier(NObjectClient::TCellTag cellTag);

private:
    using EState = EMasterConnectorState;

    const TDataNodeConfigPtr Config_;
    const NNodeTrackerClient::TAddressMap RpcAddresses_;
    const NNodeTrackerClient::TAddressMap SkynetHttpAddresses_;

    const std::vector<TString> NodeTags_;
    const NCellNode::TBootstrap* Bootstrap_;
    const IInvokerPtr ControlInvoker_;

    bool Started_ = false;

    //! Guards the current heartbeat session.
    TCancelableContextPtr HeartbeatContext_;

    //! Corresponds to #HeartbeatContext and #ControlInvoker.
    IInvokerPtr HeartbeatInvoker_;

    //! The lease transaction.
    NApi::ITransactionPtr LeaseTransaction_;

    //! Node id assigned by master or |InvalidNodeId| is not registered.
    TNodeId NodeId_ = NNodeTrackerClient::InvalidNodeId;

    struct TChunksDelta
    {
        //! Synchronization state.
        EState State = EState::Offline;

        //! Chunks that were added since the last successful heartbeat.
        THashSet<IChunkPtr> AddedSinceLastSuccess;

        //! Chunks that were removed since the last successful heartbeat.
        THashSet<IChunkPtr> RemovedSinceLastSuccess;

        //! Maps chunks that were reported added at the last heartbeat (for which no reply is received yet) to their versions.
        THashMap<IChunkPtr, int> ReportedAdded;

        //! Chunks that were reported removed at the last heartbeat (for which no reply is received yet).
        THashSet<IChunkPtr> ReportedRemoved;

        //! Set when another incremental heartbeat is successfully reported to the corresponding master.
        TPromise<void> HeartbeatBarrier = NewPromise<void>();
    };

    //! Per-cell chunks delta.
    THashMap<NObjectClient::TCellTag, TChunksDelta> ChunksDeltaMap_;

    //! All master cell tags (including the primary).
    NObjectClient::TCellTagList MasterCellTags_;

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
    void ScheduleNodeHeartbeat(NObjectClient::TCellTag cellTag, bool immediately = false);

    //! Schedules a new job heartbeat via TDelayedExecutor.
    void ScheduleJobHeartbeat(bool immediately = false);

    //! Calls #Reset and schedules a new registration attempt.
    void ResetAndScheduleRegisterAtMaster();

    //! Sends an appropriate node heartbeat.
    //! Handles the outcome and schedules the next heartbeat.
    void ReportNodeHeartbeat(NObjectClient::TCellTag cellTag);

    //! Starts a lease transaction.
    //! Sends out a registration request to master.
    void RegisterAtMaster();

    //! Requests media information from master and initializes
    //! the locations appropriately.
    void InitMedia();

    //! Synchronizes cell and cluster directories.
    void SyncDirectories();

    //! Starts the lease transaction and attaches the abort handler.
    void StartLeaseTransaction();

    //! Handles lease transaction abort.
    void OnLeaseTransactionAborted();

    //! Sends |RegisterNode| request to the primary master and waits for the response.
    void RegisterAtPrimaryMaster();

    //! Computes the current node statistics.
    NNodeTrackerClient::NProto::TNodeStatistics ComputeStatistics();

    // Implementation details for #ComputeStatistics().
    void ComputeTotalStatistics(NNodeTrackerClient::NProto::TNodeStatistics* result);
    void ComputeLocationSpecificStatistics(NNodeTrackerClient::NProto::TNodeStatistics* statistics);

    //! Returns |true| if the node is allowed to send a full heartbeat to Node Tracker
    //! of a given #cellTag.
    /*!
     *  To facilitate registration throttling, the node is only allowed to send
     *  a full heartbeat to the primary cell after
     *  it has become online at all secondary cells.
     */
    bool CanSendFullNodeHeartbeat(NObjectClient::TCellTag cellTag);

    //! Sends out a full heartbeat to Node Tracker.
    //! Handles the outcome and schedules the next heartbeat.
    void ReportFullNodeHeartbeat(NObjectClient::TCellTag cellTag);

    //! Sends out an incremental heartbeat to Node Tracker.
    //! Handles the outcome and schedules the next heartbeat.
    void ReportIncrementalNodeHeartbeat(NObjectClient::TCellTag cellTag);

    //! Sends out a heartbeat to Job Tracker.
    //! Handles the outcome and schedules the next heartbeat.
    void ReportJobHeartbeat();

    //! Similar to #ForceRegisterAtMaster but handled in Control thread.
    void StartHeartbeats();

    //! Constructs a protobuf info for an added chunk.
    NChunkClient::NProto::TChunkAddInfo BuildAddChunkInfo(IChunkPtr chunk);

    //! Constructs a protobuf info for a removed chunk.
    NChunkClient::NProto::TChunkRemoveInfo BuildRemoveChunkInfo(IChunkPtr chunk);

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

    //! Returns the channel used for registering at and reporting heartbeats
    //! to the leader of a given cell.
    /*!
     *  This channel is neither authenticated nor retrying.
     */
    NRpc::IChannelPtr GetMasterChannel(NObjectClient::TCellTag cellTag);

    //! Updates the rack of the local node.
    void UpdateRack(const TNullable<TString>& rack);

    //! Updates the data center of the local node.
    void UpdateDataCenter(const TNullable<TString>& dc);

    TChunksDelta* GetChunksDelta(NObjectClient::TCellTag cellTag);
    TChunksDelta* GetChunksDelta(const NObjectClient::TObjectId& id);

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);

};

DEFINE_REFCOUNTED_TYPE(TMasterConnector)

////////////////////////////////////////////////////////////////////////////////

} // namespace NDataNode
} // namespace NYT
