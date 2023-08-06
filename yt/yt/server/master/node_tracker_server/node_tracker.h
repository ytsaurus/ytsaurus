#pragma once

#include "public.h"

#include <yt/yt/server/master/cell_master/public.h>

#include <yt/yt/server/master/node_tracker_server/proto/node_tracker.pb.h>

#include <yt/yt/server/master/node_tracker_server/public.h>

#include <yt/yt/server/master/maintenance_tracker_server/public.h>

#include <yt/yt/server/lib/hydra_common/entity_map.h>

#include <yt/yt/ytlib/hive/cell_directory.h>

#include <yt/yt/ytlib/hydra/public.h>

#include <yt/yt/ytlib/object_client/proto/master_ypath.pb.h>

#include <yt/yt/ytlib/node_tracker_client/node_statistics.h>

#include <yt/yt/ytlib/node_tracker_client/proto/node_tracker_service.pb.h>

#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/rpc/service_detail.h>

namespace NYT::NNodeTrackerServer {

using NMaintenanceTrackerServer::EMaintenanceType;

////////////////////////////////////////////////////////////////////////////////

struct INodeTracker
    : public virtual TRefCounted
{
public:
    virtual void Initialize() = 0;

    using TCtxRegisterNode = NRpc::TTypedServiceContext<
        NNodeTrackerClient::NProto::TReqRegisterNode,
        NNodeTrackerClient::NProto::TRspRegisterNode>;
    using TCtxRegisterNodePtr = TIntrusivePtr<TCtxRegisterNode>;
    virtual void ProcessRegisterNode(const TString& address, TCtxRegisterNodePtr context) = 0;

    using TCtxHeartbeat = NRpc::TTypedServiceContext<
        NNodeTrackerClient::NProto::TReqHeartbeat,
        NNodeTrackerClient::NProto::TRspHeartbeat>;
    using TCtxHeartbeatPtr = TIntrusivePtr<TCtxHeartbeat>;
    virtual void ProcessHeartbeat(TCtxHeartbeatPtr context) = 0;

    using TCtxAddMaintenance = NRpc::TTypedServiceContext<
        NNodeTrackerClient::NProto::TReqAddMaintenance,
        NNodeTrackerClient::NProto::TRspAddMaintenance>;
    using TCtxAddMaintenancePtr = TIntrusivePtr<TCtxAddMaintenance>;

    using TCtxRemoveMaintenance = NRpc::TTypedServiceContext<
        NNodeTrackerClient::NProto::TReqRemoveMaintenance,
        NNodeTrackerClient::NProto::TRspRemoveMaintenance>;
    using TCtxRemoveMaintenancePtr = TIntrusivePtr<TCtxRemoveMaintenance>;

    DECLARE_INTERFACE_ENTITY_MAP_ACCESSORS(Node, TNode);
    DECLARE_INTERFACE_ENTITY_MAP_ACCESSORS(Host, THost);
    DECLARE_INTERFACE_ENTITY_MAP_ACCESSORS(Rack, TRack);
    DECLARE_INTERFACE_ENTITY_MAP_ACCESSORS(DataCenter, TDataCenter);

    //! Fired when a node gets registered.
    DECLARE_INTERFACE_SIGNAL(void(TNode* node), NodeRegistered);

    //! Fired when a node becomes online.
    DECLARE_INTERFACE_SIGNAL(void(TNode* node), NodeOnline);

    //! Fired when a node gets unregistered.
    DECLARE_INTERFACE_SIGNAL(void(TNode* node), NodeUnregistered);

    //! Fired when a node gets zombified.
    DECLARE_INTERFACE_SIGNAL(void(TNode* node), NodeZombified);

    //! Fired when node "banned" flag changes.
    DECLARE_INTERFACE_SIGNAL(void(TNode* node), NodeBanChanged);

    //! Fired when node "decommissioned" flag changes.
    DECLARE_INTERFACE_SIGNAL(void(TNode* node), NodeDecommissionChanged);

    //! Fired when node "disable_write_sessions" flag changes.
    DECLARE_INTERFACE_SIGNAL(void(TNode* node), NodeDisableWriteSessionsChanged);

    //! Fired when node "disable_tablet_cells" flag changes.
    DECLARE_INTERFACE_SIGNAL(void(TNode* node), NodeDisableTabletCellsChanged);

    //! Fired when node "pending_restart" flag changes.
    DECLARE_INTERFACE_SIGNAL(void(TNode* node), NodePendingRestartChanged);

    //! Fired when node tags change.
    DECLARE_INTERFACE_SIGNAL(void(TNode* node), NodeTagsChanged);

    //! Fired when node rack changes.
    /*!
     *  NB: a node's rack may also change when its host changes. This signal is
     *  also fired in those cases.
     */
    DECLARE_INTERFACE_SIGNAL(void(TNode* node, TRack* oldRack), NodeRackChanged);

    //! Fired for all nodes in a rack when that rack's DC changes.
    /*!
     *  NB: a node's DC may also change when its rack or host changes. This signal is
     *  not fired in those cases.
     */
    DECLARE_INTERFACE_SIGNAL(void(TNode* node, TDataCenter* oldDataCenter), NodeDataCenterChanged);

    //! Fired when a new data center is created.
    DECLARE_INTERFACE_SIGNAL(void(TDataCenter* dataCenter), DataCenterCreated);

    //! Fired when a data center is renamed.
    DECLARE_INTERFACE_SIGNAL(void(TDataCenter* dataCenter), DataCenterRenamed);

    //! Fired when a data center is removed.
    DECLARE_INTERFACE_SIGNAL(void(TDataCenter* dataCenter), DataCenterDestroyed);

    //! Fired when a new rack is created.
    DECLARE_INTERFACE_SIGNAL(void(TRack* rack), RackCreated);

    //! Fired when a rack is renamed.
    DECLARE_INTERFACE_SIGNAL(void(TRack* rack), RackRenamed);

    //! Fired when a rack data center changed.
    DECLARE_INTERFACE_SIGNAL(void(TRack* rack, TDataCenter* oldDataCenter), RackDataCenterChanged);

    //! Fired when a data rack is removed.
    DECLARE_INTERFACE_SIGNAL(void(TRack* rack), RackDestroyed);

    //! Fired when a new host is created.
    DECLARE_INTERFACE_SIGNAL(void(THost* host), HostCreated);

    //! Fired when a host rack changes.
    DECLARE_INTERFACE_SIGNAL(void(THost* host, TRack* oldRack), HostRackChanged);

    //! Fired when a host is removed.
    DECLARE_INTERFACE_SIGNAL(void(THost* host), HostDestroyed);


    //! Constructs the full object id from a (short) node id.
    virtual NObjectClient::TObjectId ObjectIdFromNodeId(TNodeId nodeId) = 0;

    //! Returns a node with a given id (|nullptr| if none).
    virtual TNode* FindNode(TNodeId id) = 0;

    //! Returns a node with a given id (fails if none).
    virtual TNode* GetNode(TNodeId id) = 0;

    //! Returns a node with a given id (throws if none).
    virtual TNode* GetNodeOrThrow(TNodeId id) = 0;

    //! Returns a node registered at the given address (|nullptr| if none).
    virtual TNode* FindNodeByAddress(const TString& address) = 0;

    //! Returns a node registered at the given address (fails if none).
    virtual TNode* GetNodeByAddress(const TString& address) = 0;

    //! Returns a node registered at the given address (throws if none).
    virtual TNode* GetNodeByAddressOrThrow(const TString& address) = 0;

    //! Returns an arbitrary node registered at the host (|nullptr| if none).
    virtual TNode* FindNodeByHostName(const TString& hostName) = 0;

    //! Returns a host with a given name (throws if none).
    virtual THost* GetHostByNameOrThrow(const TString& name) = 0;

    //! Returns a host with a given name (|nullptr| if none).
    virtual THost* FindHostByName(const TString& name) = 0;

    //! Returns a host with a given name (fails if none).
    virtual THost* GetHostByName(const TString& hostName) = 0;

    //! Sets the rack and notifies the subscribers.
    virtual void SetHostRack(THost* host, TRack* rack) = 0;

    //! Returns the list of all hosts belonging to a given rack.
    /*!
     *  #rack can be |nullptr|.
     */
    virtual std::vector<THost*> GetRackHosts(const TRack* rack) = 0;

    //! Returns the list of all nodes belonging to a given rack.
    /*!
     *  #rack can be |nullptr|.
     */
    virtual std::vector<TNode*> GetRackNodes(const TRack* rack) = 0;

    //! Returns the list of all racks belonging to a given data center.
    /*!
     *  #dc can be |nullptr|.
     */
    virtual std::vector<TRack*> GetDataCenterRacks(const TDataCenter* dc) = 0;

    //! Returns the set of all nodes with a given flavor.
    virtual const THashSet<TNode*>& GetNodesWithFlavor(ENodeFlavor flavor) const = 0;

    //! Sets last seen time of the node to now.
    virtual void UpdateLastSeenTime(TNode* node) = 0;

    //! Recalculates maintenance status.
    virtual void OnNodeMaintenanceUpdated(TNode* node, EMaintenanceType type) = 0;

    //! Sets the rack and notifies the subscribers.
    virtual void SetNodeHost(TNode* node, THost* host) = 0;

    //! Sets the user tags for the node.
    virtual void SetNodeUserTags(TNode* node, const std::vector<TString>& tags) = 0;

    //! Creates a mutation that updates node's resource usage and limits.
    virtual std::unique_ptr<NHydra::TMutation> CreateUpdateNodeResourcesMutation(
        const NProto::TReqUpdateNodeResources& request) = 0;

    //! Renames an existing racks. Throws on name conflict.
    virtual void RenameRack(TRack* rack, const TString& newName) = 0;

    //! Returns a rack with a given name (|nullptr| if none).
    virtual TRack* FindRackByName(const TString& name) = 0;

    //! Returns a rack with a given name (throws if none).
    virtual TRack* GetRackByNameOrThrow(const TString& name) = 0;

    //! Sets the data center and notifies the subscribers.
    virtual void SetRackDataCenter(TRack* rack, TDataCenter* dc) = 0;


    //! Renames an existing data center. Throws on name conflict.
    virtual void RenameDataCenter(TDataCenter* dc, const TString& newName) = 0;

    //! Returns a data center with a given name (|nullptr| if none).
    virtual TDataCenter* FindDataCenterByName(const TString& name) = 0;

    //! Returns a data center with a given name (throws if none).
    virtual TDataCenter* GetDataCenterByNameOrThrow(const TString& name) = 0;


    //! Returns the total cluster statistics, aggregated over all nodes.
    virtual NNodeTrackerClient::TAggregatedNodeStatistics GetAggregatedNodeStatistics() = 0;

    //! Returns cluster node statistics, aggregated over all nodes with a given flavor.
    virtual NNodeTrackerClient::TAggregatedNodeStatistics GetFlavoredNodeStatistics(ENodeFlavor flavor) = 0;

    //! Returns the number of nodes with ENodeState::Online aggregated state.
    virtual int GetOnlineNodeCount() = 0;

    //! Returns the list of nodes with the given role.
    virtual const std::vector<TNode*>& GetNodesForRole(NNodeTrackerClient::ENodeRole nodeRole) = 0;

    //! Returns the list of default addresses of nodes with the given role.
    virtual const std::vector<TString>& GetNodeAddressesForRole(NNodeTrackerClient::ENodeRole nodeRole) = 0;

    //! Called by node trackers when node reports a heartbeat.
    virtual void OnNodeHeartbeat(TNode* node, ENodeHeartbeatType heartbeatType) = 0;

    //! Forces node to report an out-of-order cellar heartbeat.
    virtual void RequestCellarHeartbeat(TNodeId nodeId) = 0;

    //! Sets node state and sends it to primary master.
    virtual void SetNodeLocalState(TNode* node, ENodeState state) = 0;

private:
    friend class TNodeTypeHandler;
    friend class THostTypeHandler;
    friend class TRackTypeHandler;
    friend class TDataCenterTypeHandler;

    virtual void ZombifyNode(TNode* node) = 0;

    virtual THost* CreateHost(const TString& address, NObjectClient::TObjectId hintId) = 0;
    virtual void ZombifyHost(THost* host) = 0;

    virtual TRack* CreateRack(const TString& name, NObjectClient::TObjectId hintId) = 0;
    virtual void ZombifyRack(TRack* rack) = 0;

    virtual TDataCenter* CreateDataCenter(const TString& name, NObjectClient::TObjectId hintId) = 0;
    virtual void ZombifyDataCenter(TDataCenter* dc) = 0;
};

DEFINE_REFCOUNTED_TYPE(INodeTracker)

////////////////////////////////////////////////////////////////////////////////

INodeTrackerPtr CreateNodeTracker(NCellMaster::TBootstrap* bootstrap);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerServer
