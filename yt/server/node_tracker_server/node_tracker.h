#pragma once

#include "public.h"

#include <yt/server/cell_master/public.h>

#include <yt/server/hydra/entity_map.h>

#include <yt/ytlib/hive/cell_directory.h>

#include <yt/ytlib/hydra/public.h>

#include <yt/ytlib/node_tracker_client/node_statistics.h>

#include <yt/core/actions/signal.h>

#include <yt/core/rpc/service_detail.h>

namespace NYT::NNodeTrackerServer {

////////////////////////////////////////////////////////////////////////////////

class TNodeTracker
    : public TRefCounted
{
public:
    TNodeTracker(
        TNodeTrackerConfigPtr config,
        NCellMaster::TBootstrap* bootstrap);
    ~TNodeTracker();

    void Initialize();

    using TCtxRegisterNode = NRpc::TTypedServiceContext<
        NNodeTrackerClient::NProto::TReqRegisterNode,
        NNodeTrackerClient::NProto::TRspRegisterNode>;
    using TCtxRegisterNodePtr = TIntrusivePtr<TCtxRegisterNode>;
    void ProcessRegisterNode(const TString& address, TCtxRegisterNodePtr context);

    typedef NRpc::TTypedServiceContext<
        NNodeTrackerClient::NProto::TReqFullHeartbeat,
        NNodeTrackerClient::NProto::TRspFullHeartbeat> TCtxFullHeartbeat;
    using TCtxFullHeartbeatPtr = TIntrusivePtr<TCtxFullHeartbeat>;
    void ProcessFullHeartbeat(TCtxFullHeartbeatPtr context);

    using TCtxIncrementalHeartbeat = NRpc::TTypedServiceContext<
        NNodeTrackerClient::NProto::TReqIncrementalHeartbeat,
        NNodeTrackerClient::NProto::TRspIncrementalHeartbeat>;
    using TCtxIncrementalHeartbeatPtr = TIntrusivePtr<TCtxIncrementalHeartbeat>;
    void ProcessIncrementalHeartbeat(TCtxIncrementalHeartbeatPtr context);


    DECLARE_ENTITY_MAP_ACCESSORS(Node, TNode);
    DECLARE_ENTITY_MAP_ACCESSORS(Rack, TRack);
    DECLARE_ENTITY_MAP_ACCESSORS(DataCenter, TDataCenter);


    //! Fired when a node gets registered.
    DECLARE_SIGNAL(void(TNode* node), NodeRegistered);
    
    //! Fired when a node gets unregistered.
    DECLARE_SIGNAL(void(TNode* node), NodeUnregistered);

    //! Fired when a node gets disposed (after being unregistered).
    DECLARE_SIGNAL(void(TNode* node), NodeDisposed);

    //! Fired when node "banned" flag changes.
    DECLARE_SIGNAL(void(TNode* node), NodeBanChanged);

    //! Fired when node "decommissioned" flag changes.
    DECLARE_SIGNAL(void(TNode* node), NodeDecommissionChanged);

    //! Fired when node "disable_tablet_cells" flag changes.
    DECLARE_SIGNAL(void(TNode* node), NodeDisableTabletCellsChanged);

    //! Fired when node tags change.
    DECLARE_SIGNAL(void(TNode* node), NodeTagsChanged);

    //! Fired when node rack changes.
    DECLARE_SIGNAL(void(TNode* node, TRack* oldRack), NodeRackChanged);

    //! Fired for all nodes in a rack when that rack's DC changes.
    /*!
     *  NB: a node's DC may also change when its rack changes. This signal is
     *  not fired in those cases.
     */
    DECLARE_SIGNAL(void(TNode* node, TDataCenter* oldDataCenter), NodeDataCenterChanged);

    //! Fired when a new data center is created.
    DECLARE_SIGNAL(void(TDataCenter* dataCenter), DataCenterCreated);

    //! Fired when a data center is renamed.
    DECLARE_SIGNAL(void(TDataCenter* dataCenter), DataCenterRenamed);

    //! Fired when a data center is removed.
    DECLARE_SIGNAL(void(TDataCenter* dataCenter), DataCenterDestroyed);

    //! Fired when a full heartbeat is received from a node.
    DECLARE_SIGNAL(void(
        TNode* node,
        NNodeTrackerClient::NProto::TReqFullHeartbeat* request),
        FullHeartbeat);

    //! Fired when an incremental heartbeat is received from a node.
    DECLARE_SIGNAL(void(
        TNode* node,
        NNodeTrackerClient::NProto::TReqIncrementalHeartbeat* request,
        NNodeTrackerClient::NProto::TRspIncrementalHeartbeat* response),
        IncrementalHeartbeat);


    //! Returns a node with a given id (|nullptr| if none).
    TNode* FindNode(TNodeId id);

    //! Returns a node with a given id (fails if none).
    TNode* GetNode(TNodeId id);

    //! Returns a node with a given id (throws if none).
    TNode* GetNodeOrThrow(TNodeId id);

    //! Returns a node registered at the given address (|nullptr| if none).
    TNode* FindNodeByAddress(const TString& address);

    //! Returns a node registered at the given address (fails if none).
    TNode* GetNodeByAddress(const TString& address);

    //! Returns a node registered at the given address (throws if none).
    TNode* GetNodeByAddressOrThrow(const TString& address);

    //! Returns an arbitrary node registered at the host (|nullptr| if none).
    TNode* FindNodeByHostName(const TString& hostName);

    //! Returns the list of all nodes belonging to a given rack.
    /*!
     *  #rack can be |nullptr|.
     */
    std::vector<TNode*> GetRackNodes(const TRack* rack);

    //! Returns the list of all racks belonging to a given data center.
    /*!
     *  #dc can be |nullptr|.
     */
    std::vector<TRack*> GetDataCenterRacks(const TDataCenter* dc);


    //! Sets the "banned" flag and notifies the subscribers.
    void SetNodeBanned(TNode* node, bool value);

    //! Sets the "decommissioned" flag and notifies the subscribers.
    void SetNodeDecommissioned(TNode* node, bool value);

    //! Sets the rack and notifies the subscribers.
    void SetNodeRack(TNode* node, TRack* rack);

    //! Sets the user tags for the node.
    void SetNodeUserTags(TNode* node, const std::vector<TString>& tags);

    //! Sets the flag disabling write sessions at the node.
    void SetDisableWriteSessions(TNode* node, bool value);

    //! Sets the flag disabling tablet cells at the node.
    void SetDisableTabletCells(TNode* node, bool value);

    //! Creates a new rack with a given name. Throws on name conflict.
    TRack* CreateRack(const TString& name);

    //! Destroys an existing rack.
    void DestroyRack(TRack* rack);

    //! Renames an existing racks. Throws on name conflict.
    void RenameRack(TRack* rack, const TString& newName);

    //! Returns a rack with a given name (|nullptr| if none).
    TRack* FindRackByName(const TString& name);

    //! Returns a rack with a given name (throws if none).
    TRack* GetRackByNameOrThrow(const TString& name);

    //! Sets the data center and notifies the subscribers.
    void SetRackDataCenter(TRack* rack, TDataCenter* dc);


    //! Creates a new data center with a given name. Throws on name conflict.
    TDataCenter* CreateDataCenter(const TString& name);

    //! Destroys an existing data center.
    void DestroyDataCenter(TDataCenter* dc);

    //! Renames an existing data center. Throws on name conflict.
    void RenameDataCenter(TDataCenter* dc, const TString& newName);

    //! Returns a data center with a given name (|nullptr| if none).
    TDataCenter* FindDataCenterByName(const TString& name);

    //! Returns a data center with a given name (throws if none).
    TDataCenter* GetDataCenterByNameOrThrow(const TString& name);


    //! Returns the total cluster statistics, aggregated over all nodes.
    NNodeTrackerClient::TTotalNodeStatistics GetTotalNodeStatistics();

    //! Returns the number of nodes with ENodeState::Online aggregated state.
    int GetOnlineNodeCount();

private:
    class TImpl;
    class TClusterNodeTypeHandler;
    class TRackTypeHandler;
    class TDataCenterTypeHandler;

    const TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TNodeTracker)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNodeTrackerServer
