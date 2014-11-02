#pragma once

#include "public.h"

#include <core/actions/signal.h>

#include <core/rpc/service_detail.h>

#include <ytlib/hydra/public.h>

#include <ytlib/node_tracker_client/node_statistics.h>

#include <server/hydra/entity_map.h>

#include <server/cell_master/public.h>

namespace NYT {
namespace NNodeTrackerServer {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

typedef NNodeTrackerClient::NProto::TReqRegisterNode TReqRegisterNode;
typedef NNodeTrackerClient::NProto::TReqIncrementalHeartbeat TReqIncrementalHeartbeat;
typedef NNodeTrackerClient::NProto::TReqFullHeartbeat TReqFullHeartbeat;

} // namespace NProto

class TNodeTracker
    : public TRefCounted
{
public:
    TNodeTracker(
        TNodeTrackerConfigPtr config,
        NCellMaster::TBootstrap* bootstrap);

    void Initialize();

    ~TNodeTracker();


    NHydra::TMutationPtr CreateRegisterNodeMutation(
        const NProto::TReqRegisterNode& request);

    NHydra::TMutationPtr CreateUnregisterNodeMutation(
        const NProto::TReqUnregisterNode& request);

    // Pass RPC service context to full heartbeat handler to avoid copying request message.
    typedef NRpc::TTypedServiceContext<
        NNodeTrackerClient::NProto::TReqFullHeartbeat,
        NNodeTrackerClient::NProto::TRspFullHeartbeat> TCtxFullHeartbeat;
    typedef TIntrusivePtr<TCtxFullHeartbeat> TCtxFullHeartbeatPtr;
    NHydra::TMutationPtr CreateFullHeartbeatMutation(
        TCtxFullHeartbeatPtr context);

    typedef NRpc::TTypedServiceContext<
        NNodeTrackerClient::NProto::TReqIncrementalHeartbeat,
        NNodeTrackerClient::NProto::TRspIncrementalHeartbeat> TCtxIncrementalHeartbeat;
    typedef TIntrusivePtr<TCtxIncrementalHeartbeat> TCtxIncrementalHeartbeatPtr;
    NHydra::TMutationPtr CreateIncrementalHeartbeatMutation(
        TCtxIncrementalHeartbeatPtr context);


    void RefreshNodeConfig(TNode* node);


    DECLARE_ENTITY_MAP_ACCESSORS(Node, TNode, TNodeId);

    //! Fired when a node gets registered.
    DECLARE_SIGNAL(void(TNode* node), NodeRegistered);
    
    //! Fired when a node gets unregistered.
    DECLARE_SIGNAL(void(TNode* node), NodeUnregistered);

    //! Fired when a node gets removed.
    DECLARE_SIGNAL(void(TNode* node), NodeRemoved);

    //! Fired when node configuration changes.
    DECLARE_SIGNAL(void(TNode* node), NodeConfigUpdated);

    //! Fired when a full heartbeat is received from a node.
    DECLARE_SIGNAL(void(TNode* node, const NProto::TReqFullHeartbeat& request), FullHeartbeat);

    //! Fired when an incremental heartbeat is received from a node.
    DECLARE_SIGNAL(void(
        TNode* node,
        const NProto::TReqIncrementalHeartbeat& request,
        NNodeTrackerClient::NProto::TRspIncrementalHeartbeat* response),
        IncrementalHeartbeat);


    //! Returns a node registered at the given address (|nullptr| if none).
    TNode* FindNodeByAddress(const Stroka& address);

    //! Returns a node registered at the given address (fails if none).
    TNode* GetNodeByAddress(const Stroka& address);

    //! Returns an arbitrary node registered at the host (|nullptr| if none).
    TNode* FindNodeByHostName(const Stroka& hostName);

    //! Returns a node with a given id (throws if none).
    TNode* GetNodeOrThrow(TNodeId id);


    //! Returns node configuration (extracted from //sys/nodes) or |nullptr| is there's none.
    TNodeConfigPtr FindNodeConfigByAddress(const Stroka& address);

    //! Similar to #FindNodeConfigByAddress but returns a default instance instead of |nullptr|.
    TNodeConfigPtr GetNodeConfigByAddress(const Stroka& address);


    NNodeTrackerClient::TTotalNodeStatistics GetTotalNodeStatistics();

    //! Returns the number of nodes in |Registered| state.
    int GetRegisteredNodeCount();

    //! Returns the number of nodes in |Online| state.
    int GetOnlineNodeCount();

private:
    class TImpl;
    
    TIntrusivePtr<TImpl> Impl_;

};

DEFINE_REFCOUNTED_TYPE(TNodeTracker)

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeTrackerServer
} // namespace NYT
