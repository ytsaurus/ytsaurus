#pragma once

#include "public.h"

#include <ytlib/actions/signal.h>

#include <ytlib/meta_state/public.h>

#include <ytlib/meta_state/map.h>

#include <ytlib/node_tracker_client/node_statistics.h>

#include <ytlib/rpc/service_detail.h>

#include <server/node_tracker_server/node_tracker.pb.h>

#include <server/cell_master/public.h>

namespace NYT {
namespace NNodeTrackerServer {

////////////////////////////////////////////////////////////////////////////////

namespace NProto {
    typedef NNodeTrackerClient::NProto::TReqFullHeartbeat TMetaReqFullHeartbeat;
} // namespace NProto

class TNodeTracker
    : public TRefCounted
{
public:
    TNodeTracker(
        TNodeTrackerConfigPtr config,
        NCellMaster::TBootstrap* bootstrap);

    ~TNodeTracker();

    NMetaState::TMutationPtr CreateRegisterNodeMutation(
        const NProto::TMetaReqRegisterNode& request);

    NMetaState::TMutationPtr CreateUnregisterNodeMutation(
        const NProto::TMetaReqUnregisterNode& request);

    // Pass RPC service context to full heartbeat handler to avoid copying request message.
    typedef NRpc::TTypedServiceContext<
        NNodeTrackerClient::NProto::TReqFullHeartbeat,
        NNodeTrackerClient::NProto::TRspFullHeartbeat> TCtxFullHeartbeat;
    typedef TIntrusivePtr<TCtxFullHeartbeat> TCtxFullHeartbeatPtr;
    NMetaState::TMutationPtr CreateFullHeartbeatMutation(
        TCtxFullHeartbeatPtr context);

    NMetaState::TMutationPtr CreateIncrementalHeartbeatMutation(
        const NProto::TMetaReqIncrementalHeartbeat& request);

    DECLARE_METAMAP_ACCESSORS(Node, TNode, TNodeId);

    //! Fired when a node gets registered.
    DECLARE_SIGNAL(void(TNode* node), NodeRegistered);
    
    //! Fired when a node gets unregistered.
    DECLARE_SIGNAL(void(TNode* node), NodeUnregistered);

    //! Fired when a full heartbeat is received from a node.
    DECLARE_SIGNAL(void(TNode* node, const NProto::TMetaReqFullHeartbeat& request), FullHeartbeat);

    //! Fired when an incremental heartbeat is received from a node.
    DECLARE_SIGNAL(void(TNode* node, const NProto::TMetaReqIncrementalHeartbeat& request), IncrementalHeartbeat);


    //! Returns a node registered at the given address (|nullptr| if none).
    TNode* FindNodeByAddress(const Stroka& address);

    //! Returns a node registered at the given address (fails if none).
    TNode* GetNodeByAddress(const Stroka& address);

    //! Returns an arbitrary node registered at the host (|nullptr| if none).
    TNode* FindNodeByHostName(const Stroka& hostName);

    //! Returns a node with a given id (throws if none).
    TNode* GetNodeOrThrow(TNodeId id);


    NNodeTrackerClient::TTotalNodeStatistics GetTotalNodeStatistics();

    //! Returns the number of nodes in |Registered| state.
    int GetRegisteredNodeCount();

    //! Returns the number of nodes in |Online| state.
    int GetOnlineNodeCount();

private:
    class TImpl;
    
    TIntrusivePtr<TImpl> Impl;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NNodeTrackerServer
} // namespace NYT
