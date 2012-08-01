#include "stdafx.h"
#include "holder_lease_tracker.h"
#include "chunk_manager.h"
#include "holder.h"

#include <ytlib/actions/bind.h>
#include <ytlib/cell_master/bootstrap.h>
#include <ytlib/cell_master/config.h>

namespace NYT {
namespace NChunkServer {

using namespace NProto;
using namespace NCellMaster;
using namespace NMetaState;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("ChunkServer");

////////////////////////////////////////////////////////////////////////////////

TNodeLeaseTracker::TNodeLeaseTracker(
    TChunkManagerConfigPtr config,
    TBootstrap* bootstrap)
    : Config(config)
    , Bootstrap(bootstrap)
    , OnlineNodeCount(0)
{
    YASSERT(config);
    YASSERT(bootstrap);
}

void TNodeLeaseTracker::OnNodeRegistered(const THolder* node, bool recovery)
{
    TNodeInfo nodeInfo;
    nodeInfo.Confirmed = !recovery;
    nodeInfo.Lease = TLeaseManager::CreateLease(
        GetTimeout(node, nodeInfo),
        BIND(
            &TNodeLeaseTracker::OnExpired,
            MakeStrong(this),
            node->GetId())
        .Via(
            Bootstrap->GetStateInvoker(EStateThreadQueue::ChunkRefresh),
            Bootstrap->GetMetaStateManager()->GetEpochContext()));
    YCHECK(HolderInfoMap.insert(MakePair(node->GetId(), holderInfo)).second);
}

void TNodeLeaseTracker::OnNodeOnline(const THolder* node, bool recovery)
{
    auto& nodeInfo = GetNodeInfo(node->GetId());
    nodeInfo.Confirmed = !recovery;
    RenewLease(node, nodeInfo);
    YASSERT(node->GetState() == ENodeState::Online);
    ++OnlineNodeCount;
}

void TNodeLeaseTracker::OnNodeUnregistered(const THolder* node)
{
    auto nodeId = node->GetId();
    auto& nodeInfo = GetNodeInfo(nodeId);
    TLeaseManager::CloseLease(nodeInfo.Lease);
    YVERIFY(NodeInfoMap.erase(nodeId) == 1);
    if (node->GetState() == ENodeState::Online) {
        --OnlineNodeCount;
    }
}

void TNodeLeaseTracker::OnNodeHeartbeat(const THolder* node)
{
    auto& nodeInfo = GetNodeInfo(node->GetId());
    nodeInfo.Confirmed = true;
    RenewLease(node, nodeInfo);
}

bool TNodeLeaseTracker::IsNodeConfirmed(const THolder* node)
{
    const auto& nodeInfo = GetNodeInfo(node->GetId());
    return nodeInfo.Confirmed;
}

int TNodeLeaseTracker::GetOnlineNodeCount()
{
    return OnlineNodeCount;
}

void TNodeLeaseTracker::OnExpired(TNodeId nodeId)
{
    // Check if the node is still registered.
    auto* nodeInfo = FindNodeInfo(nodeId);
    if (!nodeInfo)
        return;

    LOG_INFO("Node lease expired (NodeId: %d)", nodeId);

    TMetaReqUnregisterNode message;
    message.set_node_id(nodeId);
    Bootstrap
        ->GetChunkManager()
        ->CreateUnregisterNodeMutation(message)
        ->SetRetriable(Config->NodeExpirationBackoffTime)
        ->OnSuccess(BIND([=] () {
            LOG_INFO("Node expiration commit success (NodeId: %d)", nodeId);
        }))
        ->OnError(BIND([=] (const TError& error) {
            LOG_INFO("Node expiration commit failed (NodeId: %d)\n%s",
                nodeId,
                ~error.ToString());
        }))
        ->Commit();
}

TDuration TNodeLeaseTracker::GetTimeout(const THolder* node, const TNodeInfo& nodeInfo)
{
    if (!nodeInfo.Confirmed) {
        return Config->UnconfirmedNodeTimeout;
    } else if (node->GetState() == ENodeState::Registered) {
        return Config->RegisteredNodeTimeout;
    } else {
        return Config->OnlineNodeTimeout;
    }
}

void TNodeLeaseTracker::RenewLease(const THolder* node, const TNodeInfo& nodeInfo)
{
    TLeaseManager::RenewLease(
        nodeInfo.Lease,
        GetTimeout(node, nodeInfo));
}

TNodeLeaseTracker::TNodeInfo* TNodeLeaseTracker::FindNodeInfo(TNodeId nodeId)
{
    auto it = NodeInfoMap.find(nodeId);
    return it == NodeInfoMap.end() ? NULL : &it->second;
}

TNodeLeaseTracker::TNodeInfo& TNodeLeaseTracker::GetNodeInfo(TNodeId nodeId)
{
    auto it = NodeInfoMap.find(nodeId);
    YASSERT(it != NodeInfoMap.end());
    return it->second;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkServer
} // namespace NYT
