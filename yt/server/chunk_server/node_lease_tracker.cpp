#include "stdafx.h"
#include "node_lease_tracker.h"
#include "chunk_manager.h"
#include "node.h"

#include <server/cell_master/bootstrap.h>
#include <server/cell_master/config.h>
#include <server/cell_master/meta_state_facade.h>

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
    YCHECK(config);
    YCHECK(bootstrap);
}

void TNodeLeaseTracker::OnNodeRegistered(const TDataNode* node, bool recovery)
{
    auto metaStateFacade = Bootstrap->GetMetaStateFacade();
    TNodeInfo nodeInfo;
    nodeInfo.Confirmed = !recovery;
    nodeInfo.Lease = TLeaseManager::CreateLease(
        GetTimeout(node, nodeInfo),
        BIND(&TNodeLeaseTracker::OnExpired, MakeStrong(this), node->GetId())
            .Via(metaStateFacade->GetUnguardedEpochInvoker(EStateThreadQueue::ChunkRefresh)));
    YCHECK(NodeInfoMap.insert(MakePair(node->GetId(), nodeInfo)).second);
}

void TNodeLeaseTracker::OnNodeOnline(const TDataNode* node, bool recovery)
{
    auto& nodeInfo = GetNodeInfo(node->GetId());
    nodeInfo.Confirmed = !recovery;
    RenewLease(node, nodeInfo);
    YCHECK(node->GetState() == ENodeState::Online);
    ++OnlineNodeCount;
}

void TNodeLeaseTracker::OnNodeUnregistered(const TDataNode* node)
{
    auto nodeId = node->GetId();
    auto& nodeInfo = GetNodeInfo(nodeId);
    TLeaseManager::CloseLease(nodeInfo.Lease);
    YVERIFY(NodeInfoMap.erase(nodeId) == 1);
    if (node->GetState() == ENodeState::Online) {
        --OnlineNodeCount;
    }
}

void TNodeLeaseTracker::OnNodeHeartbeat(const TDataNode* node)
{
    auto& nodeInfo = GetNodeInfo(node->GetId());
    nodeInfo.Confirmed = true;
    RenewLease(node, nodeInfo);
}

bool TNodeLeaseTracker::IsNodeConfirmed(const TDataNode* node)
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
        ->OnSuccess(BIND([=] () {
            LOG_INFO("Node expiration commit success (NodeId: %d)", nodeId);
        }))
        ->OnError(BIND([=] (const TError& error) {
            LOG_ERROR("Node expiration commit failed (NodeId: %d)\n%s",
                nodeId,
                ~error.ToString());
        }))
        ->Commit();
}

TDuration TNodeLeaseTracker::GetTimeout(const TDataNode* node, const TNodeInfo& nodeInfo)
{
    if (!nodeInfo.Confirmed) {
        return Config->UnconfirmedNodeTimeout;
    } else if (node->GetState() == ENodeState::Registered) {
        return Config->RegisteredNodeTimeout;
    } else {
        return Config->OnlineNodeTimeout;
    }
}

void TNodeLeaseTracker::RenewLease(const TDataNode* node, const TNodeInfo& nodeInfo)
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
