#pragma once

#include <yt/yt/core/misc/common.h>

namespace NYT::NCellServer {

////////////////////////////////////////////////////////////////////////////////

struct TCellPeerInfo
{
    // Opaque index identifying the cell (maps to a TCellBase* in the caller).
    int CellIndex = -1;
    int LeaderNodeId = -1;
    int LeadingPeerId = -1;
    // Non-leader peers eligible to become the new leader: {peerId, nodeId}.
    std::vector<std::pair<int, int>> FollowerPeers;
};

struct TLeaderReassignment
{
    int CellIndex = -1;
    int NewLeadingPeerId = -1;
};

////////////////////////////////////////////////////////////////////////////////

// Computes leader reassignments to balance leaders across eligible nodes.
// Uses multi-hop BFS chains to handle cases where a direct move is impossible
// due to peer placement constraints.
std::vector<TLeaderReassignment> ComputeLeaderSmoothing(
    const std::vector<TCellPeerInfo>& cells,
    const std::vector<int>& eligibleNodes);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
