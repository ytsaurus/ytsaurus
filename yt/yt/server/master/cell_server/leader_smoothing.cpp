#include "leader_smoothing.h"

namespace NYT::NCellServer {

////////////////////////////////////////////////////////////////////////////////

std::vector<TLeaderReassignment> ComputeLeaderSmoothing(
    const std::vector<TCellPeerInfo>& cells,
    const std::vector<int>& eligibleNodes)
{
    if (eligibleNodes.empty() || cells.empty()) {
        return {};
    }

    int nodeCount = std::ssize(eligibleNodes);
    int totalLeaders = std::ssize(cells);
    int minAllowed = totalLeaders / nodeCount;
    int maxAllowed = (totalLeaders + nodeCount - 1) / nodeCount;

    THashMap<int, int> nodeLeaderCount;
    for (int nodeId : eligibleNodes) {
        nodeLeaderCount[nodeId] = 0;
    }
    for (const auto& cell : cells) {
        if (nodeLeaderCount.contains(cell.LeaderNodeId)) {
            nodeLeaderCount[cell.LeaderNodeId]++;
        }
    }

    // Early exit: if no node is over- or underloaded, nothing to do.
    {
        bool anyOverloaded = false;
        bool anyUnderloaded = false;
        for (const auto& [nodeId, count] : nodeLeaderCount) {
            if (count > maxAllowed) {
                anyOverloaded = true;
            } else if (count < minAllowed) {
                anyUnderloaded = true;
            }
        }
        if (!anyOverloaded || !anyUnderloaded) {
            return {};
        }
    }

    // Track current leader node id and peer id per cell (updated as moves are applied).
    std::vector<int> currentLeaderNodeId(cells.size());
    std::vector<int> currentLeadingPeerId(cells.size());
    for (int cellIndex = 0; cellIndex < std::ssize(cells); ++cellIndex) {
        currentLeaderNodeId[cellIndex] = cells[cellIndex].LeaderNodeId;
        currentLeadingPeerId[cellIndex] = cells[cellIndex].LeadingPeerId;
    }

    // Follower map per cell: nodeId -> peerId. Updated as moves are applied.
    std::vector<THashMap<int, int>> cellFollowerMap(cells.size());
    for (int cellIndex = 0; cellIndex < std::ssize(cells); ++cellIndex) {
        for (const auto& [peerId, nodeId] : cells[cellIndex].FollowerPeers) {
            cellFollowerMap[cellIndex][nodeId] = peerId;
        }
    }

    std::vector<TLeaderReassignment> results;

    // Each iteration finds and applies one BFS chain, reducing total imbalance by 2.
    // Total imbalance is bounded by 2 * totalLeaders, so this terminates.
    // A safety guard of 1000 iterations is added to prevent infinite loops in case of bugs.
    constexpr int MaxIterations = 1000;
    for (int iteration = 0; iteration < MaxIterations; ++iteration) {
        THashSet<int> overloaded;
        THashSet<int> underloaded;
        for (const auto& [nodeId, count] : nodeLeaderCount) {
            if (count > maxAllowed) {
                overloaded.insert(nodeId);
            } else if (count < minAllowed) {
                underloaded.insert(nodeId);
            }
        }

        if (overloaded.empty() || underloaded.empty()) {
            break;
        }

        bool found = false;
        for (int targetNodeId : underloaded) {
            // BFS backwards from |targetNodeId| through the follower-relationship graph.
            // An edge exists from nodeA to nodeB if some cell led by nodeA has a follower on nodeB.
            // We search for a path from an overloaded node to |targetNodeId|.
            struct TParentInfo
            {
                int ParentNodeId;
                int CellIndex;
                int PeerIdOnParent;
            };

            THashMap<int, TParentInfo> parent;
            THashSet<int> visited;
            std::deque<int> queue;

            visited.insert(targetNodeId);
            queue.push_back(targetNodeId);

            int foundSourceNodeId = -1;
            bool foundSource = false;

            while (!queue.empty() && !foundSource) {
                int currentNodeId = queue.front();
                queue.pop_front();

                for (int cellIndex = 0; cellIndex < std::ssize(cells); ++cellIndex) {
                    int leaderNodeId = currentLeaderNodeId[cellIndex];
                    if (!nodeLeaderCount.contains(leaderNodeId)) {
                        continue;
                    }
                    if (leaderNodeId == currentNodeId || visited.contains(leaderNodeId)) {
                        continue;
                    }

                    auto it = cellFollowerMap[cellIndex].find(currentNodeId);
                    if (it == cellFollowerMap[cellIndex].end()) {
                        continue;
                    }

                    if (!parent.contains(leaderNodeId)) {
                        parent[leaderNodeId] = {currentNodeId, cellIndex, it->second};
                    }

                    if (overloaded.contains(leaderNodeId)) {
                        foundSourceNodeId = leaderNodeId;
                        foundSource = true;
                        break;
                    }

                    // Intermediate nodes in a chain have net-zero count change,
                    // so traversal is safe for any node at or above minAllowed.
                    if (nodeLeaderCount[leaderNodeId] >= minAllowed) {
                        visited.insert(leaderNodeId);
                        queue.push_back(leaderNodeId);
                    }
                }
            }

            if (!foundSource) {
                continue;
            }

            // Reconstruct the chain from foundSourceNodeId to targetNodeId and apply all moves.
            // chainMoves[k] = {cellIndex, newLeadingPeerId}
            // chainNodes[k] = {fromNodeId, toNodeId}
            std::vector<std::pair<int, int>> chainMoves;
            std::vector<std::pair<int, int>> chainNodes;

            for (int curNodeId = foundSourceNodeId; curNodeId != targetNodeId; ) {
                const auto& parentInfo = parent[curNodeId];
                chainMoves.push_back({parentInfo.CellIndex, parentInfo.PeerIdOnParent});
                chainNodes.push_back({curNodeId, parentInfo.ParentNodeId});
                curNodeId = parentInfo.ParentNodeId;
            }

            for (int moveIndex = 0; moveIndex < std::ssize(chainMoves); ++moveIndex) {
                int cellIndex = chainMoves[moveIndex].first;
                int newPeerId = chainMoves[moveIndex].second;
                int fromNodeId = chainNodes[moveIndex].first;
                int toNodeId = chainNodes[moveIndex].second;

                int oldLeadingPeerId = currentLeadingPeerId[cellIndex];

                // Update follower map: toNode is no longer a follower, fromNode becomes one.
                cellFollowerMap[cellIndex].erase(toNodeId);
                cellFollowerMap[cellIndex][fromNodeId] = oldLeadingPeerId;

                currentLeaderNodeId[cellIndex] = toNodeId;
                currentLeadingPeerId[cellIndex] = newPeerId;

                nodeLeaderCount[fromNodeId]--;
                nodeLeaderCount[toNodeId]++;

                results.push_back({cellIndex, newPeerId});
            }

            found = true;
            break;
        }

        if (!found) {
            break;
        }
    }

    return results;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellServer
