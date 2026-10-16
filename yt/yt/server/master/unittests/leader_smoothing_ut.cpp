#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/master/cell_server/leader_smoothing.h>

#include <util/generic/hash.h>

#include <algorithm>
#include <numeric>
#include <random>

namespace NYT::NCellServer {
namespace {

////////////////////////////////////////////////////////////////////////////////

THashMap<int, int> ApplyAndCount(
    std::vector<TCellPeerInfo> cells,
    const std::vector<int>& eligibleNodes,
    const std::vector<TLeaderReassignment>& reassignments)
{
    for (const auto& reassignment : reassignments) {
        auto& cell = cells[reassignment.CellIndex];
        int newLeaderNodeId = -1;
        for (const auto& [peerId, nodeId] : cell.FollowerPeers) {
            if (peerId == reassignment.NewLeadingPeerId) {
                newLeaderNodeId = nodeId;
                break;
            }
        }
        EXPECT_NE(newLeaderNodeId, -1);

        int oldLeadingPeerId = cell.LeadingPeerId;
        int oldLeaderNodeId = cell.LeaderNodeId;

        cell.LeadingPeerId = reassignment.NewLeadingPeerId;
        cell.LeaderNodeId = newLeaderNodeId;
        cell.FollowerPeers.erase(
            std::remove_if(cell.FollowerPeers.begin(), cell.FollowerPeers.end(),
                [&] (const auto& peer) { return peer.first == reassignment.NewLeadingPeerId; }),
            cell.FollowerPeers.end());
        cell.FollowerPeers.push_back({oldLeadingPeerId, oldLeaderNodeId});
    }

    THashMap<int, int> counts;
    for (int nodeId : eligibleNodes) {
        counts[nodeId] = 0;
    }
    for (const auto& cell : cells) {
        if (counts.contains(cell.LeaderNodeId)) {
            counts[cell.LeaderNodeId]++;
        }
    }
    return counts;
}

bool IsBalanced(
    const THashMap<int, int>& counts,
    int totalCells,
    int nodeCount)
{
    int minAllowed = totalCells / nodeCount;
    int maxAllowed = (totalCells + nodeCount - 1) / nodeCount;
    for (const auto& [nodeId, count] : counts) {
        if (count < minAllowed || count > maxAllowed) {
            return false;
        }
    }
    return true;
}

////////////////////////////////////////////////////////////////////////////////

// Node IDs used in tests: 1=A, 2=B, 3=C, 4=D
constexpr int NodeA = 1;
constexpr int NodeB = 2;
constexpr int NodeC = 3;
constexpr int NodeD = 4;

TEST(TLeaderSmoothingTest, AlreadyBalanced)
{
    std::vector<int> nodes = {NodeA, NodeB, NodeC, NodeD};
    std::vector<TCellPeerInfo> cells;
    for (int cellIndex = 0; cellIndex < 3; ++cellIndex) {
        cells.push_back({cellIndex, NodeA, 0, {{1, NodeB}}});
    }
    for (int cellIndex = 3; cellIndex < 6; ++cellIndex) {
        cells.push_back({cellIndex, NodeB, 0, {{1, NodeC}}});
    }
    for (int cellIndex = 6; cellIndex < 9; ++cellIndex) {
        cells.push_back({cellIndex, NodeC, 0, {{1, NodeD}}});
    }
    for (int cellIndex = 9; cellIndex < 12; ++cellIndex) {
        cells.push_back({cellIndex, NodeD, 0, {{1, NodeA}}});
    }

    EXPECT_TRUE(ComputeLeaderSmoothing(cells, nodes).empty());
}

TEST(TLeaderSmoothingTest, SimpleDirectMove)
{
    std::vector<int> nodes = {NodeA, NodeB};
    std::vector<TCellPeerInfo> cells = {
        {0, NodeA, 0, {{1, NodeB}}},
        {1, NodeA, 0, {{1, NodeB}}},
        {2, NodeA, 0, {{1, NodeB}}},
        {3, NodeB, 0, {{1, NodeA}}},
    };

    auto result = ComputeLeaderSmoothing(cells, nodes);
    EXPECT_FALSE(result.empty());

    auto counts = ApplyAndCount(cells, nodes, result);
    EXPECT_TRUE(IsBalanced(counts, 4, 2));
}

TEST(TLeaderSmoothingTest, MultiHopRequired_BugScenario)
{
    // A=6, B=3, C=3, D=0. A's cells only have followers on B/C; D only reachable via B/C.
    std::vector<int> nodes = {NodeA, NodeB, NodeC, NodeD};
    std::vector<TCellPeerInfo> cells;
    for (int cellIndex = 0; cellIndex < 3; ++cellIndex) {
        cells.push_back({cellIndex, NodeA, 0, {{1, NodeB}}});
    }
    for (int cellIndex = 3; cellIndex < 6; ++cellIndex) {
        cells.push_back({cellIndex, NodeA, 0, {{1, NodeC}}});
    }
    for (int cellIndex = 6; cellIndex < 9; ++cellIndex) {
        cells.push_back({cellIndex, NodeB, 0, {{1, NodeD}}});
    }
    for (int cellIndex = 9; cellIndex < 12; ++cellIndex) {
        cells.push_back({cellIndex, NodeC, 0, {{1, NodeD}}});
    }

    auto result = ComputeLeaderSmoothing(cells, nodes);
    EXPECT_FALSE(result.empty());

    auto counts = ApplyAndCount(cells, nodes, result);
    EXPECT_TRUE(IsBalanced(counts, 12, 4));
    EXPECT_EQ(counts[NodeA], 3);
    EXPECT_EQ(counts[NodeD], 3);
}

TEST(TLeaderSmoothingTest, MultiHopRequired_StuckScenario)
{
    // A=4, B=3, C=3, D=2. A's cells only have followers on B/C; D only reachable via B/C.
    std::vector<int> nodes = {NodeA, NodeB, NodeC, NodeD};
    std::vector<TCellPeerInfo> cells = {
        {0,  NodeA, 0, {{1, NodeB}}},
        {1,  NodeA, 0, {{1, NodeB}}},
        {2,  NodeA, 0, {{1, NodeC}}},
        {3,  NodeA, 0, {{1, NodeC}}},
        {4,  NodeB, 0, {{1, NodeD}}},
        {5,  NodeB, 0, {{1, NodeA}}},
        {6,  NodeB, 0, {{1, NodeA}}},
        {7,  NodeC, 0, {{1, NodeD}}},
        {8,  NodeC, 0, {{1, NodeA}}},
        {9,  NodeC, 0, {{1, NodeA}}},
        {10, NodeD, 0, {{1, NodeA}}},
        {11, NodeD, 0, {{1, NodeB}}},
    };

    auto result = ComputeLeaderSmoothing(cells, nodes);
    EXPECT_FALSE(result.empty());

    auto counts = ApplyAndCount(cells, nodes, result);
    EXPECT_TRUE(IsBalanced(counts, 12, 4));
}

TEST(TLeaderSmoothingTest, SingleNode)
{
    std::vector<int> nodes = {NodeA};
    std::vector<TCellPeerInfo> cells = {
        {0, NodeA, 0, {}},
        {1, NodeA, 0, {}},
        {2, NodeA, 0, {}},
    };

    EXPECT_TRUE(ComputeLeaderSmoothing(cells, nodes).empty());
}

TEST(TLeaderSmoothingTest, TwoNodes)
{
    std::vector<int> nodes = {NodeA, NodeB};
    std::vector<TCellPeerInfo> cells = {
        {0, NodeA, 0, {{1, NodeB}}},
        {1, NodeA, 0, {{1, NodeB}}},
        {2, NodeA, 0, {{1, NodeB}}},
        {3, NodeA, 0, {{1, NodeB}}},
        {4, NodeB, 0, {{1, NodeA}}},
        {5, NodeB, 0, {{1, NodeA}}},
    };

    auto result = ComputeLeaderSmoothing(cells, nodes);
    EXPECT_FALSE(result.empty());

    auto counts = ApplyAndCount(cells, nodes, result);
    EXPECT_TRUE(IsBalanced(counts, 6, 2));
}

TEST(TLeaderSmoothingTest, AllCellsOnOneNode)
{
    std::vector<int> nodes = {NodeA, NodeB, NodeC};
    std::vector<TCellPeerInfo> cells = {
        {0, NodeA, 0, {{1, NodeB}}},
        {1, NodeA, 0, {{1, NodeB}}},
        {2, NodeA, 0, {{1, NodeC}}},
        {3, NodeA, 0, {{1, NodeC}}},
        {4, NodeA, 0, {{1, NodeB}}},
        {5, NodeA, 0, {{1, NodeC}}},
    };

    auto result = ComputeLeaderSmoothing(cells, nodes);
    EXPECT_FALSE(result.empty());

    auto counts = ApplyAndCount(cells, nodes, result);
    EXPECT_TRUE(IsBalanced(counts, 6, 3));
}

TEST(TLeaderSmoothingTest, ImpossibleToFullyBalance)
{
    // NodeC has no followers, so it can never get a leader. Algorithm must terminate.
    std::vector<int> nodes = {NodeA, NodeB, NodeC};
    std::vector<TCellPeerInfo> cells = {
        {0, NodeA, 0, {{1, NodeB}}},
        {1, NodeA, 0, {{1, NodeB}}},
        {2, NodeA, 0, {{1, NodeB}}},
    };

    auto result = ComputeLeaderSmoothing(cells, nodes);
    auto counts = ApplyAndCount(cells, nodes, result);
    EXPECT_EQ(counts[NodeA] + counts[NodeB] + counts[NodeC], 3);
    EXPECT_EQ(counts[NodeC], 0);
}

TEST(TLeaderSmoothingTest, EmptyInput)
{
    EXPECT_TRUE(ComputeLeaderSmoothing({}, {}).empty());
    EXPECT_TRUE(ComputeLeaderSmoothing({}, {NodeA, NodeB}).empty());
}

TEST(TLeaderSmoothingTest, ThreeHopChain)
{
    // A=5, B=3, C=3, D=1. Requires chain A->B->C->D.
    std::vector<int> nodes = {NodeA, NodeB, NodeC, NodeD};
    std::vector<TCellPeerInfo> cells;
    for (int cellIndex = 0; cellIndex < 5; ++cellIndex) {
        cells.push_back({cellIndex, NodeA, 0, {{1, NodeB}}});
    }
    for (int cellIndex = 5; cellIndex < 8; ++cellIndex) {
        cells.push_back({cellIndex, NodeB, 0, {{1, NodeC}}});
    }
    for (int cellIndex = 8; cellIndex < 11; ++cellIndex) {
        cells.push_back({cellIndex, NodeC, 0, {{1, NodeD}}});
    }
    cells.push_back({11, NodeD, 0, {{1, NodeA}}});

    auto result = ComputeLeaderSmoothing(cells, nodes);
    EXPECT_FALSE(result.empty());

    auto counts = ApplyAndCount(cells, nodes, result);
    EXPECT_TRUE(IsBalanced(counts, 12, 4));
}

TEST(TLeaderSmoothingTest, MultipleOverloadedAndUnderloaded)
{
    // A=4, B=4, C=0, D=0.
    std::vector<int> nodes = {NodeA, NodeB, NodeC, NodeD};
    std::vector<TCellPeerInfo> cells;
    for (int cellIndex = 0; cellIndex < 4; ++cellIndex) {
        cells.push_back({cellIndex, NodeA, 0, {{1, NodeC}}});
    }
    for (int cellIndex = 4; cellIndex < 8; ++cellIndex) {
        cells.push_back({cellIndex, NodeB, 0, {{1, NodeD}}});
    }

    auto result = ComputeLeaderSmoothing(cells, nodes);
    EXPECT_FALSE(result.empty());

    auto counts = ApplyAndCount(cells, nodes, result);
    EXPECT_TRUE(IsBalanced(counts, 8, 4));
}

TEST(TLeaderSmoothingTest, MultipleFollowers)
{
    // A=4, B=1, C=1. A's cells have followers on both B and C.
    std::vector<int> nodes = {NodeA, NodeB, NodeC};
    std::vector<TCellPeerInfo> cells = {
        {0, NodeA, 0, {{1, NodeB}, {2, NodeC}}},
        {1, NodeA, 0, {{1, NodeB}, {2, NodeC}}},
        {2, NodeA, 0, {{1, NodeB}, {2, NodeC}}},
        {3, NodeA, 0, {{1, NodeB}, {2, NodeC}}},
        {4, NodeB, 0, {{1, NodeA}}},
        {5, NodeC, 0, {{1, NodeA}}},
    };

    auto result = ComputeLeaderSmoothing(cells, nodes);
    EXPECT_FALSE(result.empty());

    auto counts = ApplyAndCount(cells, nodes, result);
    EXPECT_TRUE(IsBalanced(counts, 6, 3));
}

TEST(TLeaderSmoothingTest, ReassignmentsReferenceValidPeerIds)
{
    // A=6, B=3, C=3, D=0 with non-trivial peer IDs.
    std::vector<int> nodes = {NodeA, NodeB, NodeC, NodeD};
    std::vector<TCellPeerInfo> cells;
    for (int cellIndex = 0; cellIndex < 3; ++cellIndex) {
        cells.push_back({cellIndex, NodeA, 0, {{1, NodeB}}});
    }
    for (int cellIndex = 3; cellIndex < 6; ++cellIndex) {
        cells.push_back({cellIndex, NodeA, 0, {{2, NodeC}}});
    }
    for (int cellIndex = 6; cellIndex < 9; ++cellIndex) {
        cells.push_back({cellIndex, NodeB, 0, {{3, NodeD}}});
    }
    for (int cellIndex = 9; cellIndex < 12; ++cellIndex) {
        cells.push_back({cellIndex, NodeC, 0, {{3, NodeD}}});
    }

    auto result = ComputeLeaderSmoothing(cells, nodes);
    EXPECT_FALSE(result.empty());

    for (const auto& reassignment : result) {
        const auto& cell = cells[reassignment.CellIndex];
        bool found = false;
        for (const auto& [peerId, nodeId] : cell.FollowerPeers) {
            if (peerId == reassignment.NewLeadingPeerId) {
                found = true;
                break;
            }
        }
        EXPECT_TRUE(found) << "Invalid peer ID " << reassignment.NewLeadingPeerId
            << " for cell " << reassignment.CellIndex;
    }

    auto counts = ApplyAndCount(cells, nodes, result);
    EXPECT_TRUE(IsBalanced(counts, 12, 4));
}

TEST(TLeaderSmoothingTest, StressRandomDistribution)
{
    // peer_count=2, node_count=6, 6 slots per node => 18 cells.
    constexpr int NodeCount = 6;
    constexpr int SlotsPerNode = 6;
    constexpr int PeerCount = 2;
    constexpr int CellCount = NodeCount * SlotsPerNode / PeerCount;
    constexpr int Iterations = 100;

    uint64_t masterSeed = std::random_device{}();
    std::mt19937_64 masterRng(masterSeed);

    std::vector<int> nodes;
    for (int nodeIndex = 0; nodeIndex < NodeCount; ++nodeIndex) {
        // Use node IDs starting from 1 to mimic real node IDs.
        nodes.push_back(nodeIndex + 1);
    }

    for (int iter = 0; iter < Iterations; ++iter) {
        uint64_t seed = masterRng();
        std::mt19937 rng(static_cast<uint32_t>(seed));

        std::vector<int> nodeSlots(NodeCount, 0);
        std::vector<TCellPeerInfo> cells;
        cells.reserve(CellCount);
        bool assignmentPossible = true;

        for (int cellIndex = 0; cellIndex < CellCount; ++cellIndex) {
            std::vector<int> nodeOrder(NodeCount);
            std::iota(nodeOrder.begin(), nodeOrder.end(), 0);
            std::shuffle(nodeOrder.begin(), nodeOrder.end(), rng);

            std::vector<int> chosenNodeIndices;
            for (int nodeIndex : nodeOrder) {
                if (nodeSlots[nodeIndex] < SlotsPerNode) {
                    chosenNodeIndices.push_back(nodeIndex);
                    if (std::ssize(chosenNodeIndices) == PeerCount) {
                        break;
                    }
                }
            }

            if (std::ssize(chosenNodeIndices) < PeerCount) {
                assignmentPossible = false;
                break;
            }

            for (int nodeIndex : chosenNodeIndices) {
                nodeSlots[nodeIndex]++;
            }

            int LeadingPeerIndex = rng() % PeerCount;
            int leaderNodeIndex = chosenNodeIndices[LeadingPeerIndex];

            TCellPeerInfo info;
            info.CellIndex = cellIndex;
            info.LeaderNodeId = nodes[leaderNodeIndex];
            info.LeadingPeerId = LeadingPeerIndex;
            for (int peerIndex = 0; peerIndex < PeerCount; ++peerIndex) {
                if (peerIndex != LeadingPeerIndex) {
                    info.FollowerPeers.emplace_back(peerIndex, nodes[chosenNodeIndices[peerIndex]]);
                }
            }
            cells.push_back(std::move(info));
        }

        if (!assignmentPossible) {
            continue;
        }

        auto result = ComputeLeaderSmoothing(cells, nodes);
        auto counts = ApplyAndCount(cells, nodes, result);

        ASSERT_TRUE(IsBalanced(counts, CellCount, NodeCount))
            << "iter=" << iter << " seed=" << seed << " masterSeed=" << masterSeed;

        for (const auto& reassignment : result) {
            const auto& cell = cells[reassignment.CellIndex];
            bool found = false;
            for (const auto& [peerId, nodeId] : cell.FollowerPeers) {
                if (peerId == reassignment.NewLeadingPeerId) {
                    found = true;
                    break;
                }
            }
            ASSERT_TRUE(found)
                << "iter=" << iter << " seed=" << seed
                << ": invalid peer ID " << reassignment.NewLeadingPeerId
                << " for cell " << reassignment.CellIndex;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NCellServer
