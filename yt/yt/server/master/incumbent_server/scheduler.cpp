#include "scheduler.h"

#include "config.h"

namespace NYT::NIncumbentServer {

using namespace NIncumbentClient;
using namespace NHydra;

////////////////////////////////////////////////////////////////////////////////

TIncumbentMap ScheduleIncumbents(
    const TIncumbentSchedulerConfigPtr& config,
    const std::vector<TPeerDescriptor>& peers)
{
    int aliveFollowerCount = 0;
    for (const auto& peer : peers) {
        YT_VERIFY(
            peer.State == EPeerState::Leading ||
            peer.State == EPeerState::Following ||
            peer.State == EPeerState::None);

        if (peer.State == EPeerState::Following) {
            ++aliveFollowerCount;
        }
    }

    auto canUseFollowers = aliveFollowerCount >= config->MinAliveFollowers;

    auto getWeight = [&] (TIncumbencyDescriptor incumbency) {
        return config->Incumbents[incumbency.Type]->Weight;
    };

    // Peer index to list of incumbencies.
    using TDistribution = std::vector<std::vector<TIncumbencyDescriptor>>;

    auto doSchedule = [&] (
        std::vector<TIncumbencyDescriptor> incumbencies,
        bool useOfflineFollowers) -> TDistribution
    {
        TDistribution result(peers.size());
        std::vector<i64> assignedWeight(peers.size());

        SortBy(incumbencies, [&] (const TIncumbencyDescriptor& incumbency) {
            return -getWeight(incumbency);
        });

        for (auto incumbency : incumbencies) {
            std::optional<int> bestPeerIndex;
            for (int peerIndex = 0; peerIndex < std::ssize(peers); ++peerIndex) {
                auto peerState = peers[peerIndex].State;

                bool feasible = false;
                if (config->Incumbents[incumbency.Type]->UseFollowers) {
                    feasible = (
                        std::ssize(peers) == 1 ||
                        (canUseFollowers && peerState == EPeerState::Following) ||
                        (canUseFollowers && useOfflineFollowers && peerState == EPeerState::None));
                } else {
                    feasible = peerState == EPeerState::Leading;
                }

                if (feasible && (!bestPeerIndex || assignedWeight[peerIndex] < assignedWeight[*bestPeerIndex])) {
                    bestPeerIndex = peerIndex;
                }
            }

            if (bestPeerIndex) {
                result[*bestPeerIndex].push_back(incumbency);
                assignedWeight[*bestPeerIndex] += getWeight(incumbency);
            }
        }

        return result;
    };

    std::vector<TIncumbencyDescriptor> allIncumbencies;
    for (auto incumbentType : TEnumTraits<EIncumbentType>::GetDomainValues()) {
        auto shardCount = GetIncumbentShardCount(incumbentType);
        for (int shardIndex = 0; shardIndex < shardCount; ++shardIndex) {
            allIncumbencies.push_back(TIncumbencyDescriptor{
                .Type = incumbentType,
                .ShardIndex = shardIndex
            });
        }
    }

    auto stableDistribution = doSchedule(allIncumbencies, /*useOfflineFollowers*/ true);

    std::vector<TIncumbencyDescriptor> orphanedIncumbencies;
    for (int peerIndex = 0; peerIndex < std::ssize(peers); ++peerIndex) {
        if (peers[peerIndex].State == EPeerState::None) {
            const auto& peerIncumbencies = stableDistribution[peerIndex];
            orphanedIncumbencies.insert(
                orphanedIncumbencies.end(),
                peerIncumbencies.begin(),
                peerIncumbencies.end());
        }
    }

    auto orphanedDistribution = doSchedule(orphanedIncumbencies, /*useOfflineFollowers*/ false);

    auto result = CreateEmptyIncumbentMap();
    auto processDistribution = [&] (const TDistribution& distribution) {
        for (int peerIndex = 0; peerIndex < std::ssize(peers); ++peerIndex) {
            if (peers[peerIndex].State != EPeerState::None) {
                for (auto incumbency : distribution[peerIndex]) {
                    result[incumbency.Type].Addresses[incumbency.ShardIndex] = peers[peerIndex].Address;
                }
            }
        }
    };
    processDistribution(stableDistribution);
    processDistribution(orphanedDistribution);

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIncumbentServer
