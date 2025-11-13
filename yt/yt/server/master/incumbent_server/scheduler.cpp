#include "scheduler.h"

#include "config.h"

namespace NYT::NIncumbentServer {

using namespace NIncumbentClient;
using namespace NHydra;

////////////////////////////////////////////////////////////////////////////////

constexpr int TypicalPeerCount = 9;

TIncumbentMap ScheduleIncumbents(
    const TIncumbentSchedulerConfigPtr& config,
    const std::vector<TPeerDescriptor>& peers)
{
    std::string leaderAddress;
    TCompactVector<std::string, TypicalPeerCount> aliveFollowerAddresses;
    TCompactVector<TPeerDescriptor, TypicalPeerCount> followerDescriptors;
    for (const auto& peer : peers) {
        YT_VERIFY(
            peer.State == EPeerState::Leading ||
            peer.State == EPeerState::Following ||
            peer.State == EPeerState::None);

        if (peer.State == EPeerState::Leading) {
            leaderAddress = peer.Address;
        } else if (peer.State == EPeerState::Following) {
            aliveFollowerAddresses.push_back(peer.Address);
            followerDescriptors.push_back(peer);
        } else {
            followerDescriptors.push_back(peer);
        }
    }

    auto aliveFollowerCount = std::ssize(aliveFollowerAddresses);

    auto result = CreateEmptyIncumbentMap();
    auto scheduleToLeader = [&] (EIncumbentType incumbentType) {
        auto shardCount = GetIncumbentShardCount(incumbentType);
        for (int shardIndex = 0; shardIndex < shardCount; ++shardIndex) {
            result[incumbentType].Addresses[shardIndex] = leaderAddress;
        }
    };

    auto tryScheduleToFollowers = [&] (EIncumbentType incumbentType) {
        auto shardCount = GetIncumbentShardCount(incumbentType);

        std::vector<int> orphanedShardIndices;
        for (int shardIndex = 0; shardIndex < shardCount; ++shardIndex) {
            const auto& stablePeer = followerDescriptors[shardIndex % std::ssize(followerDescriptors)];
            if (stablePeer.State == EPeerState::Following) {
                result[incumbentType].Addresses[shardIndex] = stablePeer.Address;
            } else {
                orphanedShardIndices.push_back(shardIndex);
            }
        }

        return orphanedShardIndices;
    };

    // There is a possible load difference between peers if the amount of incumbencies is not
    // divisible by the amount of peers alive. This offset tries to remedy this.
    // The other option is to reintroduce incumbency weights, but only use them for placing
    // orphaned shards. For now this feels like an overkill.
    int peerIndexOffset = 0;
    for (auto incumbentType : TEnumTraits<EIncumbentType>::GetDomainValues()) {
        if(!config->Incumbents[incumbentType]->UseFollowers || aliveFollowerCount < config->MinAliveFollowers) {
            scheduleToLeader(incumbentType);
            continue;
        }

        auto orphanedShardIndices = tryScheduleToFollowers(incumbentType);

        for (int i = 0; i < std::ssize(orphanedShardIndices); ++i) {
            int shardIndex = orphanedShardIndices[i];
            result[incumbentType].Addresses[shardIndex] = aliveFollowerAddresses[(i + peerIndexOffset) % aliveFollowerCount];
        }

        peerIndexOffset = (std::ssize(orphanedShardIndices) + peerIndexOffset) % aliveFollowerCount;
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIncumbentServer
