#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/server/master/incumbent_server/config.h>
#include <yt/yt/server/master/incumbent_server/scheduler.h>

namespace NYT::NIncumbentServer {
namespace {

using namespace NHydra;
using namespace NIncumbentClient;

////////////////////////////////////////////////////////////////////////////////

class TIncumbentSchedulerTest
    : public ::testing::Test
{
public:
    TIncumbentSchedulerConfigPtr CreateConfig() const
    {
        auto config = New<TIncumbentSchedulerConfig>();
        for (auto incumbentType : TEnumTraits<EIncumbentType>::GetDomainValues()) {
            config->Incumbents[incumbentType] = New<TIncumbentSchedulingConfig>();
        }

        return config;
    }

    std::vector<TPeerDescriptor> CreatePeerDescriptors(int peerCount = 5) const
    {
        std::vector<TPeerDescriptor> descriptors;
        descriptors.reserve(peerCount);
        for (int peerIndex = 0; peerIndex < peerCount; ++peerIndex) {
            descriptors.push_back(TPeerDescriptor{
                .Address = ToString(peerIndex),
                .State = EPeerState::Following,
            });
        }

        return descriptors;
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TIncumbentSchedulerTest, TestScheduleToLeader)
{
    auto config = CreateConfig();
    config->Incumbents[EIncumbentType::ChunkReplicator]->UseFollowers = false;

    auto peers = CreatePeerDescriptors();
    peers[3].State = EPeerState::Leading;

    auto incumbentMap = ScheduleIncumbents(config, peers);

    for (auto& address : incumbentMap[EIncumbentType::ChunkReplicator].Addresses) {
        EXPECT_EQ("3", address);
    }
}

TEST_F(TIncumbentSchedulerTest, TestScheduleToFollowers)
{
    auto config = CreateConfig();
    config->Incumbents[EIncumbentType::ChunkReplicator]->UseFollowers = true;

    auto peers = CreatePeerDescriptors();
    peers[3].State = EPeerState::Leading;

    auto incumbentMap = ScheduleIncumbents(config, peers);

    THashMap<TString, int> peerToCount;
    for (auto& address : incumbentMap[EIncumbentType::ChunkReplicator].Addresses) {
        EXPECT_FALSE(address == "3");
        peerToCount[*address]++;
    }

    for (const auto& [peer, count] : peerToCount) {
        EXPECT_EQ(count, 15);
    }
}

TEST_F(TIncumbentSchedulerTest, TestOrphanedDistribution)
{
    auto config = CreateConfig();
    config->Incumbents[EIncumbentType::ChunkReplicator]->UseFollowers = true;

    auto peers = CreatePeerDescriptors();
    peers[3].State = EPeerState::Leading;

    auto stableIncumbentMap = ScheduleIncumbents(config, peers);

    peers[1].State = EPeerState::None;
    peers[4].State = EPeerState::None;

    auto incumbentMap = ScheduleIncumbents(config, peers);

    THashMap<TString, int> peerToCount;
    auto shardCount = GetIncumbentShardCount(EIncumbentType::ChunkReplicator);
    for (int peerIndex = 0; peerIndex < shardCount; ++peerIndex) {
        auto stableAddress = stableIncumbentMap[EIncumbentType::ChunkReplicator].Addresses[peerIndex];
        auto address = incumbentMap[EIncumbentType::ChunkReplicator].Addresses[peerIndex];
        EXPECT_TRUE(address == "0" || address == "2");

        if (stableAddress == "0" || stableAddress == "2") {
            EXPECT_EQ(stableAddress, address);
        }

        peerToCount[*address]++;
    }

    for (const auto& [peer, count] : peerToCount) {
        EXPECT_EQ(count, 30);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NIncumbentServer
