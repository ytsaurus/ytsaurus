#include "common.h"

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/orm/client/native/peer_discovery.h>

namespace NYT::NOrm::NClient::NNative::NTests {
namespace {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

constexpr auto SomeIP6Addresses = std::to_array<TStringBuf>({
    "[2a02:6b8:c11:238a:10d:29a2:844a:0]:8090",
    "[2a02:6b8:c0d:208c:10d:29a2:8bcf:0]:8090",
    "[2a02:6b8:c42:854a:10d:29a2:253d:0]:8090",
    "[2a02:6b8:c0d:c1a:10d:29a2:e15f:0]:8090",
});

////////////////////////////////////////////////////////////////////////////////

class TPeerDiscoveryTestSuite
    : public ::testing::Test
{
public:
    NLogging::TLogger Logger;

    NRpc::TPeerDiscoveryResponse Discover(IOrmPeerDiscoveryPtr peerDiscovery, const TString& address)
    {
        return WaitFor(peerDiscovery->Discover(
            /*channel*/ ChannelFactory_->CreateChannel(address),
            address,
            /*timeout*/ TDuration::Zero(),
            /*replyDelay*/ TDuration::Zero(),
            /*serviceName*/ ""))
            .ValueOrThrow();
    }

    void RedeployMasters()
    {
        InitializeSimulator(std::vector<TMasterInfo>{
            TMasterInfo {
                .GrpcAddress = "master1",
                .GrpcIP6Address = TString(SomeIP6Addresses[2]),
                .InstanceTag = TMasterInstanceTag{2},
            },
            TMasterInfo {
                .GrpcAddress = "master2",
                .GrpcIP6Address = TString(SomeIP6Addresses[3]),
                .InstanceTag = TMasterInstanceTag{1},
            }
        });
    }

private:
    NRpc::IChannelFactoryPtr ChannelFactory_ = MakeFakeChannelFactory();

    void SetUp() override
    {
        InitializeSimulator(std::vector<TMasterInfo>{
            TMasterInfo {
                .GrpcAddress = "master1",
                .GrpcIP6Address = TString(SomeIP6Addresses[0]),
                .InstanceTag = TMasterInstanceTag{1},
            },
            TMasterInfo {
                .GrpcAddress = "master2",
                .GrpcIP6Address = TString(SomeIP6Addresses[1]),
                .InstanceTag = TMasterInstanceTag{2},
            }
        });
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TPeerDiscoveryTestSuite, BalancerIsNotViable)
{
    auto peerDiscovery = CreateOrmPeerDiscovery<TFakeDiscoveryServiceProxy>(Logger, "balancer");
    ASSERT_FALSE(Discover(peerDiscovery, "balancer").IsUp);
}

TEST_F(TPeerDiscoveryTestSuite, DoNotBanSingleMaster)
{
    InitializeSimulator(std::vector<TMasterInfo>{
        TMasterInfo {
            .GrpcAddress = "master",
            .GrpcIP6Address = TString(SomeIP6Addresses[1]),
            .InstanceTag = TMasterInstanceTag{1},
        },
    });

    auto peerDiscovery = CreateOrmPeerDiscovery<TFakeDiscoveryServiceProxy>(Logger, "master");
    ASSERT_TRUE(Discover(peerDiscovery, "master").IsUp);
}

TEST_F(TPeerDiscoveryTestSuite, IP6AddressCache)
{
    auto peerDiscovery = CreateOrmPeerDiscovery<TFakeDiscoveryServiceProxy>(Logger, "balancer");

    // Initially, none of the masters is known to the client.
    ASSERT_FALSE(peerDiscovery->ResolveIP6Address("master1"));
    ASSERT_FALSE(peerDiscovery->ResolveIP6Address("master2"));

    // After first discovery, masters and their ip6 addresses are cached.
    Discover(peerDiscovery, "balancer");
    ASSERT_EQ(SomeIP6Addresses[0], peerDiscovery->ResolveIP6Address("master1"));
    ASSERT_EQ(SomeIP6Addresses[1], peerDiscovery->ResolveIP6Address("master2"));

    // When masters are redeployed, cache is not immediately invalidated,
    // thus stale master ip6 addresses are returned.
    RedeployMasters();
    ASSERT_EQ(SomeIP6Addresses[0], peerDiscovery->ResolveIP6Address("master1"));
    ASSERT_EQ(SomeIP6Addresses[1], peerDiscovery->ResolveIP6Address("master2"));

    // Upon second discovery, cache is updated and master ip6 addresses are up-to-date.
    Discover(peerDiscovery, "balancer");
    ASSERT_EQ(SomeIP6Addresses[2], peerDiscovery->ResolveIP6Address("master1"));
    ASSERT_EQ(SomeIP6Addresses[3], peerDiscovery->ResolveIP6Address("master2"));
}

TEST_F(TPeerDiscoveryTestSuite, InstanceTagCache)
{
    auto peerDiscovery = CreateOrmPeerDiscovery<TFakeDiscoveryServiceProxy>(Logger, "balancer");

    // Initially, none of the masters is known to the client.
    ASSERT_FALSE(peerDiscovery->GetAddress(TMasterInstanceTag{1}));
    ASSERT_FALSE(peerDiscovery->GetAddress(TMasterInstanceTag{2}));

    // After first discovery, masters and their instance tags are cached.
    Discover(peerDiscovery, "balancer");
    ASSERT_EQ("master1", peerDiscovery->GetAddress(TMasterInstanceTag{1}));
    ASSERT_EQ("master2", peerDiscovery->GetAddress(TMasterInstanceTag{2}));

    // When masters are redeployed, cache is not immediately invalidated,
    // thus stale master addresses are returned.
    RedeployMasters();
    ASSERT_EQ("master1", peerDiscovery->GetAddress(TMasterInstanceTag{1}));
    ASSERT_EQ("master2", peerDiscovery->GetAddress(TMasterInstanceTag{2}));

    // Upon second discovery, cache is updated and master addresses are up-to-date.
    Discover(peerDiscovery, "balancer");
    ASSERT_EQ("master2", peerDiscovery->GetAddress(TMasterInstanceTag{1}));
    ASSERT_EQ("master1", peerDiscovery->GetAddress(TMasterInstanceTag{2}));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

} // namespace NYT::NOrm::NClient::NNative::NTests
