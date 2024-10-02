#include "common.h"

#include <yt/yt/core/rpc/balancing_channel.h>
#include <yt/yt/core/rpc/config.h>
#include <yt/yt/core/rpc/roaming_channel.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt/orm/client/native/connection_impl.h>
#include <yt/yt/orm/client/native/peer_discovery.h>

#include <library/cpp/yt/logging/logger.h>

namespace NYT::NOrm::NClient::NNative::NTests {

using namespace NConcurrency;
using namespace NRpc;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TConnectionTestSuite
    : public ::testing::Test
{
public:
    NLogging::TLogger Logger;

    ISimulatorPtr GetSimulator() const
    {
        return Simulator_;
    }

    TFuture<std::vector<std::string>> SampleAddresses(IChannelPtr channel)
    {
        const int attempts = 1'000;
        std::vector<TFuture<std::string>> futures;
        futures.reserve(attempts);
        for (int i = 0; i < attempts; ++i) {
            futures.push_back(PingGetAddress(channel));
        }

        return AllSucceeded(std::move(futures));
    }

    TFuture<bool> IsAddressBanned(IChannelPtr channel, const std::string& address)
    {
        return SampleAddresses(channel).ApplyUnique(BIND([searchedAddress = address] (std::vector<std::string>&& addresses)
        {
            for (const auto& address : addresses) {
                if (address == searchedAddress) {
                    return false;
                }
            }
            return true;
        }));
    }

    void WaitForBanned(IConnectionPtr connection, const std::string& address)
    {
        WaitForPredicate([this, connection, address] {
            return WaitFor(IsAddressBanned(connection->GetChannel(/*retrying*/ false), address))
                .ValueOrThrow();
        });
    }

    void WaitForAlive(IConnectionPtr connection, const std::string& address)
    {
        WaitForPredicate([this, connection, address] {
            return !WaitFor(IsAddressBanned(connection->GetChannel(/*retrying*/ false), address))
                .ValueOrDefault(true);
        });
    }

    void WaitForAllDead(IConnectionPtr connection)
    {
        WaitForPredicate([this, connection] {
            return WaitFor(SampleAddresses(connection->GetChannel(/*retrying*/ false)))
                .GetCode() == NRpc::EErrorCode::Unavailable;
        });
    }

    IAttributeDictionaryPtr MakeEndpointAttributes()
    {
        return ConvertToAttributes(BuildYsonStringFluently()
            .BeginMap()
            .EndMap());
    }

    TDynamicChannelPoolConfigPtr MakeDynamicChannelPoolConfig()
    {
        auto config = New<TDynamicChannelPoolConfig>();
        config->RediscoverPeriod = TDuration::Seconds(1);
        config->RediscoverSplay = TDuration::Seconds(1);
        config->HardBackoffTime = TDuration::Seconds(5);
        config->SoftBackoffTime = TDuration::Seconds(3);
        config->Postprocess();
        return config;
    }

    TConnectionConfigPtr MakeConnectionConfig()
    {
        auto connectionConfig = New<TConnectionConfig>();
        connectionConfig->DynamicChannelPool = MakeDynamicChannelPoolConfig();
        connectionConfig->DiscoveryAddress = "balancer";
        connectionConfig->Postprocess();
        return connectionConfig;
    }

    IRoamingChannelProviderPtr MakeBalancingChannelProvider(IOrmPeerDiscoveryPtr peerDiscovery)
    {
        return CreateBalancingChannelProvider(
            NDetail::MakeBalancingChannelConfig(MakeConnectionConfig()),
            MakeFakeChannelFactory(),
            /*endpointDescription*/ "",
            /*endpointAttributes*/ MakeEndpointAttributes(),
            peerDiscovery);
    }

    IConnectionPtr MakeConnection(NLogging::TLogger logger, IOrmPeerDiscoveryPtr peerDiscovery)
    {
        auto config = MakeConnectionConfig();
        return New<NDetail::TConnection>(
            config,
            std::move(logger),
            std::move(peerDiscovery),
            NDetail::MakeBalancingChannelConfig(config),
            MakeFakeChannelFactory());
    }

protected:
    ISimulatorPtr Simulator_;

    void SetUp() override
    {
        Simulator_ = InitializeSimulator(std::vector<TMasterInfo>{
            TMasterInfo {
                .GrpcAddress = "master1",
                .InstanceTag = TMasterInstanceTag{1}
            },
            TMasterInfo {
                .GrpcAddress = "master2",
                .InstanceTag = TMasterInstanceTag{2}
            }
        });
    }

};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TConnectionTestSuite, Balancing)
{
    auto peerDiscovery = CreateOrmPeerDiscovery<TFakeDiscoveryServiceProxy>(Logger, "balancer");
    auto channelProvider = MakeBalancingChannelProvider(peerDiscovery);
    for (int i = 0; i < 1'000; ++i) {
        auto channel = WaitFor(channelProvider->GetChannel(/*serviceName*/ ""))
            .ValueOrThrow();
        ASSERT_NE("balancer", GetAddress(channel));
        ASSERT_TRUE(GetAddress(channel) == "master1" || GetAddress(channel) == "master2");
    }
}

TEST_F(TConnectionTestSuite, Connection)
{
    auto peerDiscovery = CreateOrmPeerDiscovery<TFakeDiscoveryServiceProxy>(Logger, "balancer");
    auto connection = MakeConnection(Logger, peerDiscovery);

    ASSERT_FALSE(connection->GetChannel(TMasterInstanceTag{1}, /*retrying*/ false));
    ASSERT_FALSE(connection->GetChannel(TMasterInstanceTag{2}, /*retrying*/ false));

    auto channel = connection->GetChannel(/*retrying*/ false);
    WaitFor(PingGetAddress(channel))
        .ThrowOnError();

    ASSERT_EQ("master1", peerDiscovery->GetAddress(TMasterInstanceTag{1}));
    ASSERT_EQ("master2", peerDiscovery->GetAddress(TMasterInstanceTag{2}));

    ASSERT_TRUE(connection->GetChannel(TMasterInstanceTag{1}, /*retrying*/ false));
    ASSERT_TRUE(connection->GetChannel(TMasterInstanceTag{2}, /*retrying*/ false));
}

TEST_F(TConnectionTestSuite, BanDeadPeers)
{
    auto peerDiscovery = CreateOrmPeerDiscovery<TFakeDiscoveryServiceProxy>(Logger, "balancer");
    auto connection = MakeConnection(Logger, peerDiscovery);
    auto channel = connection->GetChannel(/*retrying*/ false);

    WaitForAlive(connection, "master1");
    WaitForAlive(connection, "master2");
    GetSimulator()->Shutdown("master1");
    WaitForBanned(connection, "master1");
}

TEST_F(TConnectionTestSuite, RestartAll)
{
    auto peerDiscovery = CreateOrmPeerDiscovery<TFakeDiscoveryServiceProxy>(Logger, "balancer");
    auto connection = MakeConnection(Logger, peerDiscovery);

    WaitForAlive(connection, "master1");
    WaitForAlive(connection, "master2");

    GetSimulator()->Shutdown("master1");
    GetSimulator()->Shutdown("master2");
    WaitForAllDead(connection);

    GetSimulator()->Restart("master1");
    WaitForAlive(connection, "master1");
    GetSimulator()->Restart("master2");
    WaitForAlive(connection, "master2");
}

TEST_F(TConnectionTestSuite, DetectFailures)
{
    auto peerDiscovery = CreateOrmPeerDiscovery<TFakeDiscoveryServiceProxy>(Logger, "balancer");
    auto connection = MakeConnection(Logger, peerDiscovery);
    auto channel = connection->GetChannel(/*retrying*/ false);

    WaitForAlive(connection, "master1");
    WaitForAlive(connection, "master2");

    GetSimulator()->SetSimulationMode(ENodeShutdownSimulationMode::FailUserRequests);
    GetSimulator()->Shutdown("master1");

    while (WaitFor(PingGetAddress(channel))
        .GetCode() != NRpc::EErrorCode::Unavailable)
    { }

    ASSERT_TRUE(WaitFor(IsAddressBanned(channel, "master1"))
        .ValueOrThrow());

}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOrm::NClient::NNative::NTests
