#include <yt/core/test_framework/framework.h>

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/throughput_throttler.h>

#include <yt/ytlib/distributed_throttler/public.h>
#include <yt/ytlib/distributed_throttler/distributed_throttler.h>
#include <yt/ytlib/distributed_throttler/config.h>

#include <yt/ytlib/discovery_client/config.h>

#include <yt/server/lib/discovery_server/public.h>
#include <yt/server/lib/discovery_server/config.h>
#include <yt/server/lib/discovery_server/discovery_service.h>

#include <yt/core/rpc/local_channel.h>
#include <yt/core/rpc/local_server.h>
#include <yt/core/rpc/server.h>
#include <yt/core/rpc/static_channel_factory.h>

#include <yt/core/profiling/timing.h>

#include <thread>
#include <vector>

namespace NYT::NDistributedThrottler {
namespace {

using namespace NConcurrency;
using namespace NRpc;
using namespace NDiscoveryClient;
using namespace NDiscoveryServer;

////////////////////////////////////////////////////////////////////////////////

class TDistributedThrottlerTestSuite
    : public ::testing::Test
{
public:
    virtual void SetUp() override
    {
        ChannelFactory_ = New<TStaticChannelFactory>();
        for (const auto& address : Addresses_) {
            RpcServers_.push_back(CreateLocalServer());
            ChannelFactory_->Add(address, CreateLocalChannel(RpcServers_.back()));
            RpcServers_.back()->Start();
        }

        auto serverConfig = New<TDiscoveryServerConfig>();
        serverConfig->ServerAddresses = Addresses_;
        serverConfig->AttributesUpdatePeriod = TDuration::Seconds(2);

        for (int i = 0; i < Addresses_.size(); ++i) {
            DiscoveryServers_.push_back(CreateDiscoveryServer(serverConfig, i));
            DiscoveryServers_.back()->Initialize();
        }
    }

    virtual void TearDown() override
    {
        for (int i = 0; i < Addresses_.size(); ++i) {
            DiscoveryServers_[i]->Finalize();
            RpcServers_[i]->Stop();
        }
    }

    TDistributedThrottlerConfigPtr GenerateThrottlerConfig()
    {
        auto config = New<TDistributedThrottlerConfig>();
        config->MemberClient->ServerAddresses = Addresses_;
        config->DiscoveryClient->ServerAddresses = Addresses_;
        config->LimitUpdatePeriod = TDuration::MilliSeconds(300);
        config->LeaderUpdatePeriod = TDuration::MilliSeconds(500);
        return config;
    }

    const TStaticChannelFactoryPtr& GetChannelFactory()
    {
        return ChannelFactory_;
    }

private:
    std::vector<TString> Addresses_ = {"peer1", "peer2", "peer3", "peer4", "peer5"};
    std::vector<TDiscoveryServerPtr> DiscoveryServers_;
    std::vector<IServerPtr> RpcServers_;

    std::vector<TActionQueuePtr> ActionQueues_;
    TStaticChannelFactoryPtr ChannelFactory_;

    TDiscoveryServerPtr CreateDiscoveryServer(const TDiscoveryServerConfigPtr& serverConfig, int index)
    {
        auto serverActionQueue = New<TActionQueue>("DiscoveryServer" + ToString(index));
        auto gossipActionQueue = New<TActionQueue>("Gossip" + ToString(index));

        auto server = New<TDiscoveryServer>(
            RpcServers_[index],
            Addresses_[index],
            serverConfig,
            ChannelFactory_,
            serverActionQueue->GetInvoker(),
            gossipActionQueue->GetInvoker());

        ActionQueues_.push_back(serverActionQueue);
        ActionQueues_.push_back(gossipActionQueue);

        return server;
    }
};

TEST_F(TDistributedThrottlerTestSuite, TestLimitUniform)
{
    int throttlersCount = 4;
    auto leaderThrottlerConfig = New<TThroughputThrottlerConfig>(100);
    auto throttlerConfig = New<TThroughputThrottlerConfig>(1);
    auto config = GenerateThrottlerConfig();

    const auto& channelFactory = GetChannelFactory();
    auto rpcServer = CreateLocalServer();
    auto address = "ThrottlerService";
    channelFactory->Add(address, CreateLocalChannel(rpcServer));

    std::vector<TActionQueuePtr> actionQueues;
    std::vector<IReconfigurableThroughputThrottlerPtr> throttlers;
    for (int i = 0; i < throttlersCount; ++i) {
        auto memberActionQueue = New<TActionQueue>("MemberClient" + ToString(i));
        actionQueues.push_back(memberActionQueue);

        throttlers.push_back(CreateDistributedThrottler(
            config,
            i == 0 ? leaderThrottlerConfig : throttlerConfig,
            channelFactory,
            memberActionQueue->GetInvoker(),
            "group",
            "throttler" + ToString(i),
            rpcServer,
            address,
            DiscoveryServerLogger));
    }

    // Wait for leader to update limits.
    while (throttlers.back()->TryAcquireAvailable(10) < 2) {
        Sleep(TDuration::Seconds(1));
    }

    // Just to make sure all throttlers are alive.
    Sleep(TDuration::Seconds(3));

    NProfiling::TWallTimer timer;
    std::vector<TFuture<void>> futures;
    for (int i = 0; i < throttlersCount; ++i) {
        futures.push_back(BIND([=] {
            for (int j = 0; j < 5; ++j) {
                WaitFor(throttlers[i]->Throttle(30)).ThrowOnError();
            }
        })
        .AsyncVia(actionQueues[i]->GetInvoker())
        .Run());
    }
    WaitFor(CombineAll(futures)).ThrowOnError();

    auto duration = timer.GetElapsedTime().MilliSeconds();
    EXPECT_GE(duration, 3000);
    EXPECT_LE(duration, 7000);
}

TEST_F(TDistributedThrottlerTestSuite, TestLimitAdaptive)
{
    auto throttlerConfig = New<TThroughputThrottlerConfig>(100);
    auto config = GenerateThrottlerConfig();
    config->DistributeLimitsUniformly = false;

    const auto& channelFactory = GetChannelFactory();
    auto rpcServer = CreateLocalServer();
    auto address = "ThrottlerService";
    channelFactory->Add(address, CreateLocalChannel(rpcServer));

    std::vector<TActionQueuePtr> actionQueues;
    std::vector<IReconfigurableThroughputThrottlerPtr> throttlers;
    for (int i = 0; i < 4; ++i) {
        auto memberActionQueue = New<TActionQueue>("MemberClient" + ToString(i));
        actionQueues.push_back(memberActionQueue);

        throttlers.push_back(CreateDistributedThrottler(
            config,
            throttlerConfig,
            channelFactory,
            memberActionQueue->GetInvoker(),
            "group",
            "throttler" + ToString(i),
            rpcServer,
            address,
            DiscoveryServerLogger));
    }

    Sleep(TDuration::Seconds(3));

    NProfiling::TWallTimer timer;

    std::vector<TFuture<void>> futures;
    for (int i = 0; i < 4; ++i) {
        futures.push_back(BIND([=] {
            for (int j = 0; j < 10; ++j) {
                WaitFor(throttlers[i]->Throttle(30)).ThrowOnError();
            }
        })
        .AsyncVia(actionQueues[i]->GetInvoker())
        .Run());
    }
    WaitFor(CombineAll(futures)).ThrowOnError();

    auto duration = timer.GetElapsedTime().MilliSeconds();
    EXPECT_GE(duration, 9000);
    EXPECT_LE(duration, 15000);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NDistributedThrottler
