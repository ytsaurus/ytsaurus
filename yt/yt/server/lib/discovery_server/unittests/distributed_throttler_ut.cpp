#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/throughput_throttler.h>

#include <yt/yt/core/rpc/service_detail.h>

#include <yt/yt/ytlib/distributed_throttler/public.h>
#include <yt/yt/ytlib/distributed_throttler/distributed_throttler.h>
#include <yt/yt/ytlib/distributed_throttler/config.h>

#include <yt/yt/ytlib/discovery_client/config.h>
#include <yt/yt/ytlib/discovery_client/discovery_client.h>

#include <yt/yt/server/lib/discovery_server/public.h>
#include <yt/yt/server/lib/discovery_server/config.h>
#include <yt/yt/server/lib/discovery_server/discovery_server.h>

#include <yt/yt/server/lib/discovery_server/unittests/mock/connection.h>

#include <yt/yt/core/rpc/local_channel.h>
#include <yt/yt/core/rpc/local_server.h>
#include <yt/yt/core/rpc/server.h>
#include <yt/yt/core/rpc/static_channel_factory.h>

#include <yt/yt/core/profiling/timing.h>

#include <vector>

namespace NYT::NDistributedThrottler {
namespace {

using namespace NConcurrency;
using namespace NRpc;
using namespace NDiscoveryClient;
using namespace NDiscoveryServer;

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, Logger, "Test");

////////////////////////////////////////////////////////////////////////////////

struct TQueueToThrottler
{
    TActionQueuePtr ActionQueue;
    IReconfigurableThroughputThrottlerPtr Throttler;
};

class TDistributedThrottlerTest
    : public ::testing::Test
{
public:
    void SetUp() override
    {
        ChannelFactory_ = New<TStaticChannelFactory>();
        for (const auto& address : Addresses_) {
            RpcServers_.push_back(CreateLocalServer());
            ChannelFactory_->Add(address, CreateLocalChannel(RpcServers_.back()));
            RpcServers_.back()->Start();
        }

        auto serverConfig = New<TDiscoveryServerConfig>();
        serverConfig->ServerAddresses = Addresses_;
        serverConfig->AttributesUpdatePeriod = TDuration::MilliSeconds(500);
        serverConfig->GossipPeriod = TDuration::MilliSeconds(400);

        for (int i = 0; i < std::ssize(Addresses_); ++i) {
            DiscoveryServers_.push_back(CreateDiscoveryServer(serverConfig, i));
            DiscoveryServers_.back()->Initialize();
        }
    }

    std::vector<TQueueToThrottler> PrepareThrottlers(
        int throttlerCount,
        TThroughputThrottlerConfigPtr leaderThrottlerConfig,
        TDistributedThrottlerConfigPtr config)
    {
        YT_LOG_DEBUG("Started setting throttlers up");

        const auto& channelFactory = GetChannelFactory();
        auto rpcServer = CreateLocalServer();
        auto address = "ThrottlerService";
        channelFactory->Add(address, CreateLocalChannel(rpcServer));

        auto connectionConfig = CreateConnectionConfig();
        auto connection = New<TMockDistributedThrottlerConnection>(connectionConfig);

        std::vector<TQueueToThrottler> result;
        for (int i = 0; i < throttlerCount; ++i) {
            auto memberActionQueue = New<TActionQueue>("MemberClient" + ToString(i));

            auto factory = CreateDistributedThrottlerFactory(
                config,
                channelFactory,
                connection,
                memberActionQueue->GetInvoker(),
                "/group",
                "throttler" + ToString(i),
                rpcServer,
                address,
                Logger(),
                /*authenticator*/ nullptr);
            DistributedThrottlerFactoriesHolder_.push_back(factory);

            auto throttler = factory->GetOrCreateThrottler(
                "throttlerId",
                i == 0 ? leaderThrottlerConfig : InfiniteRequestThrottlerConfig);

            result.push_back({
                .ActionQueue = memberActionQueue,
                .Throttler = throttler});
        }

        auto discoveryClient = CreateDiscoveryClient(
            connectionConfig,
            config->DiscoveryClient,
            channelFactory);

        while (true) {
            auto rspOrError = WaitFor(discoveryClient->GetGroupMeta("/group"));
            if (!rspOrError.IsOK()) {
                continue;
            }
            auto count = rspOrError.Value().MemberCount;
            if (count >= throttlerCount - 1) {
                break;
            }
            Sleep(TDuration::Seconds(1));
        }

        Sleep(TDuration::Seconds(1));

        YT_LOG_DEBUG("Waiting for leader to update limits");

        // Wait for leader to update limits.
        while (true) {
            bool stop = true;

            // Starting from 1 to skip leader.
            for (auto i = 1; i < throttlerCount; ++i) {
                // Forcing limits recalculation.
                result[i].Throttler->TryAcquire(1);

                if (!result[i].Throttler->GetLimit()) {
                    stop = false;
                    break;
                }
            }
            if (stop) {
                break;
            }
            Sleep(TDuration::Seconds(1));
        }

        // Just to make sure all throttlers are alive.
        Sleep(TDuration::Seconds(3));

        YT_LOG_DEBUG("Throttlers are ready");

        return result;
    }

    void TearDown() override
    {
        for (int i = 0; i < std::ssize(Addresses_); ++i) {
            DiscoveryServers_[i]->Finalize();
            YT_UNUSED_FUTURE(RpcServers_[i]->Stop());
        }
    }

    TDistributedThrottlerConfigPtr GenerateThrottlerConfig()
    {
        auto config = New<TDistributedThrottlerConfig>();
        config->MemberClient->AttributeUpdatePeriod = TDuration::MilliSeconds(300);
        config->MemberClient->HeartbeatPeriod = TDuration::MilliSeconds(50);
        config->LimitUpdatePeriod = TDuration::MilliSeconds(100);
        config->LeaderUpdatePeriod = TDuration::MilliSeconds(1500);
        config->HeartbeatThrottlerCountLimit = 2;
        return config;
    }

    TDiscoveryConnectionConfigPtr CreateConnectionConfig()
    {
        auto connectionConfig = New<TDiscoveryConnectionConfig>();
        connectionConfig->Addresses = Addresses_;
        return connectionConfig;
    }

    const TStaticChannelFactoryPtr& GetChannelFactory()
    {
        return ChannelFactory_;
    }

private:
    std::vector<std::string> Addresses_ = {"peer1", "peer2", "peer3", "peer4", "peer5"};
    std::vector<IDiscoveryServerPtr> DiscoveryServers_;
    std::vector<IServerPtr> RpcServers_;

    std::vector<IDistributedThrottlerFactoryPtr> DistributedThrottlerFactoriesHolder_;

    std::vector<TActionQueuePtr> ActionQueues_;
    TStaticChannelFactoryPtr ChannelFactory_;

    IDiscoveryServerPtr CreateDiscoveryServer(const TDiscoveryServerConfigPtr& serverConfig, int index)
    {
        auto serverActionQueue = New<TActionQueue>("DiscoveryServer" + ToString(index));
        auto gossipActionQueue = New<TActionQueue>("Gossip" + ToString(index));

        auto server = NDiscoveryServer::CreateDiscoveryServer(
            RpcServers_[index],
            Addresses_[index],
            serverConfig,
            ChannelFactory_,
            serverActionQueue->GetInvoker(),
            gossipActionQueue->GetInvoker(),
            /*authenticator*/ nullptr);

        ActionQueues_.push_back(serverActionQueue);
        ActionQueues_.push_back(gossipActionQueue);

        return server;
    }
};

TEST_F(TDistributedThrottlerTest, TestLimitUniform)
{
    int throttlerCount = 4;
    auto config = GenerateThrottlerConfig();
    config->Mode = EDistributedThrottlerMode::Uniform;

    auto queueToThrottler = PrepareThrottlers(
        throttlerCount,
        TThroughputThrottlerConfig::Create(100),
        config);

    NProfiling::TWallTimer timer;
    std::vector<TFuture<void>> futures;
    for (int i = 0; i < throttlerCount; ++i) {
        futures.push_back(BIND([=] {
            for (int j = 0; j < 5; ++j) {
                // To make sure that usage rate is synchronized.
                WaitFor(queueToThrottler[i].Throttler->Throttle(1)).ThrowOnError();
                Sleep(TDuration::MilliSeconds(60));
                WaitFor(queueToThrottler[i].Throttler->Throttle(30)).ThrowOnError();
            }
        })
        .AsyncVia(queueToThrottler[i].ActionQueue->GetInvoker())
        .Run());
    }
    WaitFor(AllSet(futures)).ThrowOnError();

    i64 duration = timer.GetElapsedTime().MilliSeconds();
    EXPECT_GE(duration, 3000);
    EXPECT_LE(duration, 7000);
}

TEST_F(TDistributedThrottlerTest, TestLimitAdaptive)
{
    int throttlerCount = 4;
    auto config = GenerateThrottlerConfig();
    config->Mode = EDistributedThrottlerMode::Adaptive;

    auto queueToThrottler = PrepareThrottlers(
        throttlerCount,
        TThroughputThrottlerConfig::Create(100),
        config);

    NProfiling::TWallTimer timer;
    std::vector<TFuture<void>> futures;
    for (int i = 0; i < throttlerCount; ++i) {
        futures.push_back(BIND([=] {
            for (int j = 0; j < 10; ++j) {
                WaitFor(queueToThrottler[i].Throttler->Throttle(30)).ThrowOnError();
            }
        })
        .AsyncVia(queueToThrottler[i].ActionQueue->GetInvoker())
        .Run());
    }
    WaitFor(AllSet(futures)).ThrowOnError();

    auto duration = timer.GetElapsedTime().MilliSeconds();
    EXPECT_GE(duration, 8000u);
    EXPECT_LE(duration, 15000u);
}

TEST_F(TDistributedThrottlerTest, TestAdaptiveFractionalLimits)
{
    int throttlerCount = 4;
    auto config = GenerateThrottlerConfig();
    config->Mode = EDistributedThrottlerMode::Adaptive;
    config->ExtraLimitRatio = 0.;

    auto queueToThrottler = PrepareThrottlers(
        throttlerCount,
        TThroughputThrottlerConfig::Create(1),
        config);

    NProfiling::TWallTimer timer;
    std::vector<TFuture<void>> futures;
    for (int i = 0; i < throttlerCount; ++i) {
        futures.push_back(BIND([=] {
            for (int j = 0; j < 10; ++j) {
                WaitFor(queueToThrottler[i].Throttler->Throttle(1)).ThrowOnError();
            }
        })
        .AsyncVia(queueToThrottler[i].ActionQueue->GetInvoker())
        .Run());
    }
    WaitFor(AllSet(futures)).ThrowOnError();

    // Expected to be around 36-39 seconds.
    auto duration = timer.GetElapsedTime().MilliSeconds();
    EXPECT_GE(duration, 34000u);
    EXPECT_LE(duration, 41000u);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NDistributedThrottler
