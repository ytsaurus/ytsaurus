#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/flow/library/cpp/distributed_throttler/factory.h>
#include <yt/yt/flow/library/cpp/distributed_throttler/server.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/rpc/local_channel.h>
#include <yt/yt/core/rpc/local_server.h>
#include <yt/yt/core/rpc/server.h>

#include <yt/yt/core/test_framework/framework.h>

namespace NYT::NFlow::NDistributedThrottler {
namespace {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TDistributedThrottlerFactoryTest
    : public ::testing::Test
{
protected:
    void SetUp() override
    {
        ActionQueue_ = New<TActionQueue>("Test");
        LocalServer_ = NRpc::CreateLocalServer();
        StatusProfiler_ = CreateSyncStatusProfiler();
    }

    void TearDown() override
    {
        YT_UNUSED_FUTURE(LocalServer_->Stop());
        ActionQueue_->Shutdown();
    }

    void StartServer(std::initializer_list<std::pair<std::string, std::optional<double>>> throttlers)
    {
        auto config = New<TDistributedThrottlerServiceConfig>();
        for (const auto& [name, limit] : throttlers) {
            auto tc = New<TThroughputThrottlerConfig>();
            tc->Limit = limit;
            config->Throttlers[name] = tc;
        }
        Service_ = CreateDistributedThrottlerService(
            std::move(config),
            ActionQueue_->GetInvoker(),
            NLogging::TLogger("Test"));
        LocalServer_->RegisterService(Service_->GetRpcService());
        LocalServer_->Start();
    }

    static TDynamicThrottlerSpecPtr MakeSpec(std::optional<double> limit, TDuration period = TDuration::Seconds(1))
    {
        auto spec = New<TDynamicThrottlerSpec>();
        spec->Limit = limit;
        spec->Period = period;
        // Tight prefetch so tests do not stall on the default 5s request period.
        spec->RequestPeriod = TDuration::MilliSeconds(50);
        spec->RetryingChannel = New<NRpc::TRetryingChannelConfig>();
        spec->RpcTimeout = TDuration::Seconds(5);
        return spec;
    }

    IDistributedThrottlerFactoryPtr MakeFactory(THashMap<TThrottlerId, TDynamicThrottlerSpecPtr> throttlers)
    {
        auto channel = NRpc::CreateLocalChannel(LocalServer_);
        return CreateDistributedThrottlerFactory(
            [channel] {
                return channel;
            },
            "test-client",
            std::move(throttlers),
            StatusProfiler_,
            NLogging::TLogger("TestFactory"),
            /*profiler*/ {});
    }

    TActionQueuePtr ActionQueue_;
    NRpc::IServerPtr LocalServer_;
    IDistributedThrottlerServicePtr Service_;
    IStatusProfilerPtr StatusProfiler_;
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TDistributedThrottlerFactoryTest, GetClientReturnsSameHandleAcrossCalls)
{
    StartServer({{"api", 1000}});
    auto factory = MakeFactory({{TThrottlerId("api"), MakeSpec(1000.0)}});

    auto first = factory->GetClient(TThrottlerId("api"));
    auto second = factory->GetClient(TThrottlerId("api"));
    EXPECT_EQ(first.Get(), second.Get());
}

TEST_F(TDistributedThrottlerFactoryTest, GetClientThrowsForUnknownName)
{
    StartServer({{"api", 1000}});
    auto factory = MakeFactory({{TThrottlerId("api"), MakeSpec(1000.0)}});

    EXPECT_THROW_WITH_SUBSTRING(
        factory->GetClient(TThrottlerId("nonexistent")),
        "not configured");
}

TEST_F(TDistributedThrottlerFactoryTest, HandleSurvivesReconfigureWithChangedSpec)
{
    StartServer({{"api", 1000}});
    auto factory = MakeFactory({{TThrottlerId("api"), MakeSpec(1000.0)}});

    auto handle = factory->GetClient(TThrottlerId("api"));
    EXPECT_TRUE(WaitFor(handle->Throttle(1)).IsOK());

    factory->Reconfigure({{TThrottlerId("api"), MakeSpec(500.0, TDuration::Seconds(2))}});

    // Same handle pointer keeps working after the underlying client is rebuilt.
    EXPECT_EQ(handle.Get(), factory->GetClient(TThrottlerId("api")).Get());
    EXPECT_TRUE(WaitFor(handle->Throttle(1)).IsOK());
}

TEST_F(TDistributedThrottlerFactoryTest, HandleSurvivesReconfigureWithUnchangedSpec)
{
    StartServer({{"api", 1000}});
    auto factory = MakeFactory({{TThrottlerId("api"), MakeSpec(1000.0)}});

    auto handle = factory->GetClient(TThrottlerId("api"));
    EXPECT_TRUE(WaitFor(handle->Throttle(1)).IsOK());

    // New shared pointer carrying an equal spec — handle and underlying must
    // both stay (no rebuild).
    factory->Reconfigure({{TThrottlerId("api"), MakeSpec(1000.0)}});

    EXPECT_EQ(handle.Get(), factory->GetClient(TThrottlerId("api")).Get());
    EXPECT_TRUE(WaitFor(handle->Throttle(1)).IsOK());
}

TEST_F(TDistributedThrottlerFactoryTest, HandleThrowsAfterNameRemoved)
{
    StartServer({{"api", 1000}});
    auto factory = MakeFactory({{TThrottlerId("api"), MakeSpec(1000.0)}});

    auto handle = factory->GetClient(TThrottlerId("api"));
    EXPECT_TRUE(WaitFor(handle->Throttle(1)).IsOK());

    factory->Reconfigure({});

    EXPECT_THROW_WITH_SUBSTRING(
        WaitFor(handle->Throttle(1)).ThrowOnError(),
        "not configured");

    // GetClient with the removed name now throws too.
    EXPECT_THROW_WITH_SUBSTRING(
        factory->GetClient(TThrottlerId("api")),
        "not configured");
}

TEST_F(TDistributedThrottlerFactoryTest, HandleResumesAfterNameReadded)
{
    StartServer({{"api", 1000}});
    auto factory = MakeFactory({{TThrottlerId("api"), MakeSpec(1000.0)}});

    auto handle = factory->GetClient(TThrottlerId("api"));
    factory->Reconfigure({});

    EXPECT_FALSE(WaitFor(handle->Throttle(1)).IsOK());

    factory->Reconfigure({{TThrottlerId("api"), MakeSpec(1000.0)}});

    // The cached handle is rewired to a fresh underlying client.
    EXPECT_EQ(handle.Get(), factory->GetClient(TThrottlerId("api")).Get());
    EXPECT_TRUE(WaitFor(handle->Throttle(1)).IsOK());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NDistributedThrottler
