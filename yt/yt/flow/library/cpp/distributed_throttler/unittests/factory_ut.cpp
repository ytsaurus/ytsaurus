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

    static TDistributedThrottlerServiceConfigPtr MakeServiceConfig(
        std::initializer_list<std::pair<std::string, std::optional<double>>> throttlers)
    {
        auto config = New<TDistributedThrottlerServiceConfig>();
        for (const auto& [name, limit] : throttlers) {
            auto bucketConfig = New<TDistributedThrottlerBucketConfig>();
            bucketConfig->Throttler->Limit = limit;
            config->Throttlers[name] = bucketConfig;
        }
        return config;
    }

    void StartServer(std::initializer_list<std::pair<std::string, std::optional<double>>> throttlers)
    {
        Service_ = CreateDistributedThrottlerService(
            MakeServiceConfig(throttlers),
            ActionQueue_->GetInvoker(),
            NLogging::TLogger("Test"));
        LocalServer_->RegisterService(Service_->GetRpcService());
        LocalServer_->Start();
    }

    //! Lifts a deliberately starved bucket so a request parked on the token
    //! bucket is granted at once.
    void ReconfigureServer(std::initializer_list<std::pair<std::string, std::optional<double>>> throttlers)
    {
        Service_->Reconfigure(MakeServiceConfig(throttlers));
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

TEST_F(TDistributedThrottlerFactoryTest, GetClientOrThrowReturnsSameHandleAcrossCalls)
{
    StartServer({{"api", 1000}});
    auto factory = MakeFactory({{TThrottlerId("api"), MakeSpec(1000.0)}});

    auto first = factory->GetClientOrThrow(TThrottlerId("api"));
    auto second = factory->GetClientOrThrow(TThrottlerId("api"));
    EXPECT_EQ(first.Get(), second.Get());
}

TEST_F(TDistributedThrottlerFactoryTest, TryGetClientReturnsConfiguredHandle)
{
    StartServer({{"api", 1000}});
    auto factory = MakeFactory({{TThrottlerId("api"), MakeSpec(1000.0)}});

    EXPECT_EQ(factory->TryGetClient(TThrottlerId("api")).Get(), factory->GetClientOrThrow(TThrottlerId("api")).Get());
}

TEST_F(TDistributedThrottlerFactoryTest, TryGetClientReturnsNullForUnknownName)
{
    StartServer({{"api", 1000}});
    auto factory = MakeFactory({{TThrottlerId("api"), MakeSpec(1000.0)}});

    EXPECT_FALSE(factory->TryGetClient(TThrottlerId("nonexistent")));
}

TEST_F(TDistributedThrottlerFactoryTest, SetQuotaClassesKeepsHandleStable)
{
    StartServer({{"api", 1000}});
    auto factory = MakeFactory({{TThrottlerId("api"), MakeSpec(1000.0)}});

    auto handle = factory->GetClientOrThrow(TThrottlerId("api"));
    factory->SetQuotaClasses({{TThrottlerId("api"), "vip"}});

    EXPECT_EQ(handle.Get(), factory->GetClientOrThrow(TThrottlerId("api")).Get());
    EXPECT_TRUE(WaitFor(handle->Throttle(1)).IsOK());
}

TEST_F(TDistributedThrottlerFactoryTest, EmptyQuotaClassesUseDefault)
{
    StartServer({{"api", 1000}});
    auto factory = MakeFactory({{TThrottlerId("api"), MakeSpec(1000.0)}});
    factory->SetQuotaClasses({});

    EXPECT_TRUE(WaitFor(factory->GetClientOrThrow(TThrottlerId("api"))->Throttle(1)).IsOK());
}

TEST_F(TDistributedThrottlerFactoryTest, ClassedThrottlerHasOneHandlePerId)
{
    // Automatic input throttling and user code both reach a throttler through
    // GetClientOrThrow, so a configured class covers every request to that id;
    // there is no separate class-free handle for manual use.
    StartServer({{"api", 1000}});
    auto factory = MakeFactory({{TThrottlerId("api"), MakeSpec(1000.0)}});
    factory->SetQuotaClasses({{TThrottlerId("api"), "vip"}});

    auto first = factory->GetClientOrThrow(TThrottlerId("api"));
    auto second = factory->GetClientOrThrow(TThrottlerId("api"));
    EXPECT_EQ(first.Get(), second.Get());
    EXPECT_TRUE(WaitFor(second->Throttle(1)).IsOK());
}

TEST_F(TDistributedThrottlerFactoryTest, QuotaClassAppliesOnlyToItsThrottler)
{
    // A throttler absent from the map must not inherit another's class: that
    // was the whole point of scoping classes per throttler.
    StartServer({{"classed", 1000}, {"plain", 1000}});
    auto factory = MakeFactory({
        {TThrottlerId("classed"), MakeSpec(1000.0)},
        {TThrottlerId("plain"), MakeSpec(1000.0)},
    });
    factory->SetQuotaClasses({{TThrottlerId("classed"), "vip"}});

    EXPECT_TRUE(WaitFor(factory->GetClientOrThrow(TThrottlerId("classed"))->Throttle(1)).IsOK());
    EXPECT_TRUE(WaitFor(factory->GetClientOrThrow(TThrottlerId("plain"))->Throttle(1)).IsOK());
}

TEST_F(TDistributedThrottlerFactoryTest, QuotaClassSurvivesReconfigure)
{
    // The class holder outlives client rebuilds, so a class set before a
    // Reconfigure still reaches the freshly built client.
    StartServer({{"api", 1000}});
    auto factory = MakeFactory({{TThrottlerId("api"), MakeSpec(1000.0)}});
    factory->SetQuotaClasses({{TThrottlerId("api"), "vip"}});

    auto handle = factory->GetClientOrThrow(TThrottlerId("api"));
    factory->Reconfigure({{TThrottlerId("api"), MakeSpec(500.0, TDuration::Seconds(2))}});

    EXPECT_EQ(handle.Get(), factory->GetClientOrThrow(TThrottlerId("api")).Get());
    EXPECT_TRUE(WaitFor(handle->Throttle(1)).IsOK());
}

TEST_F(TDistributedThrottlerFactoryTest, GetClientOrThrowThrowsForUnknownName)
{
    StartServer({{"api", 1000}});
    auto factory = MakeFactory({{TThrottlerId("api"), MakeSpec(1000.0)}});

    EXPECT_THROW_WITH_SUBSTRING(
        factory->GetClientOrThrow(TThrottlerId("nonexistent")),
        "not configured");
}

TEST_F(TDistributedThrottlerFactoryTest, HandleSurvivesReconfigureWithChangedSpec)
{
    StartServer({{"api", 1000}});
    auto factory = MakeFactory({{TThrottlerId("api"), MakeSpec(1000.0)}});

    auto handle = factory->GetClientOrThrow(TThrottlerId("api"));
    EXPECT_TRUE(WaitFor(handle->Throttle(1)).IsOK());

    factory->Reconfigure({{TThrottlerId("api"), MakeSpec(500.0, TDuration::Seconds(2))}});

    // Same handle pointer keeps working after the underlying client is rebuilt.
    EXPECT_EQ(handle.Get(), factory->GetClientOrThrow(TThrottlerId("api")).Get());
    EXPECT_TRUE(WaitFor(handle->Throttle(1)).IsOK());
}

TEST_F(TDistributedThrottlerFactoryTest, HandleSurvivesReconfigureWithUnchangedSpec)
{
    StartServer({{"api", 1000}});
    auto factory = MakeFactory({{TThrottlerId("api"), MakeSpec(1000.0)}});

    auto handle = factory->GetClientOrThrow(TThrottlerId("api"));
    EXPECT_TRUE(WaitFor(handle->Throttle(1)).IsOK());

    // New shared pointer carrying an equal spec — handle and underlying must
    // both stay (no rebuild).
    factory->Reconfigure({{TThrottlerId("api"), MakeSpec(1000.0)}});

    EXPECT_EQ(handle.Get(), factory->GetClientOrThrow(TThrottlerId("api")).Get());
    EXPECT_TRUE(WaitFor(handle->Throttle(1)).IsOK());
}

TEST_F(TDistributedThrottlerFactoryTest, ServerOnlySpecChangeKeepsHandleWorking)
{
    StartServer({{"api", 1000}});
    auto initialSpec = MakeSpec(1000.0);
    initialSpec->Classes[NYT::NFlow::TQuotaClassId("vip")] = New<TDynamicThrottlerClassSpec>();
    initialSpec->Classes.at(NYT::NFlow::TQuotaClassId("vip"))->Weight = 5.0;
    initialSpec->MaxGrantAmount = 10;
    auto factory = MakeFactory({{TThrottlerId("api"), initialSpec}});

    auto handle = factory->GetClientOrThrow(TThrottlerId("api"));
    EXPECT_TRUE(WaitFor(handle->Throttle(1)).IsOK());

    auto updatedSpec = CloneYsonStruct(initialSpec);
    updatedSpec->Classes.at(NYT::NFlow::TQuotaClassId("vip"))->Weight = 1.0;
    updatedSpec->MaxGrantAmount = 1;
    factory->Reconfigure({{TThrottlerId("api"), updatedSpec}});

    EXPECT_EQ(handle.Get(), factory->GetClientOrThrow(TThrottlerId("api")).Get());
    EXPECT_TRUE(WaitFor(handle->Throttle(1)).IsOK());
}

TEST_F(TDistributedThrottlerFactoryTest, HandleThrowsAfterNameRemoved)
{
    StartServer({{"api", 1000}});
    auto factory = MakeFactory({{TThrottlerId("api"), MakeSpec(1000.0)}});

    auto handle = factory->GetClientOrThrow(TThrottlerId("api"));
    EXPECT_TRUE(WaitFor(handle->Throttle(1)).IsOK());

    factory->Reconfigure({});

    EXPECT_FALSE(factory->TryGetClient(TThrottlerId("api")));
    EXPECT_THROW_WITH_SUBSTRING(
        WaitFor(handle->Throttle(1)).ThrowOnError(),
        "not configured");

    // GetClientOrThrow with the removed name now throws too.
    EXPECT_THROW_WITH_SUBSTRING(
        factory->GetClientOrThrow(TThrottlerId("api")),
        "not configured");
}

TEST_F(TDistributedThrottlerFactoryTest, HandleResumesAfterNameReadded)
{
    StartServer({{"api", 1000}});
    auto factory = MakeFactory({{TThrottlerId("api"), MakeSpec(1000.0)}});

    auto handle = factory->GetClientOrThrow(TThrottlerId("api"));
    factory->Reconfigure({});

    EXPECT_FALSE(WaitFor(handle->Throttle(1)).IsOK());

    factory->Reconfigure({{TThrottlerId("api"), MakeSpec(1000.0)}});

    EXPECT_EQ(handle.Get(), factory->TryGetClient(TThrottlerId("api")).Get());
    // The cached handle is rewired to a fresh underlying client.
    EXPECT_EQ(handle.Get(), factory->GetClientOrThrow(TThrottlerId("api")).Get());
    EXPECT_TRUE(WaitFor(handle->Throttle(1)).IsOK());
}

TEST_F(TDistributedThrottlerFactoryTest, PendingThrottleSurvivesClientRebuild)
{
    // Starved bucket: the request parks on the server's token bucket instead of
    // being granted inside Throttle().
    StartServer({{"api", 1}});
    auto factory = MakeFactory({{TThrottlerId("api"), MakeSpec(1.0)}});

    auto handle = factory->GetClientOrThrow(TThrottlerId("api"));
    auto future = handle->Throttle(3);
    EXPECT_FALSE(future.IsSet());

    // A client-visible spec change rebuilds the underlying client while the
    // request above is still in flight.
    auto updatedSpec = MakeSpec(1.0);
    updatedSpec->RpcTimeout = TDuration::Seconds(6);
    factory->Reconfigure({{TThrottlerId("api"), updatedSpec}});

    ReconfigureServer({{"api", 1000}});

    auto error = WaitFor(future);
    EXPECT_TRUE(error.IsOK()) << ToString(error);
}

TEST_F(TDistributedThrottlerFactoryTest, CanceledThrottleStaysCancelable)
{
    StartServer({{"api", 1}});
    auto factory = MakeFactory({{TThrottlerId("api"), MakeSpec(1.0)}});

    auto handle = factory->GetClientOrThrow(TThrottlerId("api"));
    auto future = handle->Throttle(3);
    EXPECT_FALSE(future.IsSet());

    // Retaining the client through a subscriber must not replace the caller's
    // future or suppress its cancellation.
    EXPECT_TRUE(future.Cancel(TError(NYT::EErrorCode::Canceled, "Test cancellation")));

    auto error = WaitFor(future);
    EXPECT_EQ(error.GetCode(), NYT::EErrorCode::Canceled) << ToString(error);

    // The prefetcher does not propagate caller cancellation to its batched RPC;
    // let that RPC drain before checking the client remains usable.
    ReconfigureServer({{"api", 1000}});
    EXPECT_TRUE(WaitFor(handle->Throttle(1)).IsOK());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NDistributedThrottler
