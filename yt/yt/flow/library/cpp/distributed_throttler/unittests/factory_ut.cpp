#include <yt/yt/flow/library/cpp/common/spec.h>

#include <yt/yt/flow/library/cpp/distributed_throttler/factory.h>
#include <yt/yt/flow/library/cpp/distributed_throttler/server.h>

#include <yt/yt/flow/library/cpp/misc/status_profiler.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/scheduler_api.h>
#include <yt/yt/core/concurrency/suspendable_invoker.h>

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
    TActionQueuePtr Queue_;
    ISuspendableInvokerPtr ServerInvoker_;
    NRpc::IServerPtr Server_;
    IDistributedThrottlerServicePtr Service_;
    IWorkerThrottlerFactoryPtr WorkerFactory_;
    int NextClientId_ = 0;

    void SetUp() override
    {
        Queue_ = New<TActionQueue>("Test");
        ServerInvoker_ = CreateSuspendableInvoker(Queue_->GetInvoker());
        Server_ = NRpc::CreateLocalServer();
        Service_ = CreateDistributedThrottlerService(
            New<TDistributedThrottlerServiceConfig>(),
            ServerInvoker_,
            {},
            {});
        Server_->RegisterService(Service_->GetRpcService());
        Server_->Start();
        WorkerFactory_ = CreateWorkerThrottlerFactory({}, NRpc::CreateLocalChannel(Server_));
    }

    void TearDown() override
    {
        WorkerFactory_.Reset();
        YT_UNUSED_FUTURE(Server_->Stop());
        Service_.Reset();
        Queue_->Shutdown();
    }

    void ReconfigureServer(
        std::initializer_list<std::pair<std::string, TDynamicThrottlerSpecPtr>> specs)
    {
        auto config = New<TDistributedThrottlerServiceConfig>();
        for (const auto& [id, spec] : specs) {
            auto bucket = New<TDistributedThrottlerBucketConfig>();
            bucket->Throttler = spec->BuildThroughputConfig();
            bucket->ClassWeights = spec->BuildClassWeights();
            config->Throttlers.emplace(id, bucket);
        }
        Service_->Reconfigure(config);
    }

    IJobThrottlerFactoryPtr MakeFactory(
        std::initializer_list<std::pair<std::string, TDynamicThrottlerSpecPtr>> specs)
    {
        WorkerFactory_->Reconfigure(MakeSpecs(specs));
        return WorkerFactory_->CreateFactory(ToString(++NextClientId_),
            CreateSyncStatusProfiler(),
            NLogging::TLogger("TestClient"),
            {});
    }

    static TDynamicThrottlerSpecPtr MakeSpec(double limit, EThrottlerMode mode = EThrottlerMode::Prefetching)
    {
        auto spec = New<TDynamicThrottlerSpec>();
        spec->Mode = mode;
        spec->Limit = limit;
        spec->RequestPeriod = TDuration::MilliSeconds(50);
        spec->RpcTimeout = TDuration::Minutes(1);
        return spec;
    }

    static THashMap<NFlow::TThrottlerId, TDynamicThrottlerSpecPtr> MakeSpecs(
        std::initializer_list<std::pair<std::string, TDynamicThrottlerSpecPtr>> specs)
    {
        THashMap<NFlow::TThrottlerId, TDynamicThrottlerSpecPtr> result;
        for (const auto& [id, spec] : specs) {
            result.emplace(NFlow::TThrottlerId(id), spec);
        }
        return result;
    }
};

TEST_F(TDistributedThrottlerFactoryTest, ReadyQuotaIsConsumedSynchronously)
{
    auto spec = MakeSpec(1000);
    ReconfigureServer({{"api", spec}});
    auto factory = MakeFactory({{"api", spec}});
    auto handle = factory->GetClientOrThrow("api");
    handle->Release(10);
    auto ready = handle->Throttle(7);
    EXPECT_TRUE(ready.IsSet());
    EXPECT_EQ(handle->GetAvailable(), 3);
    EXPECT_TRUE(WaitFor(ready).IsOK());
}

TEST_F(TDistributedThrottlerFactoryTest, WorkerReconfigurationPreservesHandlesAndQuota)
{
    auto factory = MakeFactory({});
    EXPECT_FALSE(factory->TryGetClient("api"));
    EXPECT_THROW_WITH_SUBSTRING(factory->GetClientOrThrow("api"), "not configured");

    auto spec = MakeSpec(1000);
    ReconfigureServer({{"api", spec}});
    WorkerFactory_->Reconfigure(MakeSpecs({{"api", spec}}));
    auto handle = factory->GetClientOrThrow("api");
    factory->SetQuotaClasses({{"api", "vip"}});
    ASSERT_TRUE(WaitFor(handle->Throttle(7)).IsOK());

    ReconfigureServer({{"api", MakeSpec(0)}});
    handle->Release(7);
    WorkerFactory_->Reconfigure(MakeSpecs({{"api", CloneYsonStruct(spec)}}));
    EXPECT_EQ(handle, factory->GetClientOrThrow("api"));
    EXPECT_TRUE(handle->TryAcquire(7));

    WorkerFactory_->Reconfigure({});
    EXPECT_FALSE(factory->TryGetClient("api"));
    EXPECT_FALSE(WaitFor(handle->Throttle(1)).IsOK());
    WorkerFactory_->Reconfigure(MakeSpecs({{"api", spec}}));
    EXPECT_EQ(handle, factory->TryGetClient("api"));
}

TEST_F(TDistributedThrottlerFactoryTest, TerminalQuotaErrorPersistsUntilClientReplacement)
{
    auto spec = MakeSpec(1000);
    auto factory = MakeFactory({{"api", spec}});
    auto handle = factory->GetClientOrThrow("api");
    handle->Release(1);
    ASSERT_EQ(handle->GetAvailable(), 1);

    WaitFor(ServerInvoker_->Suspend())
        .ThrowOnError();
    auto first = handle->Throttle(2);
    auto second = handle->Throttle(3);
    EXPECT_FALSE(first.IsSet());
    EXPECT_FALSE(second.IsSet());
    ServerInvoker_->Resume();

    EXPECT_THAT(ToString(WaitFor(first)), ::testing::HasSubstr("Unknown throttler"));
    EXPECT_THAT(ToString(WaitFor(second)), ::testing::HasSubstr("Unknown throttler"));
    ReconfigureServer({{"api", spec}});

    EXPECT_THAT(ToString(WaitFor(handle->Throttle(1))), ::testing::HasSubstr("Unknown throttler"));
    EXPECT_THROW_WITH_SUBSTRING(handle->TryAcquire(1), "Unknown throttler");
    EXPECT_THROW_WITH_SUBSTRING(handle->TryAcquireAvailable(1), "Unknown throttler");
    EXPECT_THROW_WITH_SUBSTRING(handle->Acquire(1), "Unknown throttler");
    EXPECT_THROW_WITH_SUBSTRING(handle->Release(1), "Unknown throttler");
    EXPECT_THROW_WITH_SUBSTRING(handle->GetAvailable(), "Unknown throttler");
    EXPECT_THROW_WITH_SUBSTRING(handle->IsOverdraft(), "Unknown throttler");
    EXPECT_THROW_WITH_SUBSTRING(handle->GetQueueTotalAmount(), "Unknown throttler");
    EXPECT_THROW_WITH_SUBSTRING(handle->GetEstimatedOverdraftDuration(), "Unknown throttler");

    auto updated = CloneYsonStruct(spec);
    updated->RpcTimeout = TDuration::Seconds(10);
    WorkerFactory_->Reconfigure(MakeSpecs({{"api", updated}}));
    EXPECT_EQ(handle, factory->GetClientOrThrow("api"));
    EXPECT_TRUE(WaitFor(handle->Throttle(1)).IsOK());
}

TEST_F(TDistributedThrottlerFactoryTest, PendingCallsSurviveClientReplacement)
{
    auto spec = MakeSpec(1000);
    ReconfigureServer({{"api", spec}});
    auto factory = MakeFactory({{"api", spec}});
    auto handle = factory->GetClientOrThrow("api");
    WaitFor(ServerInvoker_->Suspend())
        .ThrowOnError();
    auto pending = handle->Throttle(1);
    auto canceled = handle->Throttle(1);
    EXPECT_FALSE(pending.IsSet());
    EXPECT_FALSE(canceled.IsSet());

    auto updated = CloneYsonStruct(spec);
    updated->RpcTimeout = TDuration::Seconds(10);
    WorkerFactory_->Reconfigure(MakeSpecs({{"api", updated}}));
    canceled.Cancel(TError("Canceled by caller"));
    EXPECT_THAT(ToString(WaitFor(canceled.WithTimeout(TDuration::Seconds(3)))), ::testing::HasSubstr("Canceled by caller"));
    ServerInvoker_->Resume();
    EXPECT_TRUE(WaitFor(pending.WithTimeout(TDuration::Seconds(3))).IsOK());
}

TEST_F(TDistributedThrottlerFactoryTest, PendingRequestKeepsOriginalTimeout)
{
    auto spec = MakeSpec(0);
    spec->ThrottleTimeout = TDuration::MilliSeconds(100);
    ReconfigureServer({{"api", spec}});
    auto factory = MakeFactory({{"api", spec}});
    auto handle = factory->GetClientOrThrow("api");
    auto pending = handle->Throttle(1);
    auto updated = CloneYsonStruct(spec);
    updated->ThrottleTimeout = TDuration::Minutes(1);
    WorkerFactory_->Reconfigure(MakeSpecs({{"api", updated}}));
    auto result = WaitFor(pending.WithTimeout(TDuration::Seconds(3)));
    EXPECT_TRUE(result.FindMatching(NYT::EErrorCode::Timeout));
    EXPECT_THAT(ToString(result), ::testing::HasSubstr("Error waiting for distributed throttler"));

    ReconfigureServer({{"api", MakeSpec(1000)}});
    EXPECT_TRUE(WaitFor(handle->Throttle(1).WithTimeout(TDuration::Seconds(3))).IsOK());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NDistributedThrottler
