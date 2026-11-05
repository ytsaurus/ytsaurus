#include <yt/yt/flow/library/cpp/distributed_throttler/client.h>
#include <yt/yt/flow/library/cpp/distributed_throttler/config.h>
#include <yt/yt/flow/library/cpp/distributed_throttler/server.h>
#include <yt/yt/flow/library/cpp/distributed_throttler/service_proxy.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/config.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/rpc/local_channel.h>
#include <yt/yt/core/rpc/local_server.h>
#include <yt/yt/core/rpc/server.h>

#include <yt/yt/core/test_framework/framework.h>

#include <algorithm>
#include <mutex>
#include <vector>

namespace NYT::NFlow::NDistributedThrottler {
namespace {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TDistributedThrottlerClientServerTest
    : public ::testing::Test
{
protected:
    void SetUp() override
    {
        ActionQueue_ = New<TActionQueue>("Test");
        LocalServer_ = NRpc::CreateLocalServer();
    }

    void TearDown() override
    {
        YT_UNUSED_FUTURE(LocalServer_->Stop());
        ActionQueue_->Shutdown();
    }

    void StartServer(TDistributedThrottlerServiceConfigPtr config)
    {
        Service_ = CreateDistributedThrottlerService(
            std::move(config),
            ActionQueue_->GetInvoker(),
            NLogging::TLogger("Test"));
        LocalServer_->RegisterService(Service_->GetRpcService());
        LocalServer_->Start();
    }

    IThroughputThrottlerPtr CreateClient(
        const std::string& throttlerName,
        const std::string& clientId = "default",
        std::function<ui64()> timestampProvider = {},
        std::function<TQuotaClassId()> quotaClassProvider = {},
        i64 maxPrefetchAmount = 100)
    {
        auto clientConfig = New<TDistributedThrottlerClientConfig>();
        clientConfig->ThrottlerName = throttlerName;
        clientConfig->ClientId = clientId;

        // Fast prefetching for tests.
        clientConfig->PrefetchingConfig->TargetRps = 10.0;
        clientConfig->PrefetchingConfig->MinPrefetchAmount = 1;
        clientConfig->PrefetchingConfig->MaxPrefetchAmount = maxPrefetchAmount;

        auto channel = NRpc::CreateLocalChannel(LocalServer_);
        return CreateDistributedThrottler(
            std::move(clientConfig),
            [channel] {
                return channel;
            },
            std::move(timestampProvider),
            std::move(quotaClassProvider),
            /*statusProfiler*/ nullptr,
            NLogging::TLogger("TestClient"),
            /*profiler*/ {});
    }

    IThroughputThrottlerPtr CreateLegacyClient(const std::string& throttlerName)
    {
        auto clientConfig = New<TDistributedThrottlerClientConfig>();
        clientConfig->ThrottlerName = throttlerName;
        clientConfig->ClientId = "legacy";
        clientConfig->PrefetchingConfig->TargetRps = 10.0;
        clientConfig->PrefetchingConfig->MinPrefetchAmount = 1;
        clientConfig->PrefetchingConfig->MaxPrefetchAmount = 1;

        auto channel = NRpc::CreateLocalChannel(LocalServer_);
        return CreateDistributedThrottler(
            std::move(clientConfig),
            [channel] {
                return channel;
            },
            /*priorityProvider*/ {},
            /*quotaClassProvider*/ {},
            /*statusProfiler*/ nullptr,
            NLogging::TLogger("LegacyTestClient"),
            /*profiler*/ {});
    }

    TDistributedThrottlerServiceConfigPtr MakeServerConfig(
        std::initializer_list<std::pair<std::string, std::optional<double>>> throttlers,
        std::optional<i64> maxGrantAmount = {})
    {
        auto config = New<TDistributedThrottlerServiceConfig>();
        config->DrainPeriod = TDuration::MilliSeconds(10);
        for (const auto& [name, limit] : throttlers) {
            auto bucketConfig = New<TDistributedThrottlerBucketConfig>();
            bucketConfig->Throttler->Limit = limit;
            bucketConfig->Throttler->Period = TDuration::MilliSeconds(100);
            bucketConfig->MaxGrantAmount = maxGrantAmount;
            config->Throttlers[name] = bucketConfig;
        }
        return config;
    }

    TActionQueuePtr ActionQueue_;
    NRpc::IServerPtr LocalServer_;
    IDistributedThrottlerServicePtr Service_;
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TDistributedThrottlerClientServerTest, BasicThrottle)
{
    StartServer(MakeServerConfig({{"test", 1000}}));
    auto client = CreateLegacyClient("test");

    auto result = WaitFor(client->Throttle(1));
    EXPECT_TRUE(result.IsOK());
}

TEST_F(TDistributedThrottlerClientServerTest, UnlimitedThrottler)
{
    StartServer(MakeServerConfig({{"test", std::nullopt}}));
    auto client = CreateClient("test");

    for (int i = 0; i < 100; ++i) {
        auto result = WaitFor(client->Throttle(1));
        EXPECT_TRUE(result.IsOK());
    }
}

TEST_F(TDistributedThrottlerClientServerTest, UnknownThrottler)
{
    StartServer(MakeServerConfig({{"test", 1000}}));
    auto client = CreateClient("nonexistent");

    auto result = WaitFor(client->Throttle(1));
    EXPECT_FALSE(result.IsOK());
}

TEST_F(TDistributedThrottlerClientServerTest, ConfiguredQuotaClassCrossesRpc)
{
    auto config = MakeServerConfig({{"test", 1}});
    config->Throttlers["test"]->ClassWeights = {{"vip", 5.0}};
    StartServer(std::move(config));

    auto warmup = CreateLegacyClient("test");
    EXPECT_TRUE(WaitFor(warmup->Throttle(1)).IsOK());

    auto defaultFirst = CreateClient(
        "test",
        "default-first",
        [] {
            return 0;
        },
        {},
        1);
    auto defaultSecond = CreateClient(
        "test",
        "default-second",
        [] {
            return 1;
        },
        {},
        1);
    auto vip = CreateClient(
        "test",
        "vip",
        [] {
            return 1000;
        },
        [] {
            return TQuotaClassId("vip");
        },
        1);

    std::vector<std::string> completionOrder;
    std::mutex completionOrderLock;
    auto recordCompletion = [&] (const std::string& name) {
        return BIND([&, name] (const TError&) {
            std::lock_guard guard(completionOrderLock);
            completionOrder.push_back(name);
        });
    };

    auto firstFuture = defaultFirst->Throttle(1);
    firstFuture.Subscribe(recordCompletion("first"));
    auto secondFuture = defaultSecond->Throttle(1);
    secondFuture.Subscribe(recordCompletion("second"));
    Sleep(TDuration::MilliSeconds(20));
    auto vipFuture = vip->Throttle(1);
    vipFuture.Subscribe(recordCompletion("vip"));

    EXPECT_TRUE(WaitFor(AllSucceeded(std::vector{firstFuture, secondFuture, vipFuture})).IsOK());
    std::lock_guard guard(completionOrderLock);
    EXPECT_LT(
        std::find(completionOrder.begin(), completionOrder.end(), "vip"),
        std::find(completionOrder.begin(), completionOrder.end(), "second"));
}

TEST_F(TDistributedThrottlerClientServerTest, UnknownClassUsesDefaultOrdering)
{
    auto config = MakeServerConfig({{"test", 1}});
    config->Throttlers["test"]->ClassWeights = {{"vip", 1.0}};
    StartServer(std::move(config));

    auto warmup = CreateLegacyClient("test");
    EXPECT_TRUE(WaitFor(warmup->Throttle(1)).IsOK());
    auto blocker = CreateClient("test", "blocker", [] {
        return 0;
    },
        {},
        1);
    auto unknown = CreateClient(
        "test",
        "unknown",
        [] {
            return 200;
        },
        [] {
            return TQuotaClassId("unknown");
        },
        1);
    auto defaultClient = CreateClient("test", "default", [] {
        return 100;
    },
        {},
        1);

    auto blockerFuture = blocker->Throttle(1);
    Sleep(TDuration::MilliSeconds(20));
    auto unknownFuture = unknown->Throttle(1);
    auto defaultFuture = defaultClient->Throttle(1);

    EXPECT_TRUE(WaitFor(blockerFuture).IsOK());
    EXPECT_TRUE(WaitFor(defaultFuture).IsOK());
    EXPECT_FALSE(unknownFuture.IsSet());
    EXPECT_TRUE(WaitFor(unknownFuture).IsOK());
}

TEST_F(TDistributedThrottlerClientServerTest, MissingFieldUsesDefault)
{
    StartServer(MakeServerConfig({{"test", 1000}}));
    auto channel = NRpc::CreateLocalChannel(LocalServer_);
    TDistributedThrottlerServiceProxy proxy(channel);
    auto request = proxy.RequestQuota();
    NRpc::GenerateMutationId(request);
    request->set_throttler_id("test");
    request->set_client_id("old-worker");
    request->set_amount(1);
    request->set_timestamp(0);

    EXPECT_TRUE(WaitFor(request->Invoke()).IsOK());
}

TEST_F(TDistributedThrottlerClientServerTest, NegativeAmountIsRejected)
{
    StartServer(MakeServerConfig({{"test", 1000}}));
    auto channel = NRpc::CreateLocalChannel(LocalServer_);
    TDistributedThrottlerServiceProxy proxy(channel);
    auto request = proxy.RequestQuota();
    NRpc::GenerateMutationId(request);
    request->set_throttler_id("test");
    request->set_client_id("rogue");
    request->set_amount(-1);
    request->set_timestamp(0);

    EXPECT_FALSE(WaitFor(request->Invoke()).IsOK());
}

TEST_F(TDistributedThrottlerClientServerTest, CanceledRequestStopsBlockingOtherRequests)
{
    // A canceled RPC must be abandoned on the *server*, not merely locally.
    // Asserting that the client-side future became canceled would pass even if
    // the cancellation never reached the handler, so the observable checked
    // here is that the request stops occupying the serialized drain loop.
    StartServer(MakeServerConfig({{"test", 100}}, /*maxGrantAmount*/ 1));
    auto channel = NRpc::CreateLocalChannel(LocalServer_);
    TDistributedThrottlerServiceProxy proxy(channel);

    auto makeRequest = [&] (const std::string& clientId, i64 amount, ui64 timestamp) {
        auto request = proxy.RequestQuota();
        NRpc::GenerateMutationId(request);
        request->set_throttler_id("test");
        request->set_client_id(clientId);
        request->set_amount(amount);
        request->set_timestamp(timestamp);
        return request;
    };

    // One unit per chunk at 100 units/s keeps the drain loop busy for about ten
    // seconds; the probe sorts after it, so it can only run once the hog is out.
    auto hog = makeRequest("hog", 1000, /*timestamp*/ 0)->Invoke();
    auto probe = makeRequest("probe", 1, /*timestamp*/ 1)->Invoke();

    // Synchronize on the blocked state instead of assuming it: with a 10 ms
    // drain period the loop has iterated many times by now, so an unserved
    // probe proves the hog is the one holding the loop.
    Sleep(TDuration::MilliSeconds(500));
    ASSERT_FALSE(hog.IsSet());
    ASSERT_FALSE(probe.IsSet()) << "probe was served while the hog still held the drain loop";

    hog.Cancel(TError("Canceled by test"));

    // Only a server-side abandon frees the loop; without it the probe waits out
    // the hog's remaining chunks and trips the timeout.
    auto probeResult = WaitFor(probe.WithTimeout(TDuration::Seconds(5)));
    EXPECT_TRUE(probeResult.IsOK()) << ToString(probeResult);
}

TEST_F(TDistributedThrottlerClientServerTest, EmptyQuotaClassUsesCompatibilityPath)
{
    StartServer(MakeServerConfig({{"test", 1000}}));
    auto client = CreateClient(
        "test",
        "default-client",
        {},
        [] {
            return TQuotaClassId();
        });

    EXPECT_TRUE(WaitFor(client->Throttle(1)).IsOK());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NDistributedThrottler
