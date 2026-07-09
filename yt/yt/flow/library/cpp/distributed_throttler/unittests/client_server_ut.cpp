#include <yt/yt/flow/library/cpp/distributed_throttler/client.h>
#include <yt/yt/flow/library/cpp/distributed_throttler/config.h>
#include <yt/yt/flow/library/cpp/distributed_throttler/server.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/config.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/rpc/local_channel.h>
#include <yt/yt/core/rpc/local_server.h>
#include <yt/yt/core/rpc/server.h>

#include <yt/yt/core/test_framework/framework.h>

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
        std::function<ui64()> timestampProvider = {})
    {
        auto clientConfig = New<TDistributedThrottlerClientConfig>();
        clientConfig->ThrottlerName = throttlerName;
        clientConfig->ClientId = clientId;

        // Fast prefetching for tests.
        clientConfig->PrefetchingConfig->TargetRps = 10.0;
        clientConfig->PrefetchingConfig->MinPrefetchAmount = 1;
        clientConfig->PrefetchingConfig->MaxPrefetchAmount = 100;

        auto channel = NRpc::CreateLocalChannel(LocalServer_);
        return CreateDistributedThrottler(
            std::move(clientConfig),
            [channel] {
                return channel;
            },
            std::move(timestampProvider),
            /*statusProfiler*/ nullptr,
            NLogging::TLogger("TestClient"),
            /*profiler*/ {});
    }

    TDistributedThrottlerServiceConfigPtr MakeServerConfig(
        std::initializer_list<std::pair<std::string, std::optional<double>>> throttlers)
    {
        auto config = New<TDistributedThrottlerServiceConfig>();
        for (const auto& [name, limit] : throttlers) {
            auto tc = New<TThroughputThrottlerConfig>();
            tc->Limit = limit;
            config->Throttlers[name] = tc;
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
    auto client = CreateClient("test");

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

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow::NDistributedThrottler
