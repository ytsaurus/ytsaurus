#include <benchmark/benchmark.h>

#include <yt/yt/core/bus/bus.h>
#include <yt/yt/core/bus/client.h>
#include <yt/yt/core/bus/server.h>

#include <yt/yt/core/bus/tcp/config.h>
#include <yt/yt/core/bus/tcp/client.h>
#include <yt/yt/core/bus/tcp/server.h>

#include <library/cpp/testing/common/network.h>

namespace NYT::NBus {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TNullHandler
    : public IMessageHandler
{
public:
    void HandleMessage(
        TSharedRefArray /*message*/,
        IBusPtr /*replyBus*/) noexcept override
    { }
};

IBusPtr ClientBus;

////////////////////////////////////////////////////////////////////////////////

static void BenchmarkSend(benchmark::State& state, const TSendOptions& options)
{
    IBusServerPtr server;
    NTesting::TPortHolder portHolder;
    if (state.thread_index() == 0) {
        portHolder = NTesting::GetFreePort();
        auto address = Format("localhost:%v", static_cast<ui16>(portHolder));

        auto config = TBusServerConfig::CreateTcp(static_cast<ui16>(portHolder));
        server = CreateBusServer(config);
        server->Start(New<TNullHandler>());

        auto client = CreateBusClient(TBusClientConfig::CreateTcp(address));
        ClientBus = client->CreateBus(New<TNullHandler>());
    }

    TSharedRefArray nullMessage;

    const int batchSize = 1024;

    while (state.KeepRunningBatch(batchSize)) {
        std::vector<TFuture<void>> futureBatch;
        for (int i = 0; i < batchSize; ++i) {
            futureBatch.emplace_back(ClientBus->Send(nullMessage, options));
        }

        AllSet(futureBatch).Get();
    }

    if (state.thread_index() == 0) {
        YT_UNUSED_FUTURE(server->Stop());
        ClientBus.Reset();
    }
}

TSendOptions DefaultSendOptions()
{
    TSendOptions options;
    options.TrackingLevel = EDeliveryTrackingLevel::Full;
    return options;
}

TSendOptions CancelSendOptions()
{
    TSendOptions options;
    options.TrackingLevel = EDeliveryTrackingLevel::Full;
    options.EnableSendCancelation = true;
    return options;
}

BENCHMARK_CAPTURE(BenchmarkSend, Default, DefaultSendOptions())
    ->Threads(2);

BENCHMARK_CAPTURE(BenchmarkSend, SendCancel, CancelSendOptions())
    ->Threads(2);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus
} // namespace
