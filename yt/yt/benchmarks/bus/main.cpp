#include <yt/yt/core/bus/bus.h>
#include <yt/yt/core/bus/server.h>

#include <yt/yt/core/bus/tcp/client.h>
#include <yt/yt/core/bus/tcp/config.h>
#include <yt/yt/core/bus/tcp/server.h>

#include <yt/yt/core/concurrency/delayed_executor.h>

#include <yt/yt/core/profiling/timing.h>

#include <library/cpp/getopt/last_getopt.h>

namespace NYT {

using namespace NBus;
using namespace NConcurrency;
using namespace NLogging;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

static const TLogger BusBenchmarkLogger("BusBenchmark");
static const auto& Logger = BusBenchmarkLogger;

////////////////////////////////////////////////////////////////////////////////

class TEchoServer
    : public IMessageHandler
{
public:
    void HandleMessage(
        TSharedRefArray message,
        IBusPtr replyBus) noexcept override
    {
        YT_UNUSED_FUTURE(replyBus->Send(message, {.TrackingLevel = EDeliveryTrackingLevel::Full})
            .ToUncancelable());
    }
};

////////////////////////////////////////////////////////////////////////////////

void RunServer(int port)
{
    auto busServer = CreateBusServer(TBusServerConfig::CreateTcp(port));
    auto echoServer = New<TEchoServer>();

    busServer->Start(echoServer);

    YT_LOG_INFO("Echo server is running on port %v", port);

    TDelayedExecutor::WaitForDuration(TDuration::Max());
}

////////////////////////////////////////////////////////////////////////////////

class TEchoReceiver
    : public IMessageHandler
{
public:
    void ResetPromise()
    {
        ReplyPromise_ = NewPromise<TSharedRefArray>();
    }

    TSharedRefArray GetResponse()
    {
        return WaitFor(ReplyPromise_.ToFuture())
            .ValueOrThrow();
    }

    void HandleMessage(
        TSharedRefArray message,
        IBusPtr /*replyBus*/) noexcept override
    {
        ReplyPromise_.Set(message);
    }

private:
    TPromise<TSharedRefArray> ReplyPromise_ = NewPromise<TSharedRefArray>();
};

////////////////////////////////////////////////////////////////////////////////

void RunBenchmark(int port, int iterations)
{
    auto client = CreateBusClient(TBusClientConfig::CreateTcp(Format("localhost:%v", port)));
    auto handler = New<TEchoReceiver>();

    auto bus = client->CreateBus(handler);

    TWallTimer timer;

    for (int iteration = 0; iteration < iterations; ++iteration) {
        auto message = ToString(iteration);
        auto array = TSharedRefArray(TSharedRef::FromString(message));

        handler->ResetPromise();
        WaitFor(bus->Send(array, {.TrackingLevel = EDeliveryTrackingLevel::Full}))
            .ThrowOnError();
        auto response = handler->GetResponse();

        YT_VERIFY(ToString(response.ToVector()[0]) == message);
    }

    YT_LOG_INFO("Processed %v requests in %v", iterations, timer.GetElapsedTime());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

int main(int argc, char* argv[])
{
    using namespace NLastGetopt;

    TOpts opts;

    TString mode;
    opts.AddLongOption("mode", "")
        .RequiredArgument("MODE")
        .StoreResult(&mode);

    int port;
    opts.AddLongOption("port", "")
        .RequiredArgument("PORT")
        .StoreResult(&port);

    int iterations = 1;
    opts.AddLongOption("iterations", "")
        .OptionalArgument("ITERATIONS")
        .StoreResult(&iterations);

    TOptsParseResult results(&opts, argc, argv);

    if (mode == "server") {
        NYT::RunServer(port);
    } else if (mode == "client") {
        NYT::RunBenchmark(port, iterations);
    } else {
        YT_ABORT();
    }

    return 0;
}
