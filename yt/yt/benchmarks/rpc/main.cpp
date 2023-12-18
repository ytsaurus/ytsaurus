#include <yt/yt/benchmarks/rpc/main.pb.h>

#include <yt/yt/core/bus/tcp/client.h>
#include <yt/yt/core/bus/tcp/config.h>
#include <yt/yt/core/bus/tcp/server.h>

#include <yt/yt/core/concurrency/delayed_executor.h>

#include <yt/yt/core/rpc/bus/channel.h>
#include <yt/yt/core/rpc/bus/server.h>

#include <yt/yt/core/rpc/client.h>
#include <yt/yt/core/rpc/server.h>
#include <yt/yt/core/rpc/service_detail.h>

#include <library/cpp/getopt/last_getopt.h>

namespace NYT {

using namespace NBus;
using namespace NConcurrency;
using namespace NLogging;
using namespace NProfiling;
using namespace NRpc;
using namespace NRpc::NBus;

////////////////////////////////////////////////////////////////////////////////

static const TLogger RpcBenchmarkLogger("RpcBenchmark");
static const auto& Logger = RpcBenchmarkLogger;

////////////////////////////////////////////////////////////////////////////////

class TEchoProxy
    : public TProxyBase
{
public:
    DEFINE_RPC_PROXY(TEchoProxy, Echo);

    DEFINE_RPC_PROXY_METHOD(NBenchmarkRpc, Echo);
};

////////////////////////////////////////////////////////////////////////////////

class TEchoService
    : public TServiceBase
{
public:
    explicit TEchoService(IInvokerPtr invoker)
        : TServiceBase(
            std::move(invoker),
            TEchoProxy::GetDescriptor(),
            RpcBenchmarkLogger)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(Echo)
            .SetQueueSizeLimit(100'000'000));
    }

    DECLARE_RPC_SERVICE_METHOD(NBenchmarkRpc, Echo);
};

DEFINE_RPC_SERVICE_METHOD(TEchoService, Echo)
{
    auto s = request->s();
    response->set_s(s);

    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

void RunServer(int port)
{
    auto busServer = CreateBusServer(TBusServerConfig::CreateTcp(port));
    auto rpcServer = CreateBusServer(std::move(busServer));

    auto echoQueue = New<TActionQueue>("Echo");

    rpcServer->RegisterService(New<TEchoService>(echoQueue->GetInvoker()));
    rpcServer->Start();

    YT_LOG_INFO("Echo server is running on port %v", port);

    TDelayedExecutor::WaitForDuration(TDuration::Max());
}

////////////////////////////////////////////////////////////////////////////////

void RunBenchmark(int port, int iterations)
{
    auto client = CreateBusClient(TBusClientConfig::CreateTcp(Format("localhost:%v", port)));
    auto channel = CreateBusChannel(client);
    TEchoProxy proxy(channel);
    proxy.SetDefaultAcknowledgementTimeout(std::nullopt);

    TWallTimer timer;

    for (int iteration = 0; iteration < iterations; ++iteration) {
        auto message = ToString(iteration);
        auto req = proxy.Echo();
        req->set_s(message);
        auto rsp = WaitFor(req->Invoke())
            .ValueOrThrow();
        YT_VERIFY(rsp->s() == message);
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
