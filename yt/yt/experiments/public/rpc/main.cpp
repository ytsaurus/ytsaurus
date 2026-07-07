#include <yt/yt/core/bus/tcp/config.h>

#include <yt/yt/core/concurrency/action_queue.h>
#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/misc/config.h>
#include <yt/yt/core/misc/configurable_singleton_def.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/core/rpc/client.h>
#include <yt/yt/core/rpc/server.h>
#include <yt/yt/core/rpc/service_detail.h>
#include <yt/yt/core/rpc/config.h>
#include <yt/yt/core/rpc/multi_protocol_channel_factory.h>
#include <yt/yt/core/rpc/multi_protocol_server.h>

#include <yt/yt/experiments/public/rpc/main.pb.h>

#include <library/cpp/yt/assert/assert.h>

#include <library/cpp/getopt/last_getopt.h>

namespace NYT {

using namespace NRpc;
using namespace NYT::NBus;
using namespace NYT::NBus::NTcp;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const NLogging::TLogger Logger("TestMain");

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TProgramConfig)

struct TProgramConfig
    : public TSingletonsConfig
{
    TMultiProtocolClientConfigPtr Client;
    TMultiProtocolServerConfigPtr Server;

    REGISTER_YSON_STRUCT(TProgramConfig);

    static void Register(TRegistrar registrar)
    {
        registrar.Parameter("client", &TThis::Client)
            .DefaultNew();
        registrar.Parameter("server", &TThis::Server)
            .Default();
    }
};

DEFINE_REFCOUNTED_TYPE(TProgramConfig)

////////////////////////////////////////////////////////////////////////////////

class TTestProxy
    : public TProxyBase
{
public:
    DEFINE_RPC_PROXY(TTestProxy, Test);

    DEFINE_RPC_PROXY_METHOD(NRpcTest, TestCall);
};

////////////////////////////////////////////////////////////////////////////////

class TTestService
    : public TServiceBase
{
public:
    explicit TTestService(IInvokerPtr invoker)
        : TServiceBase(
            invoker,
            TTestProxy::GetDescriptor(),
            NYT::Logger)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(TestCall)
            .SetQueueSizeLimit(10'000'000));
    }

    DECLARE_RPC_SERVICE_METHOD(NRpcTest, TestCall);
};

DEFINE_RPC_SERVICE_METHOD(TTestService, TestCall)
{
    int a = request->a();
    response->set_b(a + 42);
    context->Reply();
}

////////////////////////////////////////////////////////////////////////////////

void RunRpcServer(const IServerPtr& rpcServer)
{
    auto queue = New<TActionQueue>("Rpc");

    rpcServer->RegisterService(New<TTestService>(queue->GetInvoker()));
    rpcServer->Start();

    Cin.ReadLine();
}

void RunRpcClient(const IChannelFactoryPtr& channelFactory, const std::string& address, int numIter)
{
    Cout << "Running " << numIter << " iterations of test" << Endl;

    NProfiling::TWallTimer timer;

    auto channel = channelFactory->CreateChannel(address);
    TTestProxy proxy(channel);
    proxy.SetDefaultAcknowledgementTimeout(std::nullopt);
    TFuture<TTestProxy::TRspTestCallPtr> result;
    for (int i = 0; i < numIter; ++i) {
        auto request = proxy.TestCall();
        request->set_a(i);
        result = request->Invoke();
        if (i % 10'000 == 0) {
            Cout << "iteration " << i << Endl;
            auto response = WaitFor(result)
                .ValueOrThrow();
            YT_VERIFY(response->b() == i + 42);
        }
    }
    WaitFor(result)
        .ThrowOnError();

    auto seconds = timer.GetElapsedTime().SecondsFloat();

    Cout << Format("RPS test complete: %v sec, %v req/sec",
        seconds,
        numIter / seconds) << Endl;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

int main(int argc, const char* argv[])
{
    using namespace NYT;
    using namespace NLastGetopt;

    try {
        TOpts opts;

        opts.AddHelpOption();

        std::string mode;
        opts.AddLongOption("mode", "")
            .RequiredArgument("MODE")
            .StoreResult(&mode);

        int port = 8888;
        opts.AddLongOption("port", "")
            .RequiredArgument("PORT")
            .StoreResult(&port);

        int numIter = 1000;
        opts.AddLongOption("iters", "")
            .RequiredArgument("NUM")
            .StoreResult(&numIter);

        std::string address = "localhost:8888";
        opts.AddLongOption("address", "")
            .RequiredArgument("ADDRESS")
            .StoreResult(&address);

        std::string configFileName;
        opts.AddLongOption("config", "configuration file")
            .RequiredArgument("FILE")
            .StoreResult(&configFileName);

        TOptsParseResult results(&opts, argc, argv);

        TProgramConfigPtr config;
        if (configFileName.empty()) {
            config = New<TProgramConfig>();
            config->Postprocess();
        } else {
            // TODO(babenko): drop TString cast once TUnbufferedFileInput accepts std::string.
            auto configYson = TUnbufferedFileInput(TString(configFileName)).ReadAll();
            config = ConvertTo<TProgramConfigPtr>(NYson::TYsonString(configYson));
        }

        TSingletonManager::Configure(config);

        auto channelFactory = CreateMultiProtocolChannelFactory(config->Client);

        if (mode == "rpc-server") {
            auto serverConfig = config->Server;
            if (!serverConfig) {
                // No server section: default to plain yt-tcp on the requested port.
                serverConfig = New<TMultiProtocolServerConfig>();
                *serverConfig->MutableTypedConfig<TBusServerConfig>(DefaultProtocolName) =
                    TBusServerConfig::CreateTcp(port);
            }
            RunRpcServer(CreateMultiProtocolServer(serverConfig));
        } else if (mode == "rpc-client") {
            RunRpcClient(channelFactory, address, numIter);
        } else {
            THROW_ERROR_EXCEPTION("Unknown mode");
        }
    }
    catch (const std::exception& ex) {
        Cerr << "ERROR: " << ex.what() << Endl;
    }
}
