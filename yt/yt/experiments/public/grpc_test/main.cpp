#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/rpc/server.h>
#include <yt/yt/core/rpc/service_detail.h>

#include <yt/yt/core/rpc/grpc/config.h>
#include <yt/yt/core/rpc/grpc/server.h>
#include <yt/yt/core/rpc/grpc/channel.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/experiments/public/grpc_test/main.pb.h>

#include <library/cpp/getopt/last_getopt.h>

namespace NYT::NGrpcTest {

using namespace NRpc;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static inline const NLogging::TLogger Logger("TestMain");

////////////////////////////////////////////////////////////////////////////////

class TTestProxy
    : public TProxyBase
{
public:
    DEFINE_RPC_PROXY(TTestProxy, NYT.NGrpcTest.NProto.TestService);

    DEFINE_RPC_PROXY_METHOD(NGrpcTest::NProto, TestCall);
    DEFINE_RPC_PROXY_METHOD(NGrpcTest::NProto, TestFailedCall);
};

class TTestService
    : public TServiceBase
{
public:
    using TPtr = TIntrusivePtr<TTestService>;
    using TThis = TTestService;

    explicit TTestService(IInvokerPtr invoker)
        : TServiceBase(
            invoker,
            TTestProxy::GetDescriptor(),
            NGrpcTest::Logger)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(TestCall)
            .SetQueueSizeLimit(10000000));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(TestFailedCall)
            .SetQueueSizeLimit(10000000));
    }

    DECLARE_RPC_SERVICE_METHOD(NGrpcTest::NProto, TestCall);
    DECLARE_RPC_SERVICE_METHOD(NGrpcTest::NProto, TestFailedCall);
};

DEFINE_RPC_SERVICE_METHOD(TTestService, TestCall)
{
    context->SetRequestInfo("a: %v", request->a());
    response->set_b(request->a() + 42);
    context->SetResponseInfo("b: %v", response->b());
    context->Reply();
}

DEFINE_RPC_SERVICE_METHOD(TTestService, TestFailedCall)
{
    context->Reply(TError("Error"));
}

void RunServer(const TString& address)
{
    auto serverAddressConfig = New<NGrpc::TServerAddressConfig>();
    serverAddressConfig->Address = address;

    auto serverConfig = New<NGrpc::TServerConfig>();
    serverConfig->Addresses.push_back(serverAddressConfig);

    auto rpcServer = NGrpc::CreateServer(serverConfig);

    auto queue = New<TActionQueue>("RPC");
    rpcServer->RegisterService(New<TTestService>(queue->GetInvoker()));
    rpcServer->Start();

    Cin.ReadLine();

    rpcServer->Stop()
        .Get()
        .ThrowOnError();
}

void RunClient(const TString& address, int numIter)
{
    Cout << "Running " << numIter << " iterations of test" << Endl;

    auto channelConfig = New<NGrpc::TChannelConfig>();
    channelConfig->Address = address;

    auto channel = NGrpc::CreateGrpcChannel(channelConfig);

    NProfiling::TWallTimer timer;

    TTestProxy proxy(channel);
    proxy.SetDefaultTimeout(TDuration::MilliSeconds(100));
    TFuture<TTestProxy::TRspTestCallPtr> result;
    for (int i = 0; i < numIter; ++i) {
        auto request = proxy.TestCall();
        request->set_a(i);
        result = request->Invoke();
        if (i % 10000 == 0 || i == numIter - 1) {
            Cout << "iteration " << i << Endl;
            auto response = result.Get().ValueOrThrow();
            Y_ASSERT(response->b() == i + 42);
        }
    }
    result.Get();

    auto elapsed = timer.GetElapsedTime();
    Cout << "Elapsed = " << ToString(elapsed) << Endl;
    Cout << "RPS = " << (double) numIter / elapsed.SecondsFloat() << Endl;

}
////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NGrpcTest

int main(int argc, const char *argv[])
{
    try {
        using namespace NLastGetopt;
        using namespace NYT;
        using namespace NYT::NGrpcTest;

        TOpts opts;
        opts.AddHelpOption();

        TString mode;
        opts.AddLongOption("mode", "")
            .RequiredArgument("MODE")
            .StoreResult(&mode);

        TString address = "localhost:8888";
        opts.AddLongOption("address", "")
            .RequiredArgument("ADDRESS")
            .StoreResult(&address);

        int numIter = 1000;
        opts.AddLongOption("iters", "")
            .RequiredArgument("NUM")
            .StoreResult(&numIter);

        TOptsParseResult results(&opts, argc, argv);

        if (mode == "server") {
            RunServer(address);
        } else if (mode == "client") {
            RunClient(address, numIter);
        } else {
            THROW_ERROR_EXCEPTION("Unknown mode");
        }
    }
    catch (const std::exception& ex) {
        Cerr << "ERROR: " << ex.what() << Endl;
    }
}

