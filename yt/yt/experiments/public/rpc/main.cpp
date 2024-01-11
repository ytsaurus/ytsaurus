#include <yt/yt/core/actions/cancelable_context.h>

#include <yt/yt/core/bus/bus.h>
#include <yt/yt/core/bus/tcp/config.h>
#include <yt/yt/core/bus/private.h>
#include <yt/yt/core/bus/server.h>
#include <yt/yt/core/bus/tcp/client.h>
#include <yt/yt/core/bus/tcp/server.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/logging/log_manager.h>
#include <yt/yt/core/logging/config.h>

#include <yt/yt/core/misc/blob.h>
#include <yt/yt/core/misc/config.h>
#include <yt/yt/library/coredumper/coredumper.h>

#include <yt/yt/core/rpc/bus/channel.h>
#include <yt/yt/core/rpc/bus/server.h>
#include <yt/yt/core/rpc/client.h>
#include <yt/yt/core/rpc/private.h>
#include <yt/yt/core/rpc/server.h>
#include <yt/yt/core/rpc/service_detail.h>

#include <yt/yt/library/coredumper/coredumper.h>
#include <yt/yt/library/coredumper/config.h>

#include <yt/yt/server/lib/admin/admin_service.h>

#include <library/cpp/deprecated/atomic/atomic.h>
#include <library/cpp/yt/assert/assert.h>

#include <yt/yt/experiments/public/rpc/main.pb.h>

#include <library/cpp/getopt/last_getopt.h>

#include <util/system/event.h>
#include <util/system/hp_timer.h>

#include <util/thread/pool.h>

namespace NYT {

using namespace NRpc;
using namespace NYT::NBus;
using namespace NConcurrency;
using namespace NAdmin;

static const NYT::NLogging::TLogger Logger("TestMain");

////////////////////////////////////////////////////////////////////////////////

class TTestProxy
    : public TProxyBase
{
public:
    DEFINE_RPC_PROXY(TTestProxy, Test);

    DEFINE_RPC_PROXY_METHOD(NRpcTest, TestCall);
};

class TTestService
    : public TServiceBase
{
public:
    using TPtr = TIntrusivePtr<TTestService>;
    using TThis = TTestService;

    TTestService(IInvokerPtr invoker)
        : TServiceBase(
            invoker,
            TTestProxy::GetDescriptor(),
            NYT::Logger)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(TestCall)
            .SetQueueSizeLimit(10000000));
    }

    DECLARE_RPC_SERVICE_METHOD(NRpcTest, TestCall);
};

DEFINE_RPC_SERVICE_METHOD(TTestService, TestCall)
{
    int a = request->a();
    //Cout << "Received message with A = " << a << Endl;
    response->set_b(a + 42);
    context->Reply();
}

void RunRpcServer(int port)
{
    auto busServer = CreateBusServer(TBusServerConfig::CreateTcp(port));

    auto rpcServer = NRpc::NBus::CreateBusServer(busServer);

    auto coreDumperConfig = New<NCoreDump::TCoreDumperConfig>();
    coreDumperConfig->Path = "./";

    auto coreDumper = NCoreDump::CreateCoreDumper(std::move(coreDumperConfig));

    auto queue = New<TActionQueue>("RPC");
    rpcServer->RegisterService(New<TTestService>(queue->GetInvoker()));
    rpcServer->RegisterService(CreateAdminService(
        queue->GetInvoker(),
        std::move(coreDumper),
        /*authenticator*/ nullptr));
    rpcServer->Start();

    Cin.ReadLine();
}

void RunRpcClient(const TString& address, i32 numIter)
{
    Cout << "Running " << numIter << " iterations of test" << Endl;

    NHPTimer::STime t;
    NHPTimer::GetTime(&t);

    auto client = CreateBusClient(TBusClientConfig::CreateTcp(address));
    auto channel = NRpc::NBus::CreateBusChannel(client);
    TTestProxy proxy(channel);
    proxy.SetDefaultAcknowledgementTimeout(std::nullopt);
    TFuture<TTestProxy::TRspTestCallPtr> result;
    for (i32 i = 0; i < numIter; ++i) {
        auto request = proxy.TestCall();
        request->set_a(i);
        result = request->Invoke();
        if (i % 10000 == 0) {
            Cout << "iteration " << i << Endl;
            auto response = result.Get().ValueOrThrow();
            YT_ASSERT(response->b() == i + 42);
        }
    }
    result.Get();

    NHPTimer::STime tcur = t;
    double seconds = NHPTimer::GetTimePassed(&tcur);

    Cout << "Rps test complete in " << seconds << " seconds. RPS = " << (double) numIter / seconds << Endl;
}

void Usage()
{
    // TODO
}

TSharedRefArray Serialize(const TString& str)
{
    return TSharedRefArray(TSharedRef::FromString(str));
}

TString Deserialize(TSharedRefArray message)
{
    YT_ASSERT(message.Size() == 1);
    const auto& part = message[0];
    return TString(part.Begin(), part.Size());
}

class TBusHandler
    : public IMessageHandler
{
public:
    void HandleMessage(
        TSharedRefArray message,
        IBusPtr replyBus) noexcept override
    {
        TString str = Deserialize(message);
        Cout << "received \"" << str << "\"" << Endl;

        if (!str.empty() && str[str.length() - 1] == '?') {
            auto replyMessage = Serialize(str.substr(0, str.length() - 1) + "!");
            YT_UNUSED_FUTURE(replyBus->Send(replyMessage, NYT::NBus::TSendOptions(EDeliveryTrackingLevel::None)));
        }
    }
};

void OnSent(TString str, const TError& error)
{
    Cout << "\"" << str << "\" was sent with result " << ToString(error) << Endl;
}

void RunBusClient(const TString& address)
{
    IBusClientPtr client = CreateBusClient(TBusClientConfig::CreateTcp(address));
    IBusPtr bus = client->CreateBus(New<TBusHandler>());

    Cout << "Running client bus, type 'done' to terminate" << Endl;

    while (true) {
        TString str;
        Cin >> str;

        if (str == "done")
            break;

        auto message = Serialize(str);
        bus->Send(message, NYT::NBus::TSendOptions(EDeliveryTrackingLevel::Full))
            .Subscribe(BIND(&OnSent, str));
    }
}

void RunBusServer(int port)
{
    auto busServer = CreateBusServer(TBusServerConfig::CreateTcp(port));

    busServer->Start(New<TBusHandler>());

    Cout << "Running server bus listener, press any key to terminate" << Endl;

    TString s;
    Cin >> s;
}

class TRpsSendTestHandler
    : public IMessageHandler
{
public:
    void HandleMessage(
        TSharedRefArray message,
        IBusPtr replyBus) noexcept override
    {
        Y_UNUSED(message);
        Y_UNUSED(replyBus);
    }
};

TSharedRefArray CreateMessage(i32 size)
{
    auto data = TBlob(GetRefCountedTypeCookie<TDefaultBlobTag>(), size, false);
    for (int i = 0; i < size; ++i) {
        data[i] = i & 0xff;
    }
    return TSharedRefArray(TSharedRef::FromBlob(std::move(data)));
}

class TRpsTestTask
    : public IObjectInQueue
{
    i32 MessageSize;
    i32 NumIter;
    IBusPtr Bus;
    TAtomic& NumWorking;
    TSystemEvent& StopEvent;

public:
    TRpsTestTask(i32 messageSize, i32 numIter, IBusPtr bus, TAtomic& numWorking, TSystemEvent& stopEvent)
        : MessageSize(messageSize)
        , NumIter(numIter)
        , Bus(bus)
        , NumWorking(numWorking)
        , StopEvent(stopEvent)
    {}

    void Process(void* param) override
    {
        Y_UNUSED(param);

        TFuture<void> result;
        for (i32 i = 0; i < NumIter; ++i) {
            auto message = CreateMessage(MessageSize);
            result = Bus->Send(message, NYT::NBus::TSendOptions(EDeliveryTrackingLevel::Full));
            if (i % 1000 == 0) {
                Cout << "iteration " << i << Endl;
                result.Get();
            }
        }
        result.Get();
        AtomicDecrement(NumWorking);
        if (NumWorking == 0) {
            StopEvent.Signal();
        }
    }
};



void RunRpsSendTestMultiThreadClient(const TString& address, i32 numIter, i32 messageSize, i32 numThreads)
{
    IBusClientPtr client = CreateBusClient(TBusClientConfig::CreateTcp(address));

    Cout << "Running " << numIter << " iterations of test with size " << messageSize << " in " << numThreads << " threads" << Endl;

    ::TThreadPool taskQueue;
    taskQueue.Start(numThreads);

    std::vector<TRpsTestTask*> tasks;
    std::vector<IBusPtr> buses;
    for (i32 i = 0; i < numThreads; ++i) {
        auto bus = client->CreateBus(New<TRpsSendTestHandler>());
        buses.push_back(bus);
    }

    NHPTimer::STime t;
    NHPTimer::GetTime(&t);

    TAtomic numWorking = numThreads;
    TSystemEvent stopEvent;

    for (i32 i = 0; i < numThreads; ++i) {
        tasks.push_back(new TRpsTestTask(messageSize, numIter, buses[i], numWorking, stopEvent));
        YT_VERIFY(taskQueue.Add(tasks.back()));
    }

    stopEvent.Wait();

    NHPTimer::STime tcur = t;
    double seconds = NHPTimer::GetTimePassed(&tcur);

    Cout << "Rps test complete in " << seconds << " seconds. RPS = " << (double) numThreads * numIter / seconds << Endl;
}


void RunRpsSendTestServer(int port)
{
    auto busServer = CreateBusServer(TBusServerConfig::CreateTcp(port));
    busServer->Start(New<TRpsSendTestHandler>());

    Cout << "Running rps test listener, press any key to terminate" << Endl;

    char ch;
    Cin.ReadChar(ch);
}


class TRpsReplyClient
    : public IMessageHandler
{
public:
    using TPtr = TIntrusivePtr<TRpsReplyClient>;

    TRpsReplyClient(int numIterations)
        : NumIterations(numIterations)
    { }

    void HandleMessage(
        TSharedRefArray message,
        IBusPtr replyBus) noexcept override
    {
        Y_UNUSED(message);
        Y_UNUSED(replyBus);
        if (NumIterationsLeft == NumIterations) {
            Cout << "Test started" << Endl;
            NHPTimer::GetTime(&Timer);
        }
        --NumIterationsLeft;
        if (NumIterationsLeft % 1000 == 0) {
            Cout << NumIterationsLeft << " iterations left" << Endl;
        }

        if (NumIterationsLeft == 0) {
            double seconds = NHPTimer::GetTimePassed(&Timer);
            Cout << "Reply test complete in " << seconds << " seconds. RPS = " << (double) NumIterations / seconds << Endl;
        }
    }

    void SetIterations(int numIterations)
    {
        NumIterationsLeft = NumIterations = numIterations;
    }

private:
    i32 NumIterations;
    i32 NumIterationsLeft;
    NHPTimer::STime Timer;

};

class TRpsReplyServer
    : public IMessageHandler
{
public:
    void HandleMessage(
        TSharedRefArray message,
        IBusPtr replyBus) noexcept override
    {
        TString str = Deserialize(message);
        Cout << "received " << str << " => Test started " << Endl;

        for (i32 i = 0; i < NumIterations; ++i) {
            if (i % 1000 == 0) {
                Cout << "Iteration " << i << Endl;
            }
            auto replyMessage = CreateMessage(MessageSize);
            YT_UNUSED_FUTURE(replyBus->Send(replyMessage, NYT::NBus::TSendOptions(EDeliveryTrackingLevel::None)));
        }

        Cout << "Everything was sent" << Endl;
    }

    void SetIterations(i32 numIterations)
    {
        NumIterations = numIterations;
    }

    void SetMessageSize(i32 messageSize)
    {
        MessageSize = messageSize;
    }

private:
    i32 NumIterations;
    i32 MessageSize;
};

void RunRpsReplyTestClient(const TString& address, i32 numIter)
{
    IBusClientPtr client = CreateBusClient(TBusClientConfig::CreateTcp(address));

    TRpsReplyClient::TPtr handler = New<TRpsReplyClient>(numIter);
    IBusPtr bus = client->CreateBus(handler);

    Cout << "Running " << numIter << " replies" << Endl;
    TString str = "Go!";
    auto message = Serialize(str);
    bus->Send(message, NYT::NBus::TSendOptions(EDeliveryTrackingLevel::Full))
        .Subscribe(BIND(&OnSent, str));

    char ch;
    Cin.ReadChar(ch);
}

void RunRpsReplyTestServer(int port, i32 numIter, i32 messageSize)
{
    auto handler = New<TRpsReplyServer>();
    handler->SetIterations(numIter);
    handler->SetMessageSize(messageSize);

    auto busServer = CreateBusServer(TBusServerConfig::CreateTcp(port));
    busServer->Start(handler);

    Cout << "Running " << numIter << " replies with size " << messageSize << Endl;

    char ch;
    Cin.ReadChar(ch);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

int main(int argc, const char *argv[])
{
    try {
        using namespace NYT;
        using namespace NLastGetopt;
        TOpts opts;

        opts.AddHelpOption();

        TString mode;
        opts.AddLongOption("mode", "")
            .RequiredArgument("MODE")
            .StoreResult(&mode);

        int port = 8888;
        opts.AddLongOption("port", "")
            .RequiredArgument("PORT")
            .StoreResult(&port);

        i32 numIter = 1000;
        opts.AddLongOption("iters", "")
            .RequiredArgument("NUM")
            .StoreResult(&numIter);

        i32 messageSize = 100;
        opts.AddLongOption("netsize", "")
            .RequiredArgument("SIZE")
            .StoreResult(&messageSize);

        TString address = "localhost:8888";
        opts.AddLongOption("address", "")
            .RequiredArgument("ADDRESS")
            .StoreResult(&address);

        i32 numThreads = 1;
        opts.AddLongOption("threads", "")
            .RequiredArgument("NUM")
            .StoreResult(&numThreads);

        TString configFileName;
        opts.AddLongOption("config", "configuration file")
            .RequiredArgument("FILE")
            .StoreResult(&configFileName);

        TOptsParseResult results(&opts, argc, argv);


        if (!configFileName.empty()) {
            auto config = NLogging::TLogManagerConfig::CreateFromFile(configFileName, "/logging");
            NYT::NLogging::TLogManager::Get()->Configure(config);
        }

        if (mode == "bus-client") {
            NYT::RunBusClient(address);
        } else if (mode == "bus-server") {
            NYT::RunBusServer(port);
        } else if (mode == "rps-send-mt") {
            NYT::RunRpsSendTestMultiThreadClient(address, numIter, messageSize, numThreads);
        } else if (mode == "rps-send-server") {
            NYT::RunRpsSendTestServer(port);
        } else if (mode == "rps-reply-client") {
            NYT::RunRpsReplyTestClient(address, numIter);
        } else if (mode == "rps-reply-server") {
            NYT::RunRpsReplyTestServer(port, numIter, messageSize);
        } else if (mode == "rpc-server") {
            NYT::RunRpcServer(port);
        } else if (mode == "rpc-client") {
            NYT::RunRpcClient(address, numIter);
        } else {
            throw std::runtime_error("Unknown mode");
        }
    }
    catch (std::exception& e) {
        Cerr << "ERROR: " << e.what() << Endl;
    }

    // Wait a bit to flush all pending finalizations and get them properly logged
    // (debugging purposes only!)
    Sleep(TDuration::MilliSeconds(1000));
}

