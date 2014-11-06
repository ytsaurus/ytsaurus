#include "stdafx.h"
#include "framework.h"

#include <yt/unittests/rpc_ut.pb.h>

#include <core/misc/error.h>

#include <core/concurrency/action_queue.h>
#include <core/concurrency/parallel_collector.h>

#include <core/bus/bus.h>
#include <core/bus/config.h>
#include <core/bus/server.h>
#include <core/bus/tcp_client.h>
#include <core/bus/tcp_server.h>

#include <core/rpc/client.h>
#include <core/rpc/server.h>
#include <core/rpc/bus_server.h>
#include <core/rpc/service_detail.h>
#include <core/rpc/bus_channel.h>

namespace NYT {
namespace NRpc {
namespace {

using namespace NBus;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TMyProxy
    : public TProxyBase
{
public:
    static const Stroka GetServiceName()
    {
        return "MyService";
    }

    explicit TMyProxy(IChannelPtr channel, int protocolVersion = DefaultProtocolVersion)
        : TProxyBase(channel, GetServiceName(), protocolVersion)
    { }

    DEFINE_RPC_PROXY_METHOD(NMyRpc, SomeCall);
    DEFINE_RPC_PROXY_METHOD(NMyRpc, ModifyAttachments);
    DEFINE_RPC_PROXY_METHOD(NMyRpc, DoNothing);
    DEFINE_RPC_PROXY_METHOD(NMyRpc, CustomMessageError);
    DEFINE_RPC_PROXY_METHOD(NMyRpc, NotRegistered);
    DEFINE_RPC_PROXY_METHOD(NMyRpc, LongReply);
    DEFINE_RPC_PROXY_METHOD(NMyRpc, NoReply);

    DEFINE_ONE_WAY_RPC_PROXY_METHOD(NMyRpc, OneWay);
    DEFINE_ONE_WAY_RPC_PROXY_METHOD(NMyRpc, CheckAll);
    DEFINE_ONE_WAY_RPC_PROXY_METHOD(NMyRpc, NotRegistredOneWay);

};

////////////////////////////////////////////////////////////////////////////////

class TNonExistingServiceProxy
    : public TProxyBase
{
public:
    explicit TNonExistingServiceProxy(IChannelPtr channel)
        : TProxyBase(channel, "NonExistingService")
    { }

    DEFINE_RPC_PROXY_METHOD(NMyRpc, DoNothing);
    DEFINE_ONE_WAY_RPC_PROXY_METHOD(NMyRpc, OneWay);
};

////////////////////////////////////////////////////////////////////////////////

Stroka StringFromSharedRef(const TSharedRef& sharedRef)
{
    return Stroka(sharedRef.Begin(), sharedRef.Begin() + sharedRef.Size());
}

TSharedRef SharedRefFromString(const Stroka& s)
{
    return TSharedRef::FromString(s);
}

IChannelPtr CreateChannel(const Stroka& address)
{
    auto client = CreateTcpBusClient(New<TTcpBusClientConfig>(address));
    return CreateBusChannel(client);
}

////////////////////////////////////////////////////////////////////////////////

class TMyService
    : public TServiceBase
{
public:
    explicit TMyService(IInvokerPtr invoker)
        : TServiceBase(
            invoker,
            TMyProxy::GetServiceName(),
            NLog::TLogger("Main"))
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(SomeCall));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ModifyAttachments));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(DoNothing));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(CustomMessageError));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(LongReply));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(NoReply));

        RegisterMethod(RPC_SERVICE_METHOD_DESC(OneWay)
            .SetOneWay(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(CheckAll)
            .SetOneWay(true));

        // Note: NotRegisteredCall and NotRegistredOneWay are not registered
    }

    DECLARE_RPC_SERVICE_METHOD(NMyRpc, SomeCall);
    DECLARE_RPC_SERVICE_METHOD(NMyRpc, ModifyAttachments);
    DECLARE_RPC_SERVICE_METHOD(NMyRpc, DoNothing);
    DECLARE_RPC_SERVICE_METHOD(NMyRpc, CustomMessageError);
    DECLARE_RPC_SERVICE_METHOD(NMyRpc, LongReply);
    DECLARE_RPC_SERVICE_METHOD(NMyRpc, NoReply);

    DECLARE_ONE_WAY_RPC_SERVICE_METHOD(NMyRpc, OneWay);
    DECLARE_ONE_WAY_RPC_SERVICE_METHOD(NMyRpc, CheckAll);

    TFuture<void> GetOneWayCalled()
    {
        return OneWayCalled_;
    }

private:
    TPromise<void> OneWayCalled_ = NewPromise<void>();

};

DEFINE_RPC_SERVICE_METHOD(TMyService, SomeCall)
{
    int a = request->a();
    response->set_b(a + 100);
    context->Reply();
}

DEFINE_RPC_SERVICE_METHOD(TMyService, DoNothing)
{
    context->Reply();
}

DEFINE_RPC_SERVICE_METHOD(TMyService, LongReply)
{
    Sleep(TDuration::Seconds(1.0));
    context->Reply();
}

DEFINE_RPC_SERVICE_METHOD(TMyService, NoReply)
{ }

DEFINE_RPC_SERVICE_METHOD(TMyService, CustomMessageError)
{
    context->Reply(TError(42, "Some Error"));
}

DEFINE_ONE_WAY_RPC_SERVICE_METHOD(TMyService, OneWay)
{
    OneWayCalled_.Set();
}

////////////////////////////////////////////////////////////////////////////////

class TRpcTest
    : public ::testing::Test
{
public:
    virtual void SetUp()
    {
        auto busConfig = New<TTcpBusServerConfig>(2000);
        auto busServer = CreateTcpBusServer(busConfig);

        RpcServer = CreateBusServer(busServer);

        Queue = New<TActionQueue>();

        Service = New<TMyService>(Queue->GetInvoker());
        RpcServer->RegisterService(Service);
        RpcServer->Start();
    }

    virtual void TearDown()
    {
        RpcServer->Stop();
        RpcServer.Reset();
    }

protected:
    TActionQueuePtr Queue;
    TIntrusivePtr<TMyService> Service;
    IServerPtr RpcServer;

};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TRpcTest, Send)
{
    TMyProxy proxy(CreateChannel("localhost:2000"));
    auto request = proxy.SomeCall();
    request->set_a(42);
    auto response = request->Invoke().Get();

    EXPECT_TRUE(response->IsOK());
    EXPECT_EQ(142, response->b());
}

TEST_F(TRpcTest, ManyAsyncRequests)
{
    const int RequestCount = 1000;

    auto collector = New<TParallelCollector<void>>();

    TMyProxy proxy(CreateChannel("localhost:2000"));

    for (int i = 0; i < RequestCount; ++i) {
        auto request = proxy.SomeCall();
        request->set_a(i);
        auto result = request->Invoke().Apply(BIND([=] (TMyProxy::TRspSomeCallPtr rsp) -> TError {
            EXPECT_TRUE(rsp->IsOK());
            EXPECT_EQ(i + 100, rsp->b());
            return TError();
        }));
        collector->Collect(result);
    }

    collector->Complete().Get();
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_RPC_SERVICE_METHOD(TMyService, ModifyAttachments)
{
    for (const auto& attachment : request->Attachments()) {
        auto data = TBlob(TDefaultBlobTag());
        data.Append(attachment);
        data.Append("_", 1);
        response->Attachments().push_back(TSharedRef::FromBlob(std::move(data)));
    }
    context->Reply();
}

TEST_F(TRpcTest, Attachments)
{
    TMyProxy proxy(CreateChannel("localhost:2000"));
    auto request = proxy.ModifyAttachments();

    request->Attachments().push_back(SharedRefFromString("Hello"));
    request->Attachments().push_back(SharedRefFromString("from"));
    request->Attachments().push_back(SharedRefFromString("TMyProxy"));

    auto response = request->Invoke().Get();

    const auto& attachments = response->Attachments();
    EXPECT_EQ(3, attachments.size());
    EXPECT_EQ("Hello_",     StringFromSharedRef(attachments[0]));
    EXPECT_EQ("from_",      StringFromSharedRef(attachments[1]));
    EXPECT_EQ("TMyProxy_",  StringFromSharedRef(attachments[2]));
}

////////////////////////////////////////////////////////////////////////////////

// Now test different types of errors
TEST_F(TRpcTest, OK)
{
    TMyProxy proxy(CreateChannel("localhost:2000"));
    auto request = proxy.DoNothing();
    auto response = request->Invoke().Get();

    EXPECT_TRUE(response->GetError().IsOK());
}

TEST_F(TRpcTest, TransportError)
{
    TMyProxy proxy(CreateChannel("localhost:9999"));
    auto request = proxy.DoNothing();
    auto response = request->Invoke().Get();

    EXPECT_EQ(NRpc::EErrorCode::TransportError, response->GetError().GetCode());
}

TEST_F(TRpcTest, NoService)
{
    TNonExistingServiceProxy proxy(CreateChannel("localhost:2000"));
    auto request = proxy.DoNothing();
    auto response = request->Invoke().Get();

    EXPECT_EQ(NRpc::EErrorCode::NoSuchService, response->GetError().GetCode());
}

TEST_F(TRpcTest, NoMethod)
{
    TMyProxy proxy(CreateChannel("localhost:2000"));
    auto request = proxy.NotRegistered();
    auto response = request->Invoke().Get();

    EXPECT_EQ(NRpc::EErrorCode::NoSuchMethod, response->GetError().GetCode());
}

TEST_F(TRpcTest, Timeout)
{
    TMyProxy proxy(CreateChannel("localhost:2000"));
    proxy.SetDefaultTimeout(TDuration::Seconds(0.5));

    auto request = proxy.LongReply();
    auto response = request->Invoke().Get();

    EXPECT_EQ(NRpc::EErrorCode::Timeout, response->GetError().GetCode());
}

TEST_F(TRpcTest, NoReply)
{
    TMyProxy proxy(CreateChannel("localhost:2000"));

    auto request = proxy.NoReply();
    auto response = request->Invoke().Get();

    EXPECT_EQ(NRpc::EErrorCode::Unavailable, response->GetError().GetCode());
}

TEST_F(TRpcTest, CustomErrorMessage)
{
    TMyProxy proxy(CreateChannel("localhost:2000"));
    auto request = proxy.CustomMessageError();
    auto response = request->Invoke().Get();

    EXPECT_EQ(42, response->GetError().GetCode());
    EXPECT_EQ("Some Error", response->GetError().GetMessage());
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_ONE_WAY_RPC_SERVICE_METHOD(TMyService, CheckAll)
{
    EXPECT_EQ(12345, request->value());
    EXPECT_EQ(true, request->ok());
    EXPECT_EQ(Stroka("hello, TMyService"), request->message());

    const auto& attachments = request->Attachments();
    EXPECT_EQ(3, attachments.size());
    EXPECT_EQ("Attachments", StringFromSharedRef(attachments[0]));
    EXPECT_EQ("are", StringFromSharedRef(attachments[1]));
    EXPECT_EQ("ok", StringFromSharedRef(attachments[2]));
}

TEST_F(TRpcTest, OneWayOK)
{
    TMyProxy proxy(CreateChannel("localhost:2000"));
    auto request = proxy.OneWay();
    auto response = request->Invoke().Get();

    EXPECT_TRUE(response->IsOK());

    Service->GetOneWayCalled().Get();
}

TEST_F(TRpcTest, OneWayTransportError)
{
    TMyProxy proxy(CreateChannel("localhost:9999"));
    auto request = proxy.OneWay();
    auto response = request->Invoke().Get();

    EXPECT_FALSE(response->IsOK());
}

TEST_F(TRpcTest, OneWayNoService)
{
    TNonExistingServiceProxy proxy(CreateChannel("localhost:2000"));
    auto request = proxy.OneWay();
    auto response = request->Invoke().Get();

    // In this case we receive OK instead of NoSuchService
    EXPECT_TRUE(response->IsOK());
}

TEST_F(TRpcTest, OneWayNoMethod)
{
    TMyProxy proxy(CreateChannel("localhost:2000"));
    auto request = proxy.NotRegistredOneWay();
    auto response = request->Invoke().Get();

    // In this case we receive OK instead of NoSuchMethod
    EXPECT_TRUE(response->IsOK());
}

TEST_F(TRpcTest, LostConnection)
{
    TMyProxy proxy(CreateChannel("localhost:2000"));
    proxy.SetDefaultTimeout(TDuration::Seconds(10));

    auto request = proxy.LongReply();
    auto future = request->Invoke();

    Sleep(TDuration::Seconds(0.2));

    EXPECT_FALSE(future.IsSet());
    RpcServer->Stop();

    Sleep(TDuration::Seconds(0.2));

    // check that lost of connection is detected fast
    EXPECT_TRUE(future.IsSet());
    auto response = future.Get();
    EXPECT_FALSE(response->IsOK());
    EXPECT_EQ(NRpc::EErrorCode::TransportError, response->GetError().GetCode());
}

TEST_F(TRpcTest, ProtocolVersionMismatch)
{
    TMyProxy proxy(CreateChannel("localhost:2000"), 1);
    auto request = proxy.SomeCall();
    request->set_a(42);
    auto response = request->Invoke().Get();
    EXPECT_FALSE(response->IsOK());
    EXPECT_EQ(NRpc::EErrorCode::ProtocolError, response->GetError().GetCode());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NRpc
} // namespace NYT
