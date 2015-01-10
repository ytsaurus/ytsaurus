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
    DEFINE_RPC_PROXY_METHOD(NMyRpc, SlowCall);
    DEFINE_RPC_PROXY_METHOD(NMyRpc, SlowCanceledCall);
    DEFINE_RPC_PROXY_METHOD(NMyRpc, NoReply);

    DEFINE_ONE_WAY_RPC_PROXY_METHOD(NMyRpc, OneWay);
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

IChannelPtr CreateChannel(const Stroka& address = "localhost:2000")
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
        RegisterMethod(RPC_SERVICE_METHOD_DESC(SlowCall)
            .SetCancelable(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(SlowCanceledCall)
            .SetCancelable(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(NoReply));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(OneWay)
            .SetOneWay(true));
        // Note: NotRegisteredCall and NotRegistredOneWay are not registered
    }

    DECLARE_RPC_SERVICE_METHOD(NMyRpc, SomeCall)
    {
        context->SetRequestInfo();
        int a = request->a();
        response->set_b(a + 100);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NMyRpc, ModifyAttachments)
    {
        for (const auto& attachment : request->Attachments()) {
            auto data = TBlob(TDefaultBlobTag());
            data.Append(attachment);
            data.Append("_", 1);
            response->Attachments().push_back(TSharedRef::FromBlob(std::move(data)));
        }
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NMyRpc, DoNothing)
    {
        context->SetRequestInfo();
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NMyRpc, CustomMessageError)
    {
        context->SetRequestInfo();
        context->Reply(TError(NYT::EErrorCode(42), "Some Error"));
    }

    DECLARE_RPC_SERVICE_METHOD(NMyRpc, SlowCall)
    {
        context->SetRequestInfo();
        Sleep(TDuration::Seconds(1.0));
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NMyRpc, SlowCanceledCall)
    {
        try {
            context->SetRequestInfo();
            WaitFor(MakeDelayed(TDuration::Seconds(2)));
            context->Reply();
        } catch (const TFiberCanceledException&) {
            SlowCallCanceled_ = true;
            throw;
        }
    }

    DECLARE_RPC_SERVICE_METHOD(NMyRpc, NoReply)
    { }

    DECLARE_ONE_WAY_RPC_SERVICE_METHOD(NMyRpc, OneWay)
    {
        context->SetRequestInfo();
        OneWayCalled_.Set();
    }


    TFuture<void> GetOneWayCalled() const
    {
        return OneWayCalled_;
    }

    bool GetSlowCallCanceled() const
    {
        return SlowCallCanceled_;
    }

private:
    TPromise<void> OneWayCalled_ = NewPromise<void>();
    bool SlowCallCanceled_ = false;

};

////////////////////////////////////////////////////////////////////////////////

class TRpcTest
    : public ::testing::Test
{
public:
    virtual void SetUp()
    {
        auto busConfig = New<TTcpBusServerConfig>(2000);
        auto busServer = CreateTcpBusServer(busConfig);

        Server_ = CreateBusServer(busServer);

        Queue_ = New<TActionQueue>();

        Service_ = New<TMyService>(Queue_->GetInvoker());
        Server_->RegisterService(Service_);
        Server_->Start();
    }

    virtual void TearDown()
    {
        Server_->Stop();
        Server_.Reset();
    }

protected:
    TActionQueuePtr Queue_;
    TIntrusivePtr<TMyService> Service_;
    IServerPtr Server_;

};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TRpcTest, Send)
{
    TMyProxy proxy(CreateChannel());
    auto req = proxy.SomeCall();
    req->set_a(42);
    auto rspOrError = req->Invoke().Get();
    EXPECT_TRUE(rspOrError.IsOK());
    const auto& rsp = rspOrError.Value();
    EXPECT_EQ(142, rsp->b());
}

TEST_F(TRpcTest, ManyAsyncRequests)
{
    const int RequestCount = 1000;

    auto collector = New<TParallelCollector<void>>();

    TMyProxy proxy(CreateChannel());

    for (int i = 0; i < RequestCount; ++i) {
        auto request = proxy.SomeCall();
        request->set_a(i);
        auto result = request->Invoke().Apply(BIND([=] (TMyProxy::TRspSomeCallPtr rsp) {
            EXPECT_EQ(i + 100, rsp->b());
        }));
        collector->Collect(result);
    }

    EXPECT_TRUE(collector->Complete().Get().IsOK());
}

TEST_F(TRpcTest, Attachments)
{
    TMyProxy proxy(CreateChannel());
    auto req = proxy.ModifyAttachments();

    req->Attachments().push_back(SharedRefFromString("Hello"));
    req->Attachments().push_back(SharedRefFromString("from"));
    req->Attachments().push_back(SharedRefFromString("TMyProxy"));

    auto rspOrError = req->Invoke().Get();
    EXPECT_TRUE(rspOrError.IsOK());
    const auto& rsp = rspOrError.Value();

    const auto& attachments = rsp->Attachments();
    EXPECT_EQ(3, attachments.size());
    EXPECT_EQ("Hello_",     StringFromSharedRef(attachments[0]));
    EXPECT_EQ("from_",      StringFromSharedRef(attachments[1]));
    EXPECT_EQ("TMyProxy_",  StringFromSharedRef(attachments[2]));
}

// Now test different types of errors
TEST_F(TRpcTest, OK)
{
    TMyProxy proxy(CreateChannel());
    auto req = proxy.DoNothing();
    auto rspOrError = req->Invoke().Get();
    EXPECT_TRUE(rspOrError.IsOK());
}

TEST_F(TRpcTest, NoAck)
{
    TMyProxy proxy(CreateChannel());
    auto req = proxy.DoNothing()->SetRequestAck(false);
    auto rspOrError = req->Invoke().Get();
    EXPECT_TRUE(rspOrError.IsOK());
}

TEST_F(TRpcTest, TransportError)
{
    TMyProxy proxy(CreateChannel("localhost:9999"));
    auto req = proxy.DoNothing();
    auto rspOrError = req->Invoke().Get();
    EXPECT_EQ(NRpc::EErrorCode::TransportError, rspOrError.GetCode());
}

TEST_F(TRpcTest, NoService)
{
    TNonExistingServiceProxy proxy(CreateChannel());
    auto req = proxy.DoNothing();
    auto rspOrError = req->Invoke().Get();
    EXPECT_EQ(NRpc::EErrorCode::NoSuchService, rspOrError.GetCode());
}

TEST_F(TRpcTest, NoMethod)
{
    TMyProxy proxy(CreateChannel());
    auto req = proxy.NotRegistered();
    auto rspOrError = req->Invoke().Get();
    EXPECT_EQ(NRpc::EErrorCode::NoSuchMethod, rspOrError.GetCode());
}

TEST_F(TRpcTest, ClientTimeout)
{
    TMyProxy proxy(CreateChannel());
    proxy.SetDefaultTimeout(TDuration::Seconds(0.5));
    auto req = proxy.SlowCall();
    auto rspOrError = req->Invoke().Get();
    EXPECT_EQ(NYT::EErrorCode::Timeout, rspOrError.GetCode());
}

TEST_F(TRpcTest, ServerTimeout)
{
    TMyProxy proxy(CreateChannel());
    proxy.SetDefaultTimeout(TDuration::Seconds(0.5));
    auto req = proxy.SlowCanceledCall();
    auto rspOrError = req->Invoke().Get();
    EXPECT_EQ(NYT::EErrorCode::Timeout, rspOrError.GetCode());
    Sleep(TDuration::Seconds(1));
    EXPECT_TRUE(Service_->GetSlowCallCanceled());
}

TEST_F(TRpcTest, ClientCancel)
{
    TMyProxy proxy(CreateChannel());
    auto req = proxy.SlowCanceledCall();
    auto asyncRspOrError = req->Invoke();
    Sleep(TDuration::Seconds(0.5));
    EXPECT_FALSE(asyncRspOrError.IsSet());
    asyncRspOrError.Cancel();
    EXPECT_TRUE(asyncRspOrError.IsSet());
    auto rspOrError = asyncRspOrError.Get();
    EXPECT_EQ(NYT::EErrorCode::Canceled, rspOrError.GetCode());
    Sleep(TDuration::Seconds(1));
    EXPECT_TRUE(Service_->GetSlowCallCanceled());
}

TEST_F(TRpcTest, SlowCall)
{
    TMyProxy proxy(CreateChannel());
    proxy.SetDefaultTimeout(TDuration::Seconds(2.0));
    auto req = proxy.SlowCall();
    auto rspOrError = req->Invoke().Get();
    EXPECT_TRUE(rspOrError.IsOK());
}

TEST_F(TRpcTest, NoReply)
{
    TMyProxy proxy(CreateChannel());

    auto req = proxy.NoReply();
    auto rspOrError = req->Invoke().Get();
    EXPECT_EQ(NYT::EErrorCode::Canceled, rspOrError.GetCode());
}

TEST_F(TRpcTest, CustomErrorMessage)
{
    TMyProxy proxy(CreateChannel());
    auto req = proxy.CustomMessageError();
    auto rspOrError = req->Invoke().Get();
    EXPECT_EQ(NYT::EErrorCode(42), rspOrError.GetCode());
    EXPECT_EQ("Some Error", rspOrError.GetMessage());
}

TEST_F(TRpcTest, OneWayOK)
{
    TMyProxy proxy(CreateChannel());
    auto req = proxy.OneWay();
    auto rspOrError = req->Invoke().Get();
    EXPECT_TRUE(rspOrError.IsOK());
    Service_->GetOneWayCalled().Get();
}

TEST_F(TRpcTest, OneWayTransportError)
{
    TMyProxy proxy(CreateChannel("localhost:9999"));
    auto req = proxy.OneWay();
    auto rspOrError = req->Invoke().Get();
    EXPECT_FALSE(rspOrError.IsOK());
}

TEST_F(TRpcTest, OneWayNoService)
{
    TNonExistingServiceProxy proxy(CreateChannel());
    auto req = proxy.OneWay();
    auto rspOrError = req->Invoke().Get();
    // In this case we receive OK instead of NoSuchService
    EXPECT_TRUE(rspOrError.IsOK());
}

TEST_F(TRpcTest, OneWayNoMethod)
{
    TMyProxy proxy(CreateChannel());
    auto req = proxy.NotRegistredOneWay();
    auto rspOrError = req->Invoke().Get();
    // In this case we receive OK instead of NoSuchMethod
    EXPECT_TRUE(rspOrError.IsOK());
}

TEST_F(TRpcTest, ConnectionLost)
{
    TMyProxy proxy(CreateChannel());

    auto req = proxy.SlowCanceledCall();
    auto asyncRspOrError = req->Invoke();

    Sleep(TDuration::Seconds(0.5));

    EXPECT_FALSE(asyncRspOrError.IsSet());
    Server_->Stop();

    Sleep(TDuration::Seconds(0.5));

    EXPECT_TRUE(asyncRspOrError.IsSet());
    auto rspOrError = asyncRspOrError.Get();
    EXPECT_FALSE(rspOrError.IsOK());
    EXPECT_EQ(NRpc::EErrorCode::TransportError, rspOrError.GetCode());
    EXPECT_TRUE(Service_->GetSlowCallCanceled());
}

TEST_F(TRpcTest, ProtocolVersionMismatch)
{
    TMyProxy proxy(CreateChannel(), 1);
    auto req = proxy.SomeCall();
    req->set_a(42);
    auto rspOrError = req->Invoke().Get();
    EXPECT_FALSE(rspOrError.IsOK());
    EXPECT_EQ(NRpc::EErrorCode::ProtocolError, rspOrError.GetCode());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NRpc
} // namespace NYT
