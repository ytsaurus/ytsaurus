#include "framework.h"

#include <yt/core/bus/bus.h>
#include <yt/core/bus/config.h>
#include <yt/core/bus/server.h>
#include <yt/core/bus/tcp_client.h>
#include <yt/core/bus/tcp_server.h>

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/delayed_executor.h>

#include <yt/core/misc/error.h>

#include <yt/core/rpc/bus_channel.h>
#include <yt/core/rpc/bus_server.h>
#include <yt/core/rpc/client.h>
#include <yt/core/rpc/server.h>
#include <yt/core/rpc/service_detail.h>

#include <yt/core/unittests/rpc_ut.pb.h>

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
    DEFINE_RPC_PROXY(TMyProxy, RPC_PROXY_DESC(MyService)
        .SetProtocolVersion(1));

    DEFINE_RPC_PROXY_METHOD(NMyRpc, SomeCall);
    DEFINE_RPC_PROXY_METHOD(NMyRpc, RegularAttachments);
    DEFINE_RPC_PROXY_METHOD(NMyRpc, NullAndEmptyAttachments);
    DEFINE_RPC_PROXY_METHOD(NMyRpc, DoNothing);
    DEFINE_RPC_PROXY_METHOD(NMyRpc, CustomMessageError);
    DEFINE_RPC_PROXY_METHOD(NMyRpc, NotRegistered);
    DEFINE_RPC_PROXY_METHOD(NMyRpc, SlowCall);
    DEFINE_RPC_PROXY_METHOD(NMyRpc, SlowCanceledCall);
    DEFINE_RPC_PROXY_METHOD(NMyRpc, NoReply);

    DEFINE_ONE_WAY_RPC_PROXY_METHOD(NMyRpc, OneWay);
    DEFINE_ONE_WAY_RPC_PROXY_METHOD(NMyRpc, NotRegistredOneWay);
};

class TNonExistingServiceProxy
    : public TProxyBase
{
public:
    DEFINE_RPC_PROXY(TNonExistingServiceProxy, RPC_PROXY_DESC(NonExistingService));

    DEFINE_RPC_PROXY_METHOD(NMyRpc, DoNothing);
    DEFINE_ONE_WAY_RPC_PROXY_METHOD(NMyRpc, OneWay);
};

class TMyIncorrectProtocolVersionProxy
    : public TProxyBase
{
public:
    DEFINE_RPC_PROXY(TMyIncorrectProtocolVersionProxy, RPC_PROXY_DESC(MyService)
        .SetProtocolVersion(2));

    DEFINE_RPC_PROXY_METHOD(NMyRpc, SomeCall);
};

////////////////////////////////////////////////////////////////////////////////

TString StringFromSharedRef(const TSharedRef& sharedRef)
{
    return TString(sharedRef.Begin(), sharedRef.Begin() + sharedRef.Size());
}

TSharedRef SharedRefFromString(const TString& s)
{
    return TSharedRef::FromString(s);
}

////////////////////////////////////////////////////////////////////////////////

class TMyService
    : public TServiceBase
{
public:
    explicit TMyService(IInvokerPtr invoker)
        : TServiceBase(
            invoker,
            TMyProxy::GetDescriptor(),
            NLogging::TLogger("Main"))
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(SomeCall));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(RegularAttachments));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(NullAndEmptyAttachments));
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

    DECLARE_RPC_SERVICE_METHOD(NMyRpc, RegularAttachments)
    {
        for (const auto& attachment : request->Attachments()) {
            auto data = TBlob(TDefaultBlobTag());
            data.Append(attachment);
            data.Append("_", 1);
            response->Attachments().push_back(TSharedRef::FromBlob(std::move(data)));
        }
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NMyRpc, NullAndEmptyAttachments)
    {
        const auto& attachments = request->Attachments();
        EXPECT_EQ(2, attachments.size());
        EXPECT_FALSE(attachments[0]);
        EXPECT_TRUE(attachments[1]);
        EXPECT_TRUE(attachments[1].Empty());
        response->Attachments() = attachments;
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
            WaitFor(TDelayedExecutor::MakeDelayed(TDuration::Seconds(2)));
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

class TRpcTestBase
    : public ::testing::Test
{
public:
    virtual void SetUp()
    {
        auto busServer = CreateServer();

        Server_ = CreateBusServer(busServer);

        Queue_ = New<TActionQueue>();

        Service_ = New<TMyService>(Queue_->GetInvoker());
        Server_->RegisterService(Service_);
        Server_->Start();
    }

    virtual void TearDown()
    {
        Server_->Stop().Get();
        Server_.Reset();
    }

protected:
    virtual IBusServerPtr CreateServer() = 0;

    TActionQueuePtr Queue_;
    TIntrusivePtr<TMyService> Service_;
    IServerPtr Server_;

};

////////////////////////////////////////////////////////////////////////////////

class TRpcTest
    : public TRpcTestBase
{
public:
    virtual IBusServerPtr CreateServer()
    {
        auto busConfig = TTcpBusServerConfig::CreateTcp(2000);
        return CreateTcpBusServer(busConfig);
    }

    IChannelPtr CreateChannel(const TString& address = "localhost:2000")
    {
        auto client = CreateTcpBusClient(TTcpBusClientConfig::CreateTcp(address));
        return CreateBusChannel(client);
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TRpcTest, Send)
{
    TMyProxy proxy(CreateChannel());
    auto req = proxy.SomeCall();
    req->set_a(42);
    auto rspOrError = req->Invoke().Get();
    EXPECT_TRUE(rspOrError.IsOK()) << ToString(rspOrError);
    const auto& rsp = rspOrError.Value();
    EXPECT_EQ(142, rsp->b());
}

TEST_F(TRpcTest, ManyAsyncRequests)
{
    const int RequestCount = 1000;

    std::vector<TFuture<void>> asyncResults;

    TMyProxy proxy(CreateChannel());

    for (int i = 0; i < RequestCount; ++i) {
        auto request = proxy.SomeCall();
        request->set_a(i);
        auto asyncResult = request->Invoke().Apply(BIND([=] (TMyProxy::TRspSomeCallPtr rsp) {
            EXPECT_EQ(i + 100, rsp->b());
        }));
        asyncResults.push_back(asyncResult);
    }

    EXPECT_TRUE(Combine(asyncResults).Get().IsOK());
}

TEST_F(TRpcTest, RegularAttachments)
{
    TMyProxy proxy(CreateChannel());
    auto req = proxy.RegularAttachments();

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

TEST_F(TRpcTest, NullAndEmptyAttachments)
{
    TMyProxy proxy(CreateChannel());
    auto req = proxy.NullAndEmptyAttachments();

    req->Attachments().push_back(TSharedRef());
    req->Attachments().push_back(EmptySharedRef);

    auto rspOrError = req->Invoke().Get();
    EXPECT_TRUE(rspOrError.IsOK());
    auto rsp = rspOrError.Value();

    const auto& attachments = rsp->Attachments();
    EXPECT_EQ(2, attachments.size());
    EXPECT_FALSE(attachments[0]);
    EXPECT_TRUE(attachments[1]);
    EXPECT_TRUE(attachments[1].Empty());
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
    auto req = proxy.DoNothing();
    req->SetRequestAck(false);
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
    Sleep(TDuration::Seconds(0.1));
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
    EXPECT_EQ(NRpc::EErrorCode::Unavailable, rspOrError.GetCode());
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
    EXPECT_EQ(NYT::NRpc::EErrorCode::TransportError, rspOrError.GetCode());
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
    Server_->Stop(false);

    Sleep(TDuration::Seconds(0.5));

    EXPECT_TRUE(asyncRspOrError.IsSet());
    auto rspOrError = asyncRspOrError.Get();
    EXPECT_EQ(NRpc::EErrorCode::TransportError, rspOrError.GetCode());
    EXPECT_TRUE(Service_->GetSlowCallCanceled());
}

TEST_F(TRpcTest, ProtocolVersionMismatch)
{
    TMyIncorrectProtocolVersionProxy proxy(CreateChannel());
    auto req = proxy.SomeCall();
    req->set_a(42);
    auto rspOrError = req->Invoke().Get();
    EXPECT_EQ(NRpc::EErrorCode::ProtocolError, rspOrError.GetCode());
}

TEST_F(TRpcTest, StopWithoutActiveRequests)
{
    auto stopResult = Service_->Stop();
    EXPECT_TRUE(stopResult.IsSet());
}

TEST_F(TRpcTest, StopWithActiveRequests)
{
    TMyProxy proxy(CreateChannel());
    auto req = proxy.SlowCall();
    auto reqResult = req->Invoke();
    Sleep(TDuration::Seconds(0.5));
    auto stopResult = Service_->Stop();
    EXPECT_FALSE(stopResult.IsSet());
    EXPECT_TRUE(reqResult.Get().IsOK());
    Sleep(TDuration::Seconds(0.5));
    EXPECT_TRUE(stopResult.IsSet());
}

TEST_F(TRpcTest, NoMoreRequestsAfterStop)
{
    auto stopResult = Service_->Stop();
    EXPECT_TRUE(stopResult.IsSet());
    TMyProxy proxy(CreateChannel());
    auto req = proxy.SlowCall();
    auto reqResult = req->Invoke();
    EXPECT_FALSE(reqResult.Get().IsOK());
}

////////////////////////////////////////////////////////////////////////////////

#ifdef _linux_

// TTcpBusClient creates abstract unix sockets, supported only on Linux.
class TRpcUnixDomainTest
    : public TRpcTestBase
{
public:
    virtual IBusServerPtr CreateServer()
    {
        auto busConfig = TTcpBusServerConfig::CreateUnixDomain("unix_domain");
        return CreateTcpBusServer(busConfig);
    }

    IChannelPtr CreateChannel(const TString& address = "unix_domain")
    {
        auto clientConfig = TTcpBusClientConfig::CreateUnixDomain(address);
        auto client = CreateTcpBusClient(clientConfig);
        return CreateBusChannel(client);
    }
};

TEST_F(TRpcUnixDomainTest, Send)
{
    TMyProxy proxy(CreateChannel());
    auto req = proxy.SomeCall();
    req->set_a(42);
    auto rspOrError = req->Invoke().Get();
    EXPECT_TRUE(rspOrError.IsOK()) << ToString(rspOrError);
    const auto& rsp = rspOrError.Value();
    EXPECT_EQ(142, rsp->b());
}

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NRpc
} // namespace NYT
