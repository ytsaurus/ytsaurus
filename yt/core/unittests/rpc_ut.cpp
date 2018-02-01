#include <yt/core/test_framework/framework.h>

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

#include <yt/core/unittests/proto/rpc_ut.pb.h>

#include <yt/core/rpc/grpc/config.h>
#include <yt/core/rpc/grpc/channel.h>
#include <yt/core/rpc/grpc/server.h>

namespace NYT {
namespace NRpc {
namespace {

using namespace NBus;
using namespace NConcurrency;

static const TString DefaultAddress = "localhost:2000";

////////////////////////////////////////////////////////////////////////////////

class TMyProxy
    : public TProxyBase
{
public:
    DEFINE_RPC_PROXY(TMyProxy, MyService,
        .SetProtocolVersion(1));

    DEFINE_RPC_PROXY_METHOD(NMyRpc, SomeCall);
    DEFINE_RPC_PROXY_METHOD(NMyRpc, PassCall);
    DEFINE_RPC_PROXY_METHOD(NMyRpc, RegularAttachments);
    DEFINE_RPC_PROXY_METHOD(NMyRpc, NullAndEmptyAttachments);
    DEFINE_RPC_PROXY_METHOD(NMyRpc, DoNothing);
    DEFINE_RPC_PROXY_METHOD(NMyRpc, CustomMessageError);
    DEFINE_RPC_PROXY_METHOD(NMyRpc, NotRegistered);
    DEFINE_RPC_PROXY_METHOD(NMyRpc, SlowCall);
    DEFINE_RPC_PROXY_METHOD(NMyRpc, SlowCanceledCall);
    DEFINE_RPC_PROXY_METHOD(NMyRpc, NoReply);
};

class TNonExistingServiceProxy
    : public TProxyBase
{
public:
    DEFINE_RPC_PROXY(TNonExistingServiceProxy, NonExistingService);

    DEFINE_RPC_PROXY_METHOD(NMyRpc, DoNothing);
};

class TMyIncorrectProtocolVersionProxy
    : public TProxyBase
{
public:
    DEFINE_RPC_PROXY(TMyIncorrectProtocolVersionProxy, MyService,
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
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PassCall));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(RegularAttachments));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(NullAndEmptyAttachments));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(DoNothing));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(CustomMessageError));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(SlowCall)
            .SetCancelable(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(SlowCanceledCall)
            .SetCancelable(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(NoReply));
        // NB: NotRegisteredCall is not registered intentionally
    }

    DECLARE_RPC_SERVICE_METHOD(NMyRpc, SomeCall)
    {
        context->SetRequestInfo();
        int a = request->a();
        response->set_b(a + 100);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NMyRpc, PassCall)
    {
        context->SetRequestInfo();
        response->set_user(context->GetUser());
        response->set_mutation_id(ToString(context->GetMutationId()));
        response->set_retry(context->IsRetry());
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


    bool GetSlowCallCanceled() const
    {
        return SlowCallCanceled_;
    }

private:
    bool SlowCallCanceled_ = false;

};

////////////////////////////////////////////////////////////////////////////////

class TGrpcImpl;
template <class TImpl>
class TRpcOverBus;

template <class T>
struct TErrorCodeTraits {};

template <class T>
struct TErrorCodeTraits<TRpcOverBus<T>>
{
    static constexpr int TimeoutCode = static_cast<int>(NYT::EErrorCode::Timeout);
    static constexpr int CancelCode = static_cast<int>(NYT::EErrorCode::Canceled);
    static constexpr int TransportCode = static_cast<int>(NRpc::EErrorCode::TransportError);
};

template<>
struct TErrorCodeTraits<TGrpcImpl>
{
    static constexpr int TimeoutCode = NGrpc::GenericErrorStatusCode;
    static constexpr int CancelCode = static_cast<int>(NYT::EErrorCode::Canceled);
    static constexpr int TransportCode = NGrpc::GenericErrorStatusCode;
};

template <class TImpl>
class TTestBase
    : public ::testing::Test
{
public:
    virtual void SetUp() override final
    {
        Server_ = CreateServer();
        Queue_ = New<TActionQueue>();
        Service_ = New<TMyService>(Queue_->GetInvoker());
        Server_->RegisterService(Service_);
        Server_->Start();
    }

    virtual void TearDown() override final
    {
        Server_->Stop().Get().ThrowOnError();
        Server_.Reset();
    }

    IServerPtr CreateServer()
    {
        return TImpl::CreateServer();
    }

    IChannelPtr CreateChannel(const TString& address = DefaultAddress)
    {
        return TImpl::CreateChannel(address);
    }

    int ConvertToInt(NYT::TErrorCode errorCode)
    {
        return errorCode;
    }

    int GetTimeoutCode()
    {
        return TErrorCodeTraits<TImpl>::TimeoutCode;
    }

    int GetCancelCode()
    {
        return TErrorCodeTraits<TImpl>::CancelCode;
    }

    int GetTransportCode()
    {
        return TErrorCodeTraits<TImpl>::TransportCode;
    }
protected:
    TActionQueuePtr Queue_;
    TIntrusivePtr<TMyService> Service_;
    IServerPtr Server_;
};

////////////////////////////////////////////////////////////////////////////////

template <class TImpl>
class TRpcOverBus
{
public:
    static IServerPtr CreateServer()
    {
        auto busServer = MakeBusServer();
        return CreateBusServer(busServer);
    }

    static IChannelPtr CreateChannel(const TString& address)
    {
        return TImpl::CreateChannel(address);
    }

    static IBusServerPtr MakeBusServer()
    {
        return TImpl::MakeBusServer();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TRpcOverBusImpl
{
public:
    static IChannelPtr CreateChannel(const TString& address)
    {
        auto client = CreateTcpBusClient(TTcpBusClientConfig::CreateTcp(address));
        return CreateBusChannel(client);
    }

    static IBusServerPtr MakeBusServer()
    {
        auto busConfig = TTcpBusServerConfig::CreateTcp(2000);
        return CreateTcpBusServer(busConfig);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TGrpcImpl
{
public:
    static IChannelPtr CreateChannel(const TString& address)
    {
        auto channelConfig = New<NGrpc::TChannelConfig>();
        channelConfig->Address = address;
        return NGrpc::CreateGrpcChannel(channelConfig);
    }

    static IServerPtr CreateServer()
    {
        auto serverAddressConfig = New<NGrpc::TServerAddressConfig>();
        serverAddressConfig->Type = NGrpc::EAddressType::Insecure;
        serverAddressConfig->Address = DefaultAddress;
        auto serverConfig = New<NGrpc::TServerConfig>();
        serverConfig->Addresses.push_back(serverAddressConfig);
        return NGrpc::CreateServer(serverConfig);
    }
};

////////////////////////////////////////////////////////////////////////////////

// TTcpBusClient creates abstract unix sockets, supported only on Linux.
class TRpcOverUnixDomainImpl
{
public:
    static IBusServerPtr MakeBusServer()
    {
        auto busConfig = TTcpBusServerConfig::CreateUnixDomain("unix_domain");
        return CreateTcpBusServer(busConfig);
    }

    static IChannelPtr CreateChannel(const TString& address)
    {
        auto clientConfig = TTcpBusClientConfig::CreateUnixDomain(
            address == DefaultAddress ? "unix_domain" : address);
        auto client = CreateTcpBusClient(clientConfig);
        return CreateBusChannel(client);
    }
};

////////////////////////////////////////////////////////////////////////////////

#ifdef _linux_
using AllTransport = ::testing::Types<
    TRpcOverBus<TRpcOverBusImpl>,
    TGrpcImpl,
    TRpcOverBus<TRpcOverUnixDomainImpl>>;
using WithoutGrpc = ::testing::Types<
    TRpcOverBus<TRpcOverBusImpl>,
    TRpcOverBus<TRpcOverUnixDomainImpl>>;
#else
using AllTransport = ::testing::Types<
    TRpcOverBus<TRpcOverBusImpl>,
    TGrpcImpl>;
using WithoutGrpc = ::testing::Types<
    TRpcOverBus<TRpcOverBusImpl>>;
#endif

template <class I>
using TRpcTest = TTestBase<I>;
template <class I>
using TNotGrpcTest = TTestBase<I>;
TYPED_TEST_CASE(TRpcTest, AllTransport);
TYPED_TEST_CASE(TNotGrpcTest, WithoutGrpc);

////////////////////////////////////////////////////////////////////////////////

TYPED_TEST(TRpcTest, Send)
{
    TMyProxy proxy(this->CreateChannel());
    auto req = proxy.SomeCall();
    req->set_a(42);
    auto rspOrError = req->Invoke().Get();
    EXPECT_TRUE(rspOrError.IsOK()) << ToString(rspOrError);
    const auto& rsp = rspOrError.Value();
    EXPECT_EQ(142, rsp->b());
}

TYPED_TEST(TNotGrpcTest, SendSimple)
{
    TMyProxy proxy(this->CreateChannel());
    auto req = proxy.PassCall();
    auto mutation_id = TGuid::Create();
    req->SetUser("test");
    req->SetMutationId(mutation_id);
    req->SetRetry(true);
    auto rspOrError = req->Invoke().Get();
    EXPECT_TRUE(rspOrError.IsOK()) << ToString(rspOrError);
    const auto& rsp = rspOrError.Value();
    EXPECT_EQ("test", rsp->user());
    EXPECT_EQ(ToString(mutation_id), rsp->mutation_id());
    EXPECT_EQ(true, rsp->retry());
}

TYPED_TEST(TRpcTest, ManyAsyncRequests)
{
    const int RequestCount = 1000;

    std::vector<TFuture<void>> asyncResults;

    TMyProxy proxy(this->CreateChannel());

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

TYPED_TEST(TNotGrpcTest, RegularAttachments)
{
    TMyProxy proxy(this->CreateChannel());
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

TYPED_TEST(TNotGrpcTest, NullAndEmptyAttachments)
{
    TMyProxy proxy(this->CreateChannel());
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

TYPED_TEST(TRpcTest, OK)
{
    TMyProxy proxy(this->CreateChannel());
    auto req = proxy.DoNothing();
    auto rspOrError = req->Invoke().Get();
    EXPECT_TRUE(rspOrError.IsOK());
}

TYPED_TEST(TRpcTest, NoAck)
{
    TMyProxy proxy(this->CreateChannel());
    auto req = proxy.DoNothing();
    req->SetRequestAck(false);
    auto rspOrError = req->Invoke().Get();
    EXPECT_TRUE(rspOrError.IsOK());
}

TYPED_TEST(TRpcTest, TransportError)
{
    TMyProxy proxy(this->CreateChannel("localhost:9999"));
    auto req = proxy.DoNothing();
    auto rspOrError = req->Invoke().Get();
    EXPECT_EQ(NRpc::EErrorCode::TransportError, rspOrError.GetCode());
}

TYPED_TEST(TRpcTest, NoService)
{
    TNonExistingServiceProxy proxy(this->CreateChannel());
    auto req = proxy.DoNothing();
    auto rspOrError = req->Invoke().Get();
    EXPECT_EQ(NRpc::EErrorCode::NoSuchService, rspOrError.GetCode());
}

TYPED_TEST(TRpcTest, NoMethod)
{
    TMyProxy proxy(this->CreateChannel());
    auto req = proxy.NotRegistered();
    auto rspOrError = req->Invoke().Get();
    EXPECT_EQ(NRpc::EErrorCode::NoSuchMethod, rspOrError.GetCode());
}

TYPED_TEST(TRpcTest, ClientTimeout)
{
    TMyProxy proxy(this->CreateChannel());
    proxy.SetDefaultTimeout(TDuration::Seconds(0.5));
    auto req = proxy.SlowCall();
    auto rspOrError = req->Invoke().Get();
    EXPECT_EQ(this->GetTimeoutCode(), this->ConvertToInt(rspOrError.GetCode()));
}

TYPED_TEST(TRpcTest, ServerTimeout)
{
    TMyProxy proxy(this->CreateChannel());
    proxy.SetDefaultTimeout(TDuration::Seconds(0.5));
    auto req = proxy.SlowCanceledCall();
    auto rspOrError = req->Invoke().Get();
    EXPECT_EQ(this->GetTimeoutCode(), this->ConvertToInt(rspOrError.GetCode()));
    Sleep(TDuration::Seconds(1));
    EXPECT_TRUE(this->Service_->GetSlowCallCanceled());
}

TYPED_TEST(TRpcTest , ClientCancel)
{
    TMyProxy proxy(this->CreateChannel());
    auto req = proxy.SlowCanceledCall();
    auto asyncRspOrError = req->Invoke();
    Sleep(TDuration::Seconds(0.5));
    EXPECT_FALSE(asyncRspOrError.IsSet());
    asyncRspOrError.Cancel();
    Sleep(TDuration::Seconds(0.1));
    EXPECT_TRUE(asyncRspOrError.IsSet());
    auto rspOrError = asyncRspOrError.Get();
    EXPECT_EQ(this->GetCancelCode(), this->ConvertToInt(rspOrError.GetCode()));
    Sleep(TDuration::Seconds(1));
    EXPECT_TRUE(this->Service_->GetSlowCallCanceled());
}

TYPED_TEST(TRpcTest, SlowCall)
{
    TMyProxy proxy(this->CreateChannel());
    proxy.SetDefaultTimeout(TDuration::Seconds(2.0));
    auto req = proxy.SlowCall();
    auto rspOrError = req->Invoke().Get();
    EXPECT_TRUE(rspOrError.IsOK());
}

TYPED_TEST(TRpcTest, NoReply)
{
    TMyProxy proxy(this->CreateChannel());
    auto req = proxy.NoReply();
    auto rspOrError = req->Invoke().Get();
    EXPECT_EQ(NRpc::EErrorCode::Unavailable, rspOrError.GetCode());
}

TYPED_TEST(TRpcTest, CustomErrorMessage)
{
    TMyProxy proxy(this->CreateChannel());
    auto req = proxy.CustomMessageError();
    auto rspOrError = req->Invoke().Get();
    EXPECT_EQ(NYT::EErrorCode(42), rspOrError.GetCode());
    EXPECT_EQ("Some Error", rspOrError.GetMessage());
}

TYPED_TEST(TRpcTest, ConnectionLost)
{
    TMyProxy proxy(this->CreateChannel());

    auto req = proxy.SlowCanceledCall();
    auto asyncRspOrError = req->Invoke();

    Sleep(TDuration::Seconds(0.5));

    EXPECT_FALSE(asyncRspOrError.IsSet());
    this->Server_->Stop(false);

    Sleep(TDuration::Seconds(0.5));

    EXPECT_TRUE(asyncRspOrError.IsSet());
    auto rspOrError = asyncRspOrError.Get();
    EXPECT_EQ(this->GetTransportCode(), this->ConvertToInt(rspOrError.GetCode()));
    EXPECT_TRUE(this->Service_->GetSlowCallCanceled());
}

TYPED_TEST(TNotGrpcTest, ProtocolVersionMismatch)
{
    TMyIncorrectProtocolVersionProxy proxy(this->CreateChannel());
    auto req = proxy.SomeCall();
    req->set_a(42);
    auto rspOrError = req->Invoke().Get();
    EXPECT_EQ(NRpc::EErrorCode::ProtocolError, rspOrError.GetCode());
}

TYPED_TEST(TRpcTest, StopWithoutActiveRequests)
{
    auto stopResult = this->Service_->Stop();
    EXPECT_TRUE(stopResult.IsSet());
}

TYPED_TEST(TRpcTest, StopWithActiveRequests)
{
    TMyProxy proxy(this->CreateChannel());
    auto req = proxy.SlowCall();
    auto reqResult = req->Invoke();
    Sleep(TDuration::Seconds(0.5));
    auto stopResult = this->Service_->Stop();
    EXPECT_FALSE(stopResult.IsSet());
    EXPECT_TRUE(reqResult.Get().IsOK());
    Sleep(TDuration::Seconds(0.5));
    EXPECT_TRUE(stopResult.IsSet());
}

TYPED_TEST(TRpcTest, NoMoreRequestsAfterStop)
{
    auto stopResult = this->Service_->Stop();
    EXPECT_TRUE(stopResult.IsSet());
    TMyProxy proxy(this->CreateChannel());
    auto req = proxy.SlowCall();
    auto reqResult = req->Invoke();
    EXPECT_FALSE(reqResult.Get().IsOK());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NRpc
} // namespace NYT
