#include "stdafx.h"

#include "rpc_ut.pb.h"

#include <yt/ytlib/misc/error.h>

#include <yt/ytlib/bus/bus.h>
#include <yt/ytlib/rpc/client.h>
#include <yt/ytlib/rpc/server.h>
#include <yt/ytlib/rpc/service.h>

#include <contrib/testing/framework.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

class TMyProxy
    : public TProxyBase
{
public:
    static const Stroka ServiceName;

    TMyProxy(IChannel* channel)
        : TProxyBase(channel, ServiceName)
    { }

    RPC_PROXY_METHOD(NMyRpc, SomeCall);
    RPC_PROXY_METHOD(NMyRpc, ModifyAttachments);
    RPC_PROXY_METHOD(NMyRpc, ReplyingCall);
    RPC_PROXY_METHOD(NMyRpc, EmptyCall);
    RPC_PROXY_METHOD(NMyRpc, CustomMessageError);
    RPC_PROXY_METHOD(NMyRpc, NotRegistredCall);
};

const Stroka TMyProxy::ServiceName = "MyService";

////////////////////////////////////////////////////////////////////////////////

class TNonExistingServiceProxy
    : public TProxyBase
{
public:
    typedef TIntrusivePtr<TNonExistingServiceProxy> TPtr;

    static const Stroka ServiceName;

    TNonExistingServiceProxy(IChannel* channel)
        : TProxyBase(channel, ServiceName)
    { }

    RPC_PROXY_METHOD(NMyRpc, EmptyCall);
};

const Stroka TNonExistingServiceProxy::ServiceName = "NonExistingService";

////////////////////////////////////////////////////////////////////////////////

Stroka StringFromSharedRef(const TSharedRef& sharedRef)
{
    auto blob = sharedRef.ToBlob();
    return Stroka(blob.begin(), blob.end());
}


TSharedRef SharedRefFromString(const Stroka& s)
{
    TBlob blob(s.begin(), s.end());
    return MoveRV(blob);
}

////////////////////////////////////////////////////////////////////////////////

class TMyService
    : public TServiceBase
{
public:
    typedef TIntrusivePtr<TMyService> TPtr;
    typedef TMyService TThis;
    TMyService(IInvoker* invoker)
        : TServiceBase(
            invoker,
            TMyProxy::ServiceName,
            "Main")
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(SomeCall));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ModifyAttachments));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ReplyingCall));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(EmptyCall));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(CustomMessageError));
    }

    DECLARE_RPC_SERVICE_METHOD(NMyRpc, SomeCall);
    DECLARE_RPC_SERVICE_METHOD(NMyRpc, ModifyAttachments);
    DECLARE_RPC_SERVICE_METHOD(NMyRpc, ReplyingCall);
    DECLARE_RPC_SERVICE_METHOD(NMyRpc, EmptyCall);
    DECLARE_RPC_SERVICE_METHOD(NMyRpc, CustomMessageError);

    DECLARE_RPC_SERVICE_METHOD(NMyRpc, NotRegistredCall);

};

DEFINE_RPC_SERVICE_METHOD_IMPL(TMyService, SomeCall)
{
    int a = request->GetA();
    response->SetB(a + 100);
    context->Reply();
}

DEFINE_RPC_SERVICE_METHOD_IMPL(TMyService, ReplyingCall)
{
    UNUSED(request);
    UNUSED(response);
    context->Reply();
}

DEFINE_RPC_SERVICE_METHOD_IMPL(TMyService, ModifyAttachments)
{
    for (int i = 0; i < request->Attachments().ysize(); ++i) {
        auto blob = request->Attachments()[i].ToBlob();
        blob.push_back('_');

        response->Attachments().push_back(MoveRV(blob));
    }
    context->Reply();
}

DEFINE_RPC_SERVICE_METHOD_IMPL(TMyService, EmptyCall)
{
    UNUSED(request);
    UNUSED(response);
    UNUSED(context);
}

DEFINE_RPC_SERVICE_METHOD_IMPL(TMyService, NotRegistredCall)
{
    UNUSED(request);
    UNUSED(response);
    UNUSED(context);
}

DEFINE_RPC_SERVICE_METHOD_IMPL(TMyService, CustomMessageError)
{

    UNUSED(request);
    UNUSED(response);
    context->Reply(TError(42, "Some Error"));
}

class TRpcTest
    : public ::testing::Test
{
    IServer::TPtr Server;

public:
    virtual void SetUp()
    {
        Server = CreateRpcServer(2000);
        auto queue = New<TActionQueue>();
        Server->RegisterService(~New<TMyService>(~queue->GetInvoker()));
        Server->Start();
    }

    virtual void TearDown()
    {
        Server->Stop();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TResponseHandler
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TResponseHandler> TPtr;

    TResponseHandler(int numRepliesWaiting)
        : NumRepliesWaiting(numRepliesWaiting)
    { }

    Event Event_;

    USE_RPC_PROXY_METHOD(TMyProxy, SomeCall);

    void CheckReply(TRspSomeCall::TPtr response, int expected)
    {
        EXPECT_IS_TRUE(response->IsOK());
        EXPECT_EQ(expected, response->GetB());

        --NumRepliesWaiting;
        if (NumRepliesWaiting == 0) {
            Event_.Signal();
        }
    }

private:
    int NumRepliesWaiting;

};


////////////////////////////////////////////////////////////////////////////////

TEST_F(TRpcTest, Send)
{
    TAutoPtr<TMyProxy> proxy = new TMyProxy(~CreateBusChannel("localhost:2000"));
    auto request = proxy->SomeCall();
    request->SetA(42);
    auto result = request->Invoke();
    auto response = result->Get();

    EXPECT_IS_TRUE(response->IsOK());
    EXPECT_EQ(142, response->GetB());
}

TEST_F(TRpcTest, ManyAsyncSends)
{
    int numSends = 1000;
    auto handler = New<TResponseHandler>(numSends);

    auto proxy = new TMyProxy(~CreateBusChannel("localhost:2000"));

    for (int i = 0; i < numSends; ++i) {
        auto request = proxy->SomeCall();
        request->SetA(i);
        request->Invoke()->Subscribe(FromMethod(&TResponseHandler::CheckReply, handler, i + 100));
    }

    if (!handler->Event_.WaitT(TDuration::Seconds(4))) {
        EXPECT_IS_TRUE(false); // timeout occured
    }
}

TEST_F(TRpcTest, Attachments)
{
    TAutoPtr<TMyProxy> proxy = new TMyProxy(~CreateBusChannel("localhost:2000"));
    auto request = proxy->ModifyAttachments();

    request->Attachments().push_back(SharedRefFromString("Hello"));
    request->Attachments().push_back(SharedRefFromString("from"));
    request->Attachments().push_back(SharedRefFromString("TMyProxy"));

    auto result = request->Invoke();
    auto response = result->Get();

    const auto& attachments = response->Attachments();
    EXPECT_EQ(3, attachments.ysize());
    EXPECT_EQ("Hello_",     StringFromSharedRef(attachments[0]));
    EXPECT_EQ("from_",      StringFromSharedRef(attachments[1]));
    EXPECT_EQ("TMyProxy_",  StringFromSharedRef(attachments[2]));
}

// Now test different types of errors
TEST_F(TRpcTest, OK)
{
    TAutoPtr<TMyProxy> proxy = new TMyProxy(~CreateBusChannel("localhost:2000"));
    auto request = proxy->ReplyingCall();
    auto result = request->Invoke();
    auto response = result->Get();

    EXPECT_EQ(TError::OK, response->GetErrorCode());
}

TEST_F(TRpcTest, TransportError)
{
    TAutoPtr<TMyProxy> proxy = new TMyProxy(~CreateBusChannel("localhost:9999"));
    auto request = proxy->EmptyCall();
    auto result = request->Invoke();
    auto response = result->Get();

    EXPECT_EQ(EErrorCode::TransportError, response->GetErrorCode());
}

TEST_F(TRpcTest, NoService)
{
    TAutoPtr<TNonExistingServiceProxy> proxy = new TNonExistingServiceProxy(~CreateBusChannel("localhost:2000"));
    auto request = proxy->EmptyCall();
    auto result = request->Invoke();
    auto response = result->Get();

    EXPECT_EQ(EErrorCode::NoSuchService, response->GetErrorCode());
}

TEST_F(TRpcTest, NoMethod)
{
    TAutoPtr<TMyProxy> proxy = new TMyProxy(~CreateBusChannel("localhost:2000"));
    auto request = proxy->NotRegistredCall();
    auto result = request->Invoke();
    auto response = result->Get();

    EXPECT_EQ(EErrorCode::NoSuchVerb, response->GetErrorCode());
}

TEST_F(TRpcTest, Timeout)
{
    TAutoPtr<TMyProxy> proxy = new TMyProxy(~CreateBusChannel("localhost:2000"));
    proxy->SetTimeout(TDuration::Seconds(1));

    auto request = proxy->EmptyCall();
    auto result = request->Invoke();
    auto response = result->Get();

    EXPECT_EQ(EErrorCode::Timeout, response->GetErrorCode());
}

TEST_F(TRpcTest, CustomMessage)
{
    TAutoPtr<TMyProxy> proxy = new TMyProxy(~CreateBusChannel("localhost:2000"));
    auto request = proxy->CustomMessageError();
    auto result = request->Invoke();
    auto response = result->Get();

    EXPECT_EQ(42, response->GetErrorCode());
    EXPECT_EQ("Some Error", response->GetError().GetMessage());
}


////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
