#include "stdafx.h"

#include "rpc_ut.pb.h"

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
    typedef TIntrusivePtr<TMyProxy> TPtr;

    static const Stroka ServiceName;

    TMyProxy(IChannel::TPtr channel)
        : TProxyBase(channel, ServiceName)
    { }

    RPC_PROXY_METHOD(NMyRpc, SomeCall);
    RPC_PROXY_METHOD(NMyRpc, ModifyAttachments);
    RPC_PROXY_METHOD(NMyRpc, ReplyingCall);
    RPC_PROXY_METHOD(NMyRpc, EmptyCall);
    RPC_PROXY_METHOD(NMyRpc, CustomMessageError);
    RPC_PROXY_METHOD(NMyRpc, NotRegistredCall);
};

const Stroka TMyProxy::ServiceName = "RpcUT";

////////////////////////////////////////////////////////////////////////////////

class TNonExistingServiceProxy
    : public TProxyBase
{
public:
    typedef TIntrusivePtr<TNonExistingServiceProxy> TPtr;

    static const Stroka ServiceName;

    TNonExistingServiceProxy(IChannel::TPtr channel)
        : TProxyBase(channel, ServiceName)
    { }

    RPC_PROXY_METHOD(NMyRpc, EmptyCall);
};

const Stroka TNonExistingServiceProxy::ServiceName = "NonExistingService";

////////////////////////////////////////////////////////////////////////////////

Stroka StringFromSharedRef(TSharedRef sharedRef)
{
    TBlob blob = sharedRef.ToBlob();
    return Stroka(blob.begin(), blob.end());
}


TSharedRef SharedRefFromString(const Stroka& s)
{
    TBlob blob = TBlob(s.begin(), s.end());
    return TSharedRef(blob);
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

    RPC_SERVICE_METHOD_DECL(NMyRpc, SomeCall);
    RPC_SERVICE_METHOD_DECL(NMyRpc, ModifyAttachments);
    RPC_SERVICE_METHOD_DECL(NMyRpc, ReplyingCall);
    RPC_SERVICE_METHOD_DECL(NMyRpc, EmptyCall);
    RPC_SERVICE_METHOD_DECL(NMyRpc, CustomMessageError);

    RPC_SERVICE_METHOD_DECL(NMyRpc, NotRegistredCall);

};

RPC_SERVICE_METHOD_IMPL(TMyService, SomeCall)
{
    int a = request->GetA();
    response->SetB(a + 100);
    context->Reply();
}

RPC_SERVICE_METHOD_IMPL(TMyService, ReplyingCall)
{
    UNUSED(request);
    UNUSED(response);
    context->Reply();
}

RPC_SERVICE_METHOD_IMPL(TMyService, ModifyAttachments)
{
    for (int i = 0; i < request->Attachments().ysize(); ++i) {
        TBlob blob = request->Attachments()[i].ToBlob();
        blob.push_back('_');

        response->Attachments().push_back(TSharedRef(blob));
    }
    context->Reply();
}

RPC_SERVICE_METHOD_IMPL(TMyService, EmptyCall)
{
    UNUSED(request);
    UNUSED(response);
    UNUSED(context);
}

RPC_SERVICE_METHOD_IMPL(TMyService, NotRegistredCall)
{
    UNUSED(request);
    UNUSED(response);
    UNUSED(context);
}

RPC_SERVICE_METHOD_IMPL(TMyService, CustomMessageError)
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

TEST_F(TRpcTest, Send)
{
    auto proxy = new TMyProxy(CreateBusChannel("localhost:2000"));
    auto request = proxy->SomeCall();
    request->SetA(42);
    auto result = request->Invoke();
    auto response = result->Get();

    EXPECT_IS_TRUE(response->IsOK());
    EXPECT_EQ(142, response->GetB());
}

TEST_F(TRpcTest, Attachments)
{
    auto proxy = new TMyProxy(CreateBusChannel("localhost:2000"));
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
    auto proxy = new TMyProxy(CreateBusChannel("localhost:2000"));
    auto request = proxy->ReplyingCall();
    auto result = request->Invoke();
    auto response = result->Get();

    EXPECT_EQ(EErrorCode::OK, response->GetErrorCode());
}

TEST_F(TRpcTest, TransportError)
{
    auto proxy = new TMyProxy(CreateBusChannel("localhost:2001"));
    auto request = proxy->EmptyCall();
    auto result = request->Invoke();
    auto response = result->Get();

    EXPECT_EQ(EErrorCode::TransportError, response->GetErrorCode());
}

// TODO: uncomment this when YT-276 is fixed
//TEST_F(TRpcTest, NoService)
//{
//    auto proxy = new TNonExistingServiceProxy(New<TChannel>("localhost:2000"));
//    auto request = proxy->EmptyCall();
//    auto result = request->Invoke();
//    auto response = result->Get();

//    EXPECT_EQ(EErrorCode::NoService, response->GetErrorCode());
//}

//TEST_F(TRpcTest, NoMethod)
//{
//    auto proxy = new TMyProxy(New<TChannel>("localhost:2000"));
//    auto request = proxy->NotRegistredCall();
//    auto result = request->Invoke();
//    auto response = result->Get();

//    EXPECT_EQ(EErrorCode::NoMethod, response->GetErrorCode());
//}

TEST_F(TRpcTest, Timeout)
{
    auto proxy = new TMyProxy(CreateBusChannel("localhost:2000"));
    proxy->SetTimeout(TDuration::Seconds(1));

    auto request = proxy->EmptyCall();
    auto result = request->Invoke();
    auto response = result->Get();

    EXPECT_EQ(EErrorCode::Timeout, response->GetErrorCode());
}

TEST_F(TRpcTest, CustomMessage)
{
    auto proxy = new TMyProxy(CreateBusChannel("localhost:2000"));
    auto request = proxy->CustomMessageError();
    auto result = request->Invoke();
    auto response = result->Get();

    EXPECT_EQ(42, response->GetErrorCode());
    EXPECT_EQ("Some Error", response->GetError().GetMessage());
}


////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
