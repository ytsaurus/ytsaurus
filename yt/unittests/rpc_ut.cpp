#include "stdafx.h"

#include <yt/unittests/rpc_ut.pb.h>

#include <ytlib/misc/error.h>

#include <ytlib/actions/action_queue.h>

#include <ytlib/bus/bus.h>
#include <ytlib/bus/config.h>
#include <ytlib/bus/tcp_client.h>
#include <ytlib/bus/tcp_server.h>

#include <ytlib/rpc/client.h>
#include <ytlib/rpc/server.h>
#include <ytlib/rpc/service.h>
#include <ytlib/rpc/bus_channel.h>

#include <contrib/testing/framework.h>

namespace NYT {

using namespace NBus;
using namespace NRpc;

////////////////////////////////////////////////////////////////////////////////

class TMyProxy
    : public TProxyBase
{
public:
    static const Stroka ServiceName;

    explicit TMyProxy(IChannelPtr channel)
        : TProxyBase(channel, ServiceName)
    { }

    DEFINE_RPC_PROXY_METHOD(NMyRpc, SomeCall);
    DEFINE_RPC_PROXY_METHOD(NMyRpc, ModifyAttachments);
    DEFINE_RPC_PROXY_METHOD(NMyRpc, ModifyAttributes);
    DEFINE_RPC_PROXY_METHOD(NMyRpc, ReplyingCall);
    DEFINE_RPC_PROXY_METHOD(NMyRpc, EmptyCall);
    DEFINE_RPC_PROXY_METHOD(NMyRpc, CustomMessageError);
    DEFINE_RPC_PROXY_METHOD(NMyRpc, NotRegisteredCall);
    DEFINE_RPC_PROXY_METHOD(NMyRpc, LongReply);

    DEFINE_ONE_WAY_RPC_PROXY_METHOD(NMyRpc, OneWay);
    DEFINE_ONE_WAY_RPC_PROXY_METHOD(NMyRpc, CheckAll);
    DEFINE_ONE_WAY_RPC_PROXY_METHOD(NMyRpc, NotRegistredOneWay);

};

const Stroka TMyProxy::ServiceName = "MyService";

////////////////////////////////////////////////////////////////////////////////

class TNonExistingServiceProxy
    : public TProxyBase
{
public:
    typedef TIntrusivePtr<TNonExistingServiceProxy> TPtr;

    static const Stroka ServiceName;

    TNonExistingServiceProxy(IChannelPtr channel)
        : TProxyBase(channel, ServiceName)
    { }

    DEFINE_RPC_PROXY_METHOD(NMyRpc, EmptyCall);
    DEFINE_ONE_WAY_RPC_PROXY_METHOD(NMyRpc, OneWay);
};

const Stroka TNonExistingServiceProxy::ServiceName = "NonExistingService";

////////////////////////////////////////////////////////////////////////////////

Stroka StringFromSharedRef(const TSharedRef& sharedRef)
{
    return Stroka(sharedRef.Begin(), sharedRef.Begin() + sharedRef.Size());
}


TSharedRef SharedRefFromString(const Stroka& s)
{
    TBlob blob(s.begin(), s.end());
    return MoveRV(blob);
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
    typedef TMyService TThis;

    TMyService(IInvokerPtr invoker, Event* event)
        : TServiceBase(invoker, TMyProxy::ServiceName, "Main")
        , Event_(event)
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(SomeCall));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ModifyAttachments));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ModifyAttributes));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(ReplyingCall));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(EmptyCall));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(CustomMessageError));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(LongReply));

        RegisterMethod(RPC_SERVICE_METHOD_DESC(OneWay)
            .SetOneWay(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(CheckAll)
            .SetOneWay(true));

        // Note: NotRegisteredCall and NotRegistredOneWay are not registered
    }

    DECLARE_RPC_SERVICE_METHOD(NMyRpc, SomeCall);
    DECLARE_RPC_SERVICE_METHOD(NMyRpc, ModifyAttachments);
    DECLARE_RPC_SERVICE_METHOD(NMyRpc, ModifyAttributes);
    DECLARE_RPC_SERVICE_METHOD(NMyRpc, ReplyingCall);
    DECLARE_RPC_SERVICE_METHOD(NMyRpc, EmptyCall);
    DECLARE_RPC_SERVICE_METHOD(NMyRpc, CustomMessageError);
    DECLARE_RPC_SERVICE_METHOD(NMyRpc, LongReply);

    DECLARE_ONE_WAY_RPC_SERVICE_METHOD(NMyRpc, OneWay);
    DECLARE_ONE_WAY_RPC_SERVICE_METHOD(NMyRpc, CheckAll);

    DECLARE_RPC_SERVICE_METHOD(NMyRpc, NotRegisteredCall);
    DECLARE_ONE_WAY_RPC_SERVICE_METHOD(NMyRpc, NotRegistredOneWay);
private:
    // To signal for one-way rpc requests when processed the request
    Event* Event_;
};

DEFINE_RPC_SERVICE_METHOD(TMyService, SomeCall)
{
    int a = request->a();
    response->set_b(a + 100);
    context->Reply();
}

DEFINE_RPC_SERVICE_METHOD(TMyService, ReplyingCall)
{
    UNUSED(request);
    UNUSED(response);
    context->Reply();
}

DEFINE_RPC_SERVICE_METHOD(TMyService, EmptyCall)
{
    UNUSED(request);
    UNUSED(response);
    UNUSED(context);
}

DEFINE_RPC_SERVICE_METHOD(TMyService, LongReply)
{
    UNUSED(request);
    UNUSED(response);
    Sleep(TDuration::Seconds(5));
    context->Reply();
}


DEFINE_RPC_SERVICE_METHOD(TMyService, NotRegisteredCall)
{
    UNUSED(request);
    UNUSED(response);
    UNUSED(context);
}

DEFINE_RPC_SERVICE_METHOD(TMyService, CustomMessageError)
{

    UNUSED(request);
    UNUSED(response);
    context->Reply(TError(42, "Some Error"));
}

DEFINE_ONE_WAY_RPC_SERVICE_METHOD(TMyService, OneWay)
{
    UNUSED(request);
}

////////////////////////////////////////////////////////////////////////////////

class TRpcTest
    : public ::testing::Test
{
    // need to remember
    TActionQueuePtr Queue;
public:
    IServerPtr RpcServer;

    virtual void SetUp()
    {
        auto busConfig = New<NBus::TTcpBusServerConfig>();
        busConfig->Port = 2000;
        auto busServer = NBus::CreateTcpBusServer(busConfig);

        RpcServer = CreateRpcServer(busServer);

        Queue = New<TActionQueue>();

        RpcServer->RegisterService(New<TMyService>(Queue->GetInvoker(), &ReadyEvent));
        RpcServer->Start();
    }

    virtual void TearDown()
    {
        RpcServer->Stop();
        RpcServer.Reset();
    }

    // For services to signal when they processed incoming onewey rpc request
    Event ReadyEvent;
};

////////////////////////////////////////////////////////////////////////////////

class TResponseHandler
    : public TRefCounted
{
public:
    TResponseHandler(int numRepliesWaiting, Event* event_)
        : NumRepliesWaiting(numRepliesWaiting)
        , Event_(event_)
    { }

    void CheckReply(int expected, TMyProxy::TRspSomeCallPtr response)
    {
        EXPECT_TRUE(response->IsOK());
        EXPECT_EQ(expected, response->b());

        if (AtomicDecrement(NumRepliesWaiting) == 0) {
            Event_->Signal();
        }
    }

private:
    TAtomic NumRepliesWaiting;
    Event* Event_;

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

TEST_F(TRpcTest, ManyAsyncSends)
{
    int numSends = 1000;

    Event event;
    auto handler = New<TResponseHandler>(numSends, &event);

    TMyProxy proxy(CreateChannel("localhost:2000"));

    for (int i = 0; i < numSends; ++i) {
        auto request = proxy.SomeCall();
        request->set_a(i);
        request->Invoke().Subscribe(BIND(
            &TResponseHandler::CheckReply,
            handler,
            i + 100));
    }

    EXPECT_TRUE(event.WaitT(TDuration::Seconds(4))); // assert no timeout
}

////////////////////////////////////////////////////////////////////////////////

DEFINE_RPC_SERVICE_METHOD(TMyService, ModifyAttachments)
{
    FOREACH(const auto& attachment, request->Attachments()) {
        std::vector<char> data(attachment.Begin(), attachment.End());
        data.push_back('_');
        response->Attachments().push_back(MoveRV(data));
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

DEFINE_RPC_SERVICE_METHOD(TMyService, ModifyAttributes)
{
    const auto& attributes = request->Attributes();
    EXPECT_EQ(NYTree::TYsonString("stroka1"), attributes.GetYson("value1"));
    EXPECT_EQ(NYTree::TYsonString("stroka2"), attributes.GetYson("value2"));
    EXPECT_EQ(NYTree::TYsonString("stroka3"), attributes.GetYson("value3"));

    auto& new_attributes = response->Attributes();
    new_attributes.MergeFrom(attributes);
    new_attributes.Remove("value1");
    new_attributes.SetYson("value2", NYTree::TYsonString("another_stroka"));

    context->Reply();
}

TEST_F(TRpcTest, Attributes)
{
    TMyProxy proxy(CreateChannel("localhost:2000"));
    auto request = proxy.ModifyAttributes();

    request->Attributes().SetYson("value1", NYTree::TYsonString("stroka1"));
    request->Attributes().SetYson("value2", NYTree::TYsonString("stroka2"));
    request->Attributes().SetYson("value3", NYTree::TYsonString("stroka3"));

    auto response = request->Invoke().Get();
    const auto& attributes = response->Attributes();

    EXPECT_FALSE(attributes.Contains("value1"));
    EXPECT_EQ(NYTree::TYsonString("another_stroka"), attributes.GetYson("value2"));
    EXPECT_EQ(NYTree::TYsonString("stroka3"), attributes.GetYson("value3"));
} 


////////////////////////////////////////////////////////////////////////////////

// Now test different types of errors
TEST_F(TRpcTest, OK)
{
    TMyProxy proxy(CreateChannel("localhost:2000"));
    auto request = proxy.ReplyingCall();
    auto response = request->Invoke().Get();

    EXPECT_EQ(TError::OK, response->GetError().GetCode());
}

TEST_F(TRpcTest, TransportError)
{
    TMyProxy proxy(CreateChannel("localhost:9999"));
    auto request = proxy.EmptyCall();
    auto response = request->Invoke().Get();

    EXPECT_EQ(EErrorCode::TransportError, response->GetError().GetCode());
}

TEST_F(TRpcTest, NoService)
{
    TNonExistingServiceProxy proxy(CreateChannel("localhost:2000"));
    auto request = proxy.EmptyCall();
    auto response = request->Invoke().Get();

    EXPECT_EQ(EErrorCode::NoSuchService, response->GetError().GetCode());
}

TEST_F(TRpcTest, NoMethod)
{
    TMyProxy proxy(CreateChannel("localhost:2000"));
    auto request = proxy.NotRegisteredCall();
    auto response = request->Invoke().Get();

    EXPECT_EQ(EErrorCode::NoSuchVerb, response->GetError().GetCode());
}

TEST_F(TRpcTest, Timeout)
{
    TMyProxy proxy(CreateChannel("localhost:2000"));
    proxy.SetDefaultTimeout(TDuration::Seconds(1));

    auto request = proxy.EmptyCall();
    auto response = request->Invoke().Get();

    EXPECT_EQ(EErrorCode::Timeout, response->GetError().GetCode());
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
    EXPECT_EQ("Attachments",     StringFromSharedRef(attachments[0]));
    EXPECT_EQ("are",      StringFromSharedRef(attachments[1]));
    EXPECT_EQ("ok",  StringFromSharedRef(attachments[2]));

    auto& attributes = request->Attributes();
    EXPECT_EQ(NYTree::TYsonString("world"), attributes.GetYson("hello"));
    EXPECT_EQ(NYTree::TYsonString("42"), attributes.GetYson("value"));

    EXPECT_EQ("world", attributes.Get<Stroka>("hello"));
    EXPECT_EQ(42, attributes.Get<i64>("value"));

    EXPECT_FALSE(attributes.FindYson("another_value").HasValue());

    Event_->Signal();
}

TEST_F(TRpcTest, OneWaySend)
{
    TMyProxy proxy(CreateChannel("localhost:2000"));
    auto request = proxy.CheckAll();

    request->set_value(12345);
    request->set_ok(true);
    request->set_message(Stroka("hello, TMyService"));
    request->Attachments().push_back(SharedRefFromString("Attachments"));
    request->Attachments().push_back(SharedRefFromString("are"));
    request->Attachments().push_back(SharedRefFromString("ok"));

    request->Attributes().SetYson("hello", NYTree::TYsonString("world"));
    request->Attributes().SetYson("value", NYTree::TYsonString("42"));

    auto response = request->Invoke().Get();
    EXPECT_EQ(TError::OK, response->GetError().GetCode());

    EXPECT_TRUE(ReadyEvent.WaitT(TDuration::Seconds(4))); // assert no timeout
}

////////////////////////////////////////////////////////////////////////////////

// Different types of errors in one-way rpc
// TODO: think about refactoring
TEST_F(TRpcTest, OneWayOK)
{
    TMyProxy proxy(CreateChannel("localhost:2000"));
    auto request = proxy.OneWay();
    auto response = request->Invoke().Get();

    EXPECT_TRUE(response->IsOK());
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

    // In this case we receive OK instead of NoSuchVerb
    EXPECT_TRUE(response->IsOK());
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TRpcTest, LostConnection)
{
    TMyProxy proxy(CreateChannel("localhost:2000"));
    proxy.SetDefaultTimeout(TDuration::Seconds(10));

    auto request = proxy.LongReply();
    auto future = request->Invoke();

    Sleep(TDuration::Seconds(1));

    EXPECT_FALSE(future.IsSet());
    RpcServer->Stop();

    Sleep(TDuration::Seconds(1));

    // check that lost of connection is detected fast
    EXPECT_TRUE(future.IsSet());
    auto response = future.Get();
    EXPECT_FALSE(response->IsOK());
    EXPECT_EQ(EErrorCode::TransportError, response->GetError().GetCode());
}


} // namespace NYT
