#include "stdafx.h"

#include <ytlib/bus/bus.h>
#include <ytlib/bus/nl_server.h>
#include <ytlib/bus/nl_client.h>
#include <ytlib/misc/singleton.h>

#include <contrib/testing/framework.h>

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

IMessage::TPtr CreateMessage(int numParts)
{
    TBlob data(numParts);
    yvector <TRef> parts;
    parts.reserve(numParts);
    for (int i = 0; i < numParts; ++i) {
        parts.push_back(TRef(data.begin() + i, 1));
    }
    return CreateMessageFromParts(MoveRV(data), parts);
}

IMessage::TPtr Serialize(Stroka str)
{
    TBlob data(str.begin(), str.vend());
    return CreateMessageFromPart(TSharedRef(MoveRV(data)));
}

Stroka Deserialize(IMessage::TPtr message)
{
    const yvector<TSharedRef>& parts = message->GetParts();
    YASSERT(parts.ysize() == 1);
    const TSharedRef& part = parts[0];
    return Stroka(part.Begin(), part.Size());
}

IBusServer::TPtr StartBusServer(IMessageHandler* handler)
{
    auto config = New<TNLBusServerConfig>();
    config->Port = 2000;
    auto server = CreateNLBusServer(~config);
    server->Start(handler);
    return server;
}

////////////////////////////////////////////////////////////////////////////////

class TEmptyBusHandler
    : public IMessageHandler
{
public:
    virtual void OnMessage(
        IMessage::TPtr message,
        IBus::TPtr replyBus)
    {
        UNUSED(message);
        UNUSED(replyBus);
    }
};

class TReplying42BusHandler
    : public IMessageHandler
{
public:
    TReplying42BusHandler(int numParts)
        : NumPartsExpecting(numParts)
    { }

    virtual void OnMessage(
        IMessage::TPtr message,
        IBus::TPtr replyBus)
    {
        EXPECT_EQ(NumPartsExpecting, message->GetParts().ysize());
        auto replyMessage = Serialize("42");
        replyBus->Send(replyMessage);
    }
private:
    int NumPartsExpecting;
};

class TChecking42BusHandler
    : public IMessageHandler
{
public:
    typedef TIntrusivePtr<TChecking42BusHandler> TPtr;

    TChecking42BusHandler(int numRepliesWaiting)
        : NumRepliesWaiting(numRepliesWaiting)
    { }

    Event Event_;

    virtual void OnMessage(
        IMessage::TPtr message,
        IBus::TPtr replyBus)
    {
        UNUSED(replyBus);

        Stroka value = Deserialize(message);
        EXPECT_EQ("42", value);

        --NumRepliesWaiting;
        if (NumRepliesWaiting == 0) {
            Event_.Signal();
        }
    }

private:
    int NumRepliesWaiting;
};

void TestReplies(int numRequests, int numParts)
{
    auto server = StartBusServer(~New<TReplying42BusHandler>(numParts));
    auto client = CreateNLBusClient(~New<TNLBusClientConfig>("localhost:2000"));
    auto handler = New<TChecking42BusHandler>(numRequests);
    auto bus = client->CreateBus(~handler);
    auto message = CreateMessage(numParts);

    IBus::TSendResult result;
    for (int i = 0; i < numRequests; ++i) {
        result = bus->Send(message);
    }

    result.Get();
    if(!handler->Event_.WaitT(TDuration::Seconds(2))) {
        EXPECT_IS_TRUE(false); // timeout occured
    }

    server->Stop();
}

////////////////////////////////////////////////////////////////////////////////

TEST(TBusTest, OK)
{
    auto server = StartBusServer(~New<TEmptyBusHandler>());
    auto client = CreateNLBusClient(~New<TNLBusClientConfig>("localhost:2000"));
    auto bus = client->CreateBus(~New<TEmptyBusHandler>());
    auto message = CreateMessage(1);
    auto result = bus->Send(message).Get();
    EXPECT_EQ(ESendResult::OK, result);
    server->Stop();
}

TEST(TBusTest, Failed)
{
    auto client = CreateNLBusClient(~New<TNLBusClientConfig>("localhost:2000"));
    auto bus = client->CreateBus(~New<TEmptyBusHandler>());
    auto message = CreateMessage(1);
    auto result = bus->Send(message).Get();
    EXPECT_EQ(ESendResult::Failed, result);
}

TEST(TBusTest, OneReply)
{
    TestReplies(1, 1);
}

TEST(TBusTest, ManyReplies)
{
    TestReplies(1000, 100);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
