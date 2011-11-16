#include "stdafx.h"

#include "../ytlib/bus/bus.h"
#include "../ytlib/bus/bus_server.h"
#include "../ytlib/bus/bus_client.h"
#include "../ytlib/misc/singleton.h"

#include <contrib/testing/framework.h>

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

IMessage::TPtr CreateMessage(int numParts)
{
    TBlob data(numParts);
    yvector <TRef> parts;
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

class TEmptyBusHandler
    : public IMessageHandler
{
public:
    virtual void OnMessage(
        IMessage::TPtr message,
        IBus::TPtr replyBus)
    { }
};

class TReplying42BusHandler
    : public IMessageHandler
{
public:
    virtual void OnMessage(
        IMessage::TPtr message,
        IBus::TPtr replyBus)
    {
        auto replyMessage = Serialize("42");
        replyBus->Send(replyMessage);
    }
};

class TChecking42BusHandler
    : public IMessageHandler
{
public:
    typedef TIntrusivePtr<TChecking42BusHandler> TPtr;

    Event Event_;

    virtual void OnMessage(
        IMessage::TPtr message,
        IBus::TPtr replyBus)
    {
        Stroka value = Deserialize(message);
        EXPECT_EQ("42", value);
        Event_.Signal();
    }
};

TEST(TBusTest, OK)
{
    auto listener = New<TBusServer>(2000, New<TEmptyBusHandler>());
    auto client = New<TBusClient>("localhost:2000");
    auto bus = client->CreateBus(~New<TEmptyBusHandler>());
    auto message = CreateMessage(1);
    auto result = bus->Send(message)->Get();
    EXPECT_EQ(IBus::ESendResult::OK, result);
    listener->Terminate();
}

TEST(TBusTest, Failed)
{
    auto client = New<TBusClient>("localhost:2000");
    auto bus = client->CreateBus(~New<TEmptyBusHandler>());
    auto message = CreateMessage(1);
    auto result = bus->Send(message)->Get();
    EXPECT_EQ(IBus::ESendResult::Failed, result);
}

TEST(TBusTest, Reply)
{
    auto listener = New<TBusServer>(2000, ~New<TReplying42BusHandler>());
    auto client = New<TBusClient>("localhost:2000");
    auto handler = New<TChecking42BusHandler>();
    auto bus = client->CreateBus(~handler);
    auto message = CreateMessage(1);
    bus->Send(message)->Get();

    if(!handler->Event_.Wait(2000)) {
        EXPECT_IS_TRUE(false);
    }

    listener->Terminate();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
