#include "stdafx.h"
#include "framework.h"

#include <core/bus/bus.h>
#include <core/bus/config.h>
#include <core/bus/server.h>
#include <core/bus/client.h>
#include <core/bus/tcp_server.h>
#include <core/bus/tcp_client.h>

#include <core/misc/singleton.h>

namespace NYT {
namespace NBus {
namespace {

////////////////////////////////////////////////////////////////////////////////

TSharedRefArray CreateMessage(int numParts)
{
    auto data = TSharedRef::Allocate(numParts);

    std::vector<TSharedRef> parts;
    for (int i = 0; i < numParts; ++i) {
        parts.push_back(data.Slice(TRef(data.Begin() + i, 1)));
    }

    return TSharedRefArray(std::move(parts));
}

TSharedRefArray Serialize(Stroka str)
{
    return TSharedRefArray(TSharedRef::FromString(str));
}

Stroka Deserialize(TSharedRefArray message)
{
    YASSERT(message.Size() == 1);
    const auto& part = message[0];
    return Stroka(part.Begin(), part.Size());
}

IBusServerPtr StartBusServer(IMessageHandlerPtr handler)
{
    auto config = New<TTcpBusServerConfig>(2000);
    auto server = CreateTcpBusServer(config);
    server->Start(handler);
    return server;
}

////////////////////////////////////////////////////////////////////////////////

class TEmptyBusHandler
    : public IMessageHandler
{
public:
    virtual void OnMessage(
        TSharedRefArray message,
        IBusPtr replyBus)
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
        TSharedRefArray message,
        IBusPtr replyBus)
    {
        EXPECT_EQ(NumPartsExpecting, message.Size());
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
    TChecking42BusHandler(int numRepliesWaiting)
        : NumRepliesWaiting(numRepliesWaiting)
    { }

    Event Event_;

    virtual void OnMessage(
        TSharedRefArray message,
        IBusPtr replyBus)
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
    auto server = StartBusServer(New<TReplying42BusHandler>(numParts));
    auto client = CreateTcpBusClient(New<TTcpBusClientConfig>("localhost:2000"));
    auto handler = New<TChecking42BusHandler>(numRequests);
    auto bus = client->CreateBus(handler);
    auto message = CreateMessage(numParts);

    TAsyncError result;
    for (int i = 0; i < numRequests; ++i) {
        result = bus->Send(message);
    }

    result.Get();
    if (!handler->Event_.WaitT(TDuration::Seconds(2))) {
        EXPECT_TRUE(false); // timeout occurred
    }

    server->Stop();
}

////////////////////////////////////////////////////////////////////////////////

TEST(TBusTest, OK)
{
    auto server = StartBusServer(New<TEmptyBusHandler>());
    auto client = CreateTcpBusClient(New<TTcpBusClientConfig>("localhost:2000"));
    auto bus = client->CreateBus(New<TEmptyBusHandler>());
    auto message = CreateMessage(1);
    auto result = bus->Send(message).Get();
    EXPECT_TRUE(result.IsOK());
    server->Stop();
}

TEST(TBusTest, Failed)
{
    auto client = CreateTcpBusClient(New<TTcpBusClientConfig>("localhost:2000"));
    auto bus = client->CreateBus(New<TEmptyBusHandler>());
    auto message = CreateMessage(1);
    auto result = bus->Send(message).Get();
    EXPECT_FALSE(result.IsOK());
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

} // namespace
} // namespace NBus
} // namespace NYT
