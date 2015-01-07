#include "stdafx.h"
#include "framework.h"

#include <core/bus/bus.h>
#include <core/bus/config.h>
#include <core/bus/server.h>
#include <core/bus/client.h>
#include <core/bus/tcp_server.h>
#include <core/bus/tcp_client.h>

#include <core/concurrency/event_count.h>

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
        replyBus->Send(replyMessage, EDeliveryTrackingLevel::None);
    }
private:
    int NumPartsExpecting;
};

class TChecking42BusHandler
    : public IMessageHandler
{
public:
    explicit TChecking42BusHandler(int numRepliesWaiting)
        : NumRepliesWaiting(numRepliesWaiting)
    { }

    void WaitUntilDone()
    {
        Event_.Wait();
    }

private:
    std::atomic<int> NumRepliesWaiting;
    NConcurrency::TEvent Event_;


    virtual void OnMessage(TSharedRefArray message, IBusPtr /*replyBus*/)
    {
        auto value = Deserialize(message);
        EXPECT_EQ("42", value);

        if (--NumRepliesWaiting == 0) {
            Event_.NotifyAll();
        }
    }

};

void TestReplies(int numRequests, int numParts, EDeliveryTrackingLevel level = EDeliveryTrackingLevel::Full)
{
    auto server = StartBusServer(New<TReplying42BusHandler>(numParts));
    auto client = CreateTcpBusClient(New<TTcpBusClientConfig>("localhost:2000"));
    auto handler = New<TChecking42BusHandler>(numRequests);
    auto bus = client->CreateBus(handler);
    auto message = CreateMessage(numParts);

    std::vector<TFuture<void>> results;
    for (int i = 0; i < numRequests; ++i) {
        auto result = bus->Send(message, level);
        if (result) {
            results.push_back(result);
        }
    }

    for (const auto& result : results) {
        auto error = result.Get();
        EXPECT_TRUE(error.IsOK());
    }

    handler->WaitUntilDone();

    server->Stop();
}

////////////////////////////////////////////////////////////////////////////////

TEST(TBusTest, OK)
{
    auto server = StartBusServer(New<TEmptyBusHandler>());
    auto client = CreateTcpBusClient(New<TTcpBusClientConfig>("localhost:2000"));
    auto bus = client->CreateBus(New<TEmptyBusHandler>());
    auto message = CreateMessage(1);
    auto result = bus->Send(message, EDeliveryTrackingLevel::Full).Get();
    EXPECT_TRUE(result.IsOK());
    server->Stop();
}

TEST(TBusTest, Failed)
{
    auto client = CreateTcpBusClient(New<TTcpBusClientConfig>("localhost:2000"));
    auto bus = client->CreateBus(New<TEmptyBusHandler>());
    auto message = CreateMessage(1);
    auto result = bus->Send(message, EDeliveryTrackingLevel::Full).Get();
    EXPECT_FALSE(result.IsOK());
}

TEST(TBusTest, OneReplyNoTracking)
{
    TestReplies(1, 1, EDeliveryTrackingLevel::None);
}

TEST(TBusTest, OneReplyFullTracking)
{
    TestReplies(1, 1, EDeliveryTrackingLevel::Full);
}

TEST(TBusTest, OneReplyErrorOnlyTracking)
{
    TestReplies(1, 1, EDeliveryTrackingLevel::ErrorOnly);
}

TEST(TBusTest, ManyReplies)
{
    TestReplies(1000, 100);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NBus
} // namespace NYT
