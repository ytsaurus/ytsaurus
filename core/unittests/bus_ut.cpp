#include <yt/core/test_framework/framework.h>

#include <yt/core/bus/bus.h>
#include <yt/core/bus/client.h>
#include <yt/core/bus/server.h>

#include <yt/core/bus/tcp/config.h>
#include <yt/core/bus/tcp/client.h>
#include <yt/core/bus/tcp/server.h>

#include <yt/core/concurrency/event_count.h>

namespace NYT {
namespace NBus {
namespace {

////////////////////////////////////////////////////////////////////////////////

TSharedRefArray CreateMessage(int numParts)
{
    auto data = TSharedMutableRef::Allocate(numParts);

    std::vector<TSharedRef> parts;
    for (int i = 0; i < numParts; ++i) {
        parts.push_back(data.Slice(i, i + 1));
    }

    return TSharedRefArray(std::move(parts));
}

TSharedRefArray Serialize(TString str)
{
    return TSharedRefArray(TSharedRef::FromString(str));
}

TString Deserialize(TSharedRefArray message)
{
    Y_ASSERT(message.Size() == 1);
    const auto& part = message[0];
    return TString(part.Begin(), part.Size());
}

IBusServerPtr StartBusServer(IMessageHandlerPtr handler)
{
    auto config = TTcpBusServerConfig::CreateTcp(2000);
    auto server = CreateTcpBusServer(config);
    server->Start(handler);
    return server;
}

////////////////////////////////////////////////////////////////////////////////

class TEmptyBusHandler
    : public IMessageHandler
{
public:
    virtual void HandleMessage(
        TSharedRefArray message,
        IBusPtr replyBus) noexcept override
    {
        Y_UNUSED(message);
        Y_UNUSED(replyBus);
    }
};

class TReplying42BusHandler
    : public IMessageHandler
{
public:
    TReplying42BusHandler(int numParts)
        : NumPartsExpecting(numParts)
    { }

    virtual void HandleMessage(
        TSharedRefArray message,
        IBusPtr replyBus) noexcept override
    {
        EXPECT_EQ(NumPartsExpecting, message.Size());
        auto replyMessage = Serialize("42");
        replyBus->Send(replyMessage, NBus::TSendOptions(EDeliveryTrackingLevel::None));
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


    virtual void HandleMessage(
        TSharedRefArray message,
        IBusPtr /*replyBus*/) noexcept override
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
    auto client = CreateTcpBusClient(TTcpBusClientConfig::CreateTcp("localhost:2000"));
    auto handler = New<TChecking42BusHandler>(numRequests);
    auto bus = client->CreateBus(handler);
    auto message = CreateMessage(numParts);

    std::vector<TFuture<void>> results;
    for (int i = 0; i < numRequests; ++i) {
        auto result = bus->Send(message, NBus::TSendOptions(level));
        if (result) {
            results.push_back(result);
        }
    }

    for (const auto& result : results) {
        auto error = result.Get();
        EXPECT_TRUE(error.IsOK());
    }

    handler->WaitUntilDone();

    server->Stop()
        .Get()
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

TEST(TBusTest, ConfigDefaultConstructor)
{
    auto config = New<TTcpBusClientConfig>();
}

TEST(TBusTest, CreateTcpBusClientConfig)
{
    auto config = TTcpBusClientConfig::CreateTcp("localhost:2000");
    EXPECT_EQ("localhost:2000", config->Address.Get());
    EXPECT_FALSE(config->UnixDomainName);
}

TEST(TBusTest, CreateUnixDomainBusClientConfig)
{
    auto config = TTcpBusClientConfig::CreateUnixDomain("unix-socket");
    EXPECT_EQ("unix-socket", config->UnixDomainName.Get());
}

TEST(TBusTest, OK)
{
    auto server = StartBusServer(New<TEmptyBusHandler>());
    auto client = CreateTcpBusClient(TTcpBusClientConfig::CreateTcp("localhost:2000"));
    auto bus = client->CreateBus(New<TEmptyBusHandler>());
    auto message = CreateMessage(1);
    auto result = bus->Send(message, NBus::TSendOptions(EDeliveryTrackingLevel::Full))
        .Get();
    EXPECT_TRUE(result.IsOK());
    server->Stop()
        .Get()
        .ThrowOnError();
}

TEST(TBusTest, Failed)
{
    auto client = CreateTcpBusClient(TTcpBusClientConfig::CreateTcp("localhost:2000"));
    auto bus = client->CreateBus(New<TEmptyBusHandler>());
    auto message = CreateMessage(1);
    auto result = bus->Send(message, NBus::TSendOptions(EDeliveryTrackingLevel::Full)).Get();
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
