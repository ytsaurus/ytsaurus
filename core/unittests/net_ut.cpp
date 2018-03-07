#include <yt/core/test_framework/framework.h>

#include <yt/core/net/connection.h>
#include <yt/core/net/listener.h>
#include <yt/core/net/dialer.h>
#include <yt/core/net/config.h>

#include <yt/core/concurrency/poller.h>
#include <yt/core/concurrency/thread_pool_poller.h>

namespace NYT {
namespace {

using namespace NYT::NNet;
using namespace NYT::NConcurrency;


////////////////////////////////////////////////////////////////////////////////

class TNetTest
    : public ::testing::Test
{
public:
    TNetTest()
    {
        Poller = CreateThreadPoolPoller(2, "nettest");
    }

    ~TNetTest()
    {
        Poller->Shutdown();
    }

protected:
    IPollerPtr Poller;
    const TDialerConfigPtr Config = New<TDialerConfig>();
    NLogging::TLogger Logger = NLogging::TLogger("Net");

    IDialerPtr CreateDialer()
    {
        return NNet::CreateDialer(
            Config,
            Poller,
            Logger);
    }
};

TEST_F(TNetTest, CreateConnectionPair)
{
    IConnectionPtr a, b;
    std::tie(a, b) = CreateConnectionPair(Poller);
}

TEST_F(TNetTest, TransferFourBytes)
{
    IConnectionPtr a, b;
    std::tie(a, b) = CreateConnectionPair(Poller);

    a->Write(TSharedRef::FromString("ping")).Get();

    TSharedMutableRef buffer = TSharedMutableRef::Allocate(10);
    ASSERT_EQ(4, b->Read(buffer).Get().ValueOrThrow());
    ASSERT_EQ(ToString(buffer.Slice(0, 4)), TString("ping"));
}

TEST_F(TNetTest, TransferFourBytesUsingWriteV)
{
    IConnectionPtr a, b;
    std::tie(a, b) = CreateConnectionPair(Poller);

    a->WriteV(TSharedRefArray(std::vector<TSharedRef>{
        TSharedRef::FromString("p"),
        TSharedRef::FromString("i"),
        TSharedRef::FromString("n"),
        TSharedRef::FromString("g")
    })).Get();

    TSharedMutableRef buffer = TSharedMutableRef::Allocate(10);
    ASSERT_EQ(4, b->Read(buffer).Get().ValueOrThrow());
    ASSERT_EQ(ToString(buffer.Slice(0, 4)), TString("ping"));
}

TEST_F(TNetTest, BigTransfer)
{
    const int N = 1024, K = 256 * 1024;

    IConnectionPtr a, b;    
    std::tie(a, b) = CreateConnectionPair(Poller);

    auto sender = BIND([=] {
        auto buffer = TSharedRef::FromString(TString(K, 'f'));
        for (int i = 0; i < N; ++i) {
            WaitFor(a->Write(buffer)).ThrowOnError();
        }

        WaitFor(a->CloseWrite()).ThrowOnError();
    })
        .AsyncVia(Poller->GetInvoker())
        .Run();

    auto receiver = BIND([=] {
        auto buffer = TSharedMutableRef::Allocate(3 * K);
        size_t received = 0;
        while (true) {
            int res = WaitFor(b->Read(buffer)).ValueOrThrow();
            if (res == 0) break;
            received += res;
        }

        EXPECT_EQ(N * K, received);
    })
        .AsyncVia(Poller->GetInvoker())
        .Run();

    sender.Get().ThrowOnError();
    receiver.Get().ThrowOnError();
}

TEST_F(TNetTest, BidirectionalTransfer)
{
    const int N = 1024, K = 256 * 1024;

    IConnectionPtr a, b;    
    std::tie(a, b) = CreateConnectionPair(Poller);

    auto startSender = [&] (IConnectionPtr conn) {
        return BIND([=] {
            auto buffer = TSharedRef::FromString(TString(K, 'f'));
            for (int i = 0; i < N; ++i) {
                WaitFor(conn->Write(buffer)).ThrowOnError();
            }

            WaitFor(conn->CloseWrite()).ThrowOnError();
        })
            .AsyncVia(Poller->GetInvoker())
            .Run();
    };
    
    auto startReceiver = [&] (IConnectionPtr conn) {
        return BIND([=] {
            auto buffer = TSharedMutableRef::Allocate(K * 4);
            size_t received = 0;
            while (true) {
                int res = WaitFor(conn->Read(buffer)).ValueOrThrow();
                if (res == 0) break;
                received += res;
            }

            EXPECT_EQ(N * K, received);
        })
            .AsyncVia(Poller->GetInvoker())
            .Run();
    };

    std::vector<TFuture<void>> futures = {
        startSender(a),
        startReceiver(a),
        startSender(b),
        startReceiver(b)
    };

    Combine(futures).Get().ThrowOnError();
}

TEST_F(TNetTest, StressConcurrentClose)
{
    for (int i = 0; i < 10; i++) {
        IConnectionPtr a, b;
        std::tie(a, b) = CreateConnectionPair(Poller);

        auto runSender = [&] (IConnectionPtr conn) {
            return BIND([=] {
                auto buffer = TSharedRef::FromString(TString(16 * 1024, 'f'));
                while (true) {
                    WaitFor(conn->Write(buffer)).ThrowOnError();
                }
            })
                .AsyncVia(Poller->GetInvoker())
                .Run();
        };

        auto runReceiver = [&] (IConnectionPtr conn) {
            return BIND([=] {
                auto buffer = TSharedMutableRef::Allocate(16 * 1024);
                while (true) {
                    WaitFor(conn->Read(buffer)).ThrowOnError();
                }
            })
                .AsyncVia(Poller->GetInvoker())
                .Run();
        };

        runSender(a);
        runReceiver(a);
        runSender(b);
        runReceiver(b);

        Sleep(TDuration::MilliSeconds(10));
        a->Close().Get();
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST_F(TNetTest, Bind)
{
    BIND([&] {
        auto address = TNetworkAddress::CreateIPv6Loopback(0);
        auto listener = CreateListener(address, Poller);
        EXPECT_THROW(CreateListener(listener->Address(), Poller), TErrorException);
    })
        .AsyncVia(Poller->GetInvoker())
        .Run()
        .Get()
        .ThrowOnError();
}

TEST_F(TNetTest, DialError)
{
    BIND([&] {
        auto address = TNetworkAddress::CreateIPv6Loopback(4000);
        auto dialer = CreateDialer();
        EXPECT_THROW(dialer->Dial(address).Get().ValueOrThrow(), TErrorException);
    })
        .AsyncVia(Poller->GetInvoker())
        .Run()
        .Get()
        .ThrowOnError();
}

TEST_F(TNetTest, DialSuccess)
{
    BIND([&] {
        auto address = TNetworkAddress::CreateIPv6Loopback(0);
        auto listener = CreateListener(address, Poller);
        auto dialer = CreateDialer();

        auto futureDial = dialer->Dial(listener->Address());
        auto futureAccept = listener->Accept();

        WaitFor(futureDial).ValueOrThrow();
        WaitFor(futureAccept).ValueOrThrow();
    })
        .AsyncVia(Poller->GetInvoker())
        .Run()
        .Get()
        .ThrowOnError();
}

TEST_F(TNetTest, ManyDials)
{
    BIND([&] {
        auto address = TNetworkAddress::CreateIPv6Loopback(0);
        auto listener = CreateListener(address, Poller);
        auto dialer = CreateDialer();

        auto futureAccept1 = listener->Accept();
        auto futureAccept2 = listener->Accept();

        auto futureDial1 = dialer->Dial(listener->Address());
        auto futureDial2 = dialer->Dial(listener->Address());

        WaitFor(Combine(std::vector<TFuture<IConnectionPtr>>{futureDial1, futureDial2, futureAccept1, futureAccept2})).ValueOrThrow();
    })
        .AsyncVia(Poller->GetInvoker())
        .Run()
        .Get()
        .ThrowOnError();
}

TEST_F(TNetTest, AbandonDial)
{
    BIND([&] {
        auto address = TNetworkAddress::CreateIPv6Loopback(0);
        auto listener = CreateListener(address, Poller);
        auto dialer = CreateDialer();

        dialer->Dial(listener->Address());
    })
        .AsyncVia(Poller->GetInvoker())
        .Run()
        .Get()
        .ThrowOnError();        
}

TEST_F(TNetTest, AbandonAccept)
{
    BIND([&] {
        auto address = TNetworkAddress::CreateIPv6Loopback(0);
        auto listener = CreateListener(address, Poller);

        listener->Accept();
    })
        .AsyncVia(Poller->GetInvoker())
        .Run()
        .Get()
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
