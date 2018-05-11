#include <yt/core/test_framework/framework.h>

#include "test_key.h"

#include <yt/core/net/address.h>
#include <yt/core/net/dialer.h>
#include <yt/core/net/connection.h>
#include <yt/core/net/listener.h>
#include <yt/core/net/config.h>
#include <yt/core/net/private.h>

#include <yt/core/concurrency/poller.h>
#include <yt/core/concurrency/thread_pool_poller.h>

#include <yt/core/rpc/grpc/dispatcher.h>

#include <yt/core/crypto/tls.h>

namespace NYT {
namespace {

using namespace NCrypto;
using namespace NRpc;
using namespace NNet;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

class TTlsTest
    : public ::testing::Test
{
public:
    TTlsTest()
    {
        // Piggybacking on openssl initialization in grpc.
        GrpcLock = NRpc::NGrpc::TDispatcher::Get()->CreateLibraryLock();
        Context = New<TSslContext>();

        Context->AddCertificate(TestCertificate);
        Context->AddPrivateKey(TestCertificate);

        Poller = CreateThreadPoolPoller(2, "TlsTest");
    }

    ~TTlsTest()
    {
        Poller->Shutdown();
    }

    NRpc::NGrpc::TGrpcLibraryLockPtr GrpcLock;
    TSslContextPtr Context;
    IPollerPtr Poller;
};

TEST_F(TTlsTest, CreateContext)
{
    // Not that trivial as it seems!
}

TEST_F(TTlsTest, CreateListener)
{
    auto localhost = TNetworkAddress::CreateIPv6Loopback(0);
    auto listener = Context->CreateListener(localhost, Poller);
}

TEST_F(TTlsTest, CreateDialer)
{
    auto config = New<TDialerConfig>();
    config->SetDefaults();
    auto dialer = Context->CreateDialer(config, Poller, NetLogger);
}

TEST_F(TTlsTest, SimplePingPong)
{
    auto localhost = TNetworkAddress::CreateIPv6Loopback(0);
    auto listener = Context->CreateListener(localhost, Poller);

    auto config = New<TDialerConfig>();
    config->SetDefaults();
    auto dialer = Context->CreateDialer(config, Poller, NetLogger);

    auto asyncFirstSide = dialer->Dial(listener->Address());
    auto asyncSecondSide = listener->Accept();

    auto firstSide = asyncFirstSide.Get().ValueOrThrow();
    auto secondSide = asyncSecondSide.Get().ValueOrThrow();

    auto buffer = TSharedRef::FromString(TString("ping"));
    auto outputBuffer = TSharedMutableRef::Allocate(4);

    auto result = firstSide->Write(buffer).Get();
    ASSERT_EQ(secondSide->Read(outputBuffer).Get().ValueOrThrow(), 4);
    result.ThrowOnError();
    ASSERT_EQ(ToString(outputBuffer), ToString(buffer));

    secondSide->Write(buffer).Get().ThrowOnError();
    ASSERT_EQ(firstSide->Read(outputBuffer).Get().ValueOrThrow(), 4);
    ASSERT_EQ(ToString(outputBuffer), ToString(buffer));

    WaitFor(firstSide->Close())
        .ThrowOnError();
    ASSERT_EQ(secondSide->Read(outputBuffer).Get().ValueOrThrow(), 0);
}

TEST(TTlsTestWithoutFixture, LoadCertificateChain)
{
    NRpc::NGrpc::TDispatcher::Get()->CreateLibraryLock();
    auto context = New<TSslContext>();
    context->AddCertificateChain(TestCertificateChain);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
