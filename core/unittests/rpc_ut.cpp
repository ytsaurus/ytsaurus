#include <yt/core/test_framework/framework.h>

#include <yt/core/bus/bus.h>
#include <yt/core/bus/config.h>
#include <yt/core/bus/server.h>
#include <yt/core/bus/tcp_client.h>
#include <yt/core/bus/tcp_server.h>

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/delayed_executor.h>

#include <yt/core/misc/error.h>

#include <yt/core/rpc/bus_channel.h>
#include <yt/core/rpc/bus_server.h>
#include <yt/core/rpc/client.h>
#include <yt/core/rpc/server.h>
#include <yt/core/rpc/service_detail.h>

#include <yt/core/unittests/proto/rpc_ut.pb.h>

#include <yt/core/rpc/grpc/config.h>
#include <yt/core/rpc/grpc/channel.h>
#include <yt/core/rpc/grpc/server.h>

namespace NYT {
namespace NRpc {
namespace {

using namespace NBus;
using namespace NConcurrency;

static const TString DefaultAddress = "localhost:2000";

////////////////////////////////////////////////////////////////////////////////

class TMyProxy
    : public TProxyBase
{
public:
    DEFINE_RPC_PROXY(TMyProxy, MyService,
        .SetProtocolVersion(1));

    DEFINE_RPC_PROXY_METHOD(NMyRpc, SomeCall);
    DEFINE_RPC_PROXY_METHOD(NMyRpc, PassCall);
    DEFINE_RPC_PROXY_METHOD(NMyRpc, RegularAttachments);
    DEFINE_RPC_PROXY_METHOD(NMyRpc, NullAndEmptyAttachments);
    DEFINE_RPC_PROXY_METHOD(NMyRpc, DoNothing);
    DEFINE_RPC_PROXY_METHOD(NMyRpc, CustomMessageError);
    DEFINE_RPC_PROXY_METHOD(NMyRpc, NotRegistered);
    DEFINE_RPC_PROXY_METHOD(NMyRpc, SlowCall);
    DEFINE_RPC_PROXY_METHOD(NMyRpc, SlowCanceledCall);
    DEFINE_RPC_PROXY_METHOD(NMyRpc, NoReply);
};

class TNonExistingServiceProxy
    : public TProxyBase
{
public:
    DEFINE_RPC_PROXY(TNonExistingServiceProxy, NonExistingService);

    DEFINE_RPC_PROXY_METHOD(NMyRpc, DoNothing);
};

class TMyIncorrectProtocolVersionProxy
    : public TProxyBase
{
public:
    DEFINE_RPC_PROXY(TMyIncorrectProtocolVersionProxy, MyService,
        .SetProtocolVersion(2));

    DEFINE_RPC_PROXY_METHOD(NMyRpc, SomeCall);
};

////////////////////////////////////////////////////////////////////////////////

TString StringFromSharedRef(const TSharedRef& sharedRef)
{
    return TString(sharedRef.Begin(), sharedRef.Begin() + sharedRef.Size());
}

TSharedRef SharedRefFromString(const TString& s)
{
    return TSharedRef::FromString(s);
}

////////////////////////////////////////////////////////////////////////////////

class TMyService
    : public TServiceBase
{
public:
    explicit TMyService(IInvokerPtr invoker)
        : TServiceBase(
            invoker,
            TMyProxy::GetDescriptor(),
            NLogging::TLogger("Main"))
    {
        RegisterMethod(RPC_SERVICE_METHOD_DESC(SomeCall));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(PassCall));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(RegularAttachments));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(NullAndEmptyAttachments));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(DoNothing));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(CustomMessageError));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(SlowCall)
            .SetCancelable(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(SlowCanceledCall)
            .SetCancelable(true));
        RegisterMethod(RPC_SERVICE_METHOD_DESC(NoReply));
        // NB: NotRegisteredCall is not registered intentionally
    }

    DECLARE_RPC_SERVICE_METHOD(NMyRpc, SomeCall)
    {
        context->SetRequestInfo();
        int a = request->a();
        response->set_b(a + 100);
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NMyRpc, PassCall)
    {
        context->SetRequestInfo();
        response->set_user(context->GetUser());
        response->set_mutation_id(ToString(context->GetMutationId()));
        response->set_retry(context->IsRetry());
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NMyRpc, RegularAttachments)
    {
        for (const auto& attachment : request->Attachments()) {
            auto data = TBlob(TDefaultBlobTag());
            data.Append(attachment);
            data.Append("_", 1);
            response->Attachments().push_back(TSharedRef::FromBlob(std::move(data)));
        }
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NMyRpc, NullAndEmptyAttachments)
    {
        const auto& attachments = request->Attachments();
        EXPECT_EQ(2, attachments.size());
        EXPECT_FALSE(attachments[0]);
        EXPECT_TRUE(attachments[1]);
        EXPECT_TRUE(attachments[1].Empty());
        response->Attachments() = attachments;
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NMyRpc, DoNothing)
    {
        context->SetRequestInfo();
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NMyRpc, CustomMessageError)
    {
        context->SetRequestInfo();
        context->Reply(TError(NYT::EErrorCode(42), "Some Error"));
    }

    DECLARE_RPC_SERVICE_METHOD(NMyRpc, SlowCall)
    {
        context->SetRequestInfo();
        Sleep(TDuration::Seconds(1.0));
        context->Reply();
    }

    DECLARE_RPC_SERVICE_METHOD(NMyRpc, SlowCanceledCall)
    {
        try {
            context->SetRequestInfo();
            WaitFor(TDelayedExecutor::MakeDelayed(TDuration::Seconds(2)));
            context->Reply();
        } catch (const TFiberCanceledException&) {
            SlowCallCanceled_ = true;
            throw;
        }
    }

    DECLARE_RPC_SERVICE_METHOD(NMyRpc, NoReply)
    { }


    bool GetSlowCallCanceled() const
    {
        return SlowCallCanceled_;
    }

private:
    bool SlowCallCanceled_ = false;

};

////////////////////////////////////////////////////////////////////////////////

template <bool Secure>
class TRpcOverGrpc;
template <class TImpl>
class TRpcOverBus;

template <class TImpl>
class TTestBase
    : public ::testing::Test
{
public:
    virtual void SetUp() override final
    {
        Server_ = CreateServer();
        Queue_ = New<TActionQueue>();
        Service_ = New<TMyService>(Queue_->GetInvoker());
        Server_->RegisterService(Service_);
        Server_->Start();
    }

    virtual void TearDown() override final
    {
        Server_->Stop().Get().ThrowOnError();
        Server_.Reset();
    }

    IServerPtr CreateServer()
    {
        return TImpl::CreateServer();
    }

    IChannelPtr CreateChannel(const TString& address = DefaultAddress)
    {
        return TImpl::CreateChannel(address);
    }

    static bool CheckCancelCode(TErrorCode code)
    {
        if (code == NYT::EErrorCode::Canceled) {
            return true;
        }
        if (code == NYT::NRpc::EErrorCode::TransportError && TImpl::AllowTransportErrors) {
            return true;
        }
        return false;
    }

    static bool CheckTimeoutCode(TErrorCode code)
    {
        if (code == NYT::EErrorCode::Timeout) {
            return true;
        }
        if (code == NYT::NRpc::EErrorCode::TransportError && TImpl::AllowTransportErrors) {
            return true;
        }
        return false;
    }

protected:
    TActionQueuePtr Queue_;
    TIntrusivePtr<TMyService> Service_;
    IServerPtr Server_;
};

////////////////////////////////////////////////////////////////////////////////

template <class TImpl>
class TRpcOverBus
{
public:
    static constexpr bool AllowTransportErrors = false;

    static IServerPtr CreateServer()
    {
        auto busServer = MakeBusServer();
        return CreateBusServer(busServer);
    }

    static IChannelPtr CreateChannel(const TString& address)
    {
        return TImpl::CreateChannel(address);
    }

    static IBusServerPtr MakeBusServer()
    {
        return TImpl::MakeBusServer();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TRpcOverBusImpl
{
public:
    static IChannelPtr CreateChannel(const TString& address)
    {
        auto client = CreateTcpBusClient(TTcpBusClientConfig::CreateTcp(address));
        return CreateBusChannel(client);
    }

    static IBusServerPtr MakeBusServer()
    {
        auto busConfig = TTcpBusServerConfig::CreateTcp(2000);
        return CreateTcpBusServer(busConfig);
    }
};

////////////////////////////////////////////////////////////////////////////////

static const TString RootCert(
    "-----BEGIN CERTIFICATE-----\n"
    "MIICJTCCAY4CCQD+dLIhVg9x1DANBgkqhkiG9w0BAQUFADBXMQswCQYDVQQGEwJS\n"
    "VTETMBEGA1UECBMKU29tZS1TdGF0ZTEPMA0GA1UEChMGWWFuZGV4MQ4wDAYDVQQL\n"
    "EwVJbmZyYTESMBAGA1UEAxMJbG9jYWxob3N0MB4XDTE4MDIwMTExNDA1MVoXDTE4\n"
    "MDMwMzExNDA1MVowVzELMAkGA1UEBhMCUlUxEzARBgNVBAgTClNvbWUtU3RhdGUx\n"
    "DzANBgNVBAoTBllhbmRleDEOMAwGA1UECxMFSW5mcmExEjAQBgNVBAMTCWxvY2Fs\n"
    "aG9zdDCBnzANBgkqhkiG9w0BAQEFAAOBjQAwgYkCgYEA5I6RnIl1z0c4FIdzCbLX\n"
    "cHjJMHLvkrg+TZYmPofXZ2hILr43Ekpg1KyCi5QARZDIIxtDcj/ESmQiWnAg5iVH\n"
    "QVhp3jX0JtE9Of/L+mo3bJrP0f1pFNwAoOMpQYpo/s1vFzRRmQIKkiCaA2ooZqTa\n"
    "kOyYQynLLhDuQt3V/b28isUCAwEAATANBgkqhkiG9w0BAQUFAAOBgQBG8RVCLGC8\n"
    "QZihIJkSq0x30Kee5BKQYAlGISC5mMLwS0dxaMNe05E4tZ8ZeKTjzt/mk1xR0c4w\n"
    "e2WATo9aSr2AdFyrYXnGjFhZU7tpQQdDr63aL7UOkjjnsx9gYOuq+DT53iFGE2hA\n"
    "p+Lg8byzWYrUA86gnj7XuPaIyDmMwCwYfg==\n"
    "-----END CERTIFICATE-----\n");

static const TString ClientKey(
    "-----BEGIN RSA PRIVATE KEY-----\n"
    "MIICWwIBAAKBgQCscvdKfMfBrpLV1ZQaCW5VCW5pOKSrTNoJlXhuCSo+s4t7eW/D\n"
    "88CaSt0e8BgBLjOGZwUXiFzbAaxozzJA4/7AN3n/u0mBP94+QRb8Ky9mi2e0fV65\n"
    "AMk49NK1GCygSaEMzBgRyGM/YqSF3m2pYmPaV+w6yXLbx8kHCTVIlRE59wIDAQAB\n"
    "AoGAeHr90FmrSeLIjYLhLOun3Ntw+yxqx48yyf1WnTHkBNh1u7dU4Yc76dAh+opt\n"
    "iu2Xa/Avu4g2r3/Uz4jqqDFh+gzTLQZ2IgVO6lyXmPpREQdEM4NY7egOBS0oG5wy\n"
    "1P7qYQNDI1Ik6RZwUQu/LcX1aujW6taY9FvH/48Xtx4Fm7kCQQDgLJUYUoUeNmOX\n"
    "XxlhxS6JMZMKbfjPpINnV5Ox/o9QARSmplkDpOTD5QI9Oi8pjrTULjEsXuzTrPdd\n"
    "ffrU2+VFAkEAxO56+z3sEkODLRN0Lxmh2h3IWgS1px8qx8iQIRlvHS03xPxRAdeG\n"
    "GxS0AlnUWv+wpGxevDz+p6TF5rTNphrgCwJAVRqXv5QUVVPwmxbPW/6vnAXl/J+j\n"
    "RbD+8cydlYU+gvDmFh4wzOFct8HJB9V+8hfrLmIa6O1gmzue7S9WeljAwQJAYt0q\n"
    "hlj229Bi2U1L5Z1joMGU5IQ6wbSm4HcyzDsdijM6LT1SWa9eVtgU6p04O5rjjhgN\n"
    "k0i3u2fAJmscVshlMwJAC0suZ6vGzGcxsszRMKTUhAwWs9MtQhLQCNAJCarG3WQu\n"
    "en5U7JosPjsR39SH5uyVB6WlZMk8EBVWciLsbm1rfA==\n"
    "-----END RSA PRIVATE KEY-----\n");

static const TString ClientCert(
    "-----BEGIN CERTIFICATE-----\n"
    "MIICJjCCAY8CCQCfTTq2MFp+wDANBgkqhkiG9w0BAQUFADBXMQswCQYDVQQGEwJS\n"
    "VTETMBEGA1UECBMKU29tZS1TdGF0ZTEPMA0GA1UEChMGWWFuZGV4MQ4wDAYDVQQL\n"
    "EwVJbmZyYTESMBAGA1UEAxMJbG9jYWxob3N0MB4XDTE4MDIwMTEyMDkxNVoXDTE4\n"
    "MDMwMzEyMDkxNVowWDELMAkGA1UEBhMCUlUxEzARBgNVBAgTClNvbWUtU3RhdGUx\n"
    "DzANBgNVBAcTBmNsaWVudDEPMA0GA1UEChMGWWFuZGV4MRIwEAYDVQQDEwlsb2Nh\n"
    "bGhvc3QwgZ8wDQYJKoZIhvcNAQEBBQADgY0AMIGJAoGBAKxy90p8x8GuktXVlBoJ\n"
    "blUJbmk4pKtM2gmVeG4JKj6zi3t5b8PzwJpK3R7wGAEuM4ZnBReIXNsBrGjPMkDj\n"
    "/sA3ef+7SYE/3j5BFvwrL2aLZ7R9XrkAyTj00rUYLKBJoQzMGBHIYz9ipIXebali\n"
    "Y9pX7DrJctvHyQcJNUiVETn3AgMBAAEwDQYJKoZIhvcNAQEFBQADgYEAUDizQpQ4\n"
    "pa+AFBsJagcmm8rIsM6WaBnkF0AOyvMlGob66bqVlSRrC553a6tJQWQVlQ4RZ2hC\n"
    "1uHSyN+OynER83/hu41NXWum+1oDaHv4PaBthoeUbpG5d8efZOCTKVnM2AC810dA\n"
    "XLl9IQguAc3fzFqMhUiAb9kl2pjdAvSMw2w=\n"
    "-----END CERTIFICATE-----\n");

static const TString ServerKey(
    "-----BEGIN RSA PRIVATE KEY-----\n"
    "MIICXQIBAAKBgQDU60RX3B8S1o9TycaIR6fo5hYTgnVA/ZfY0cJ5eQZ44I+pqWHt\n"
    "rE6s7gS5VuDIf70ziChLRjUA77Y30I9P7RAcAUtzCsq7X7EApXA41FSgA9kPqDqI\n"
    "iWxlr9x6HmBzdsSU8kKt+kYUbRC3F9sAbB1D0NgDm899+Z1aLIIo/4/TUQIDAQAB\n"
    "AoGAFk9MivQ6oUuwGvRyhAcWFfY96+9of9XSmlpoSTggMHw/MWTZKQE8ASyzfAQZ\n"
    "a4jO915V8oU5uo5jhneo/JP3cva66hTtTLAtOc6A6jK16hfhQMTEBCI/XIHv3zEr\n"
    "1vcaHAGnEDZZX13WjnUODB8Rt+rKJb2t7/0wbM1Xt6RSPt0CQQD5gkI1NbMgofsy\n"
    "9rlfHDLoujm9Rhyb8wTUiS5Or3xku2FoW0ptmnaYEL2sqnngpdHI5m9ZmXuOKDE/\n"
    "5mtgT8JDAkEA2nVRf014Lfzd3b0ia+7+Dqf2r50Kp6sBvre5E50gkKKaehg6+Ugg\n"
    "o4hVJDCrCY16b/OlozAFD9iMSI7hOQiM2wJAWnwBtLhHwOLdbWsKaNKaJ8o5XEnL\n"
    "4EZujwE82O5NJ17JAYZx5HOq5JTVpIOidXTNMpVW9mBx7WjoC2ttr1zdbwJBAMHs\n"
    "vSDV42Znf4h0ehb4O/1Eqx6vuKKokk78BsZbiGn8fkb+NXPOzHJ+9p2+ukYrmlHB\n"
    "JvubCBNN9xH+C/62EVsCQQCzp3vpwEF+DtU5oLh6C5Hd5jb8iwzhqrNXpYHK71ev\n"
    "a0vOkz9+Zo+lZJC2QRkzMNpBLX9vPjznxWH6iXC+BVMj\n"
    "-----END RSA PRIVATE KEY-----\n");

static const TString ServerCert(
    "-----BEGIN CERTIFICATE-----\n"
    "MIICJjCCAY8CCQCfTTq2MFp+vzANBgkqhkiG9w0BAQUFADBXMQswCQYDVQQGEwJS\n"
    "VTETMBEGA1UECBMKU29tZS1TdGF0ZTEPMA0GA1UEChMGWWFuZGV4MQ4wDAYDVQQL\n"
    "EwVJbmZyYTESMBAGA1UEAxMJbG9jYWxob3N0MB4XDTE4MDIwMTEyMDg0NloXDTE4\n"
    "MDMwMzEyMDg0NlowWDELMAkGA1UEBhMCUlUxEzARBgNVBAgTClNvbWUtU3RhdGUx\n"
    "DzANBgNVBAcTBnNlcnZlcjEPMA0GA1UEChMGWWFuZGV4MRIwEAYDVQQDEwlsb2Nh\n"
    "bGhvc3QwgZ8wDQYJKoZIhvcNAQEBBQADgY0AMIGJAoGBANTrRFfcHxLWj1PJxohH\n"
    "p+jmFhOCdUD9l9jRwnl5Bnjgj6mpYe2sTqzuBLlW4Mh/vTOIKEtGNQDvtjfQj0/t\n"
    "EBwBS3MKyrtfsQClcDjUVKAD2Q+oOoiJbGWv3HoeYHN2xJTyQq36RhRtELcX2wBs\n"
    "HUPQ2AObz335nVosgij/j9NRAgMBAAEwDQYJKoZIhvcNAQEFBQADgYEAmHQDStiH\n"
    "uw7AcI2jH8yK2mPWEPyIO5e64+t+N7bItk3TiGCMacr+/vKEb9DsiK8fPuLAExea\n"
    "AHo49XAAEWwX+I9JMM2y/BGvcD3qlLqcZ2Y8gy8bgi/vyaNNsijGrmn5rYejj+zu\n"
    "DWRD3xlaQ+4oYcCGFMKyctKlQTUxYnisWsw=\n"
    "-----END CERTIFICATE-----\n");

////////////////////////////////////////////////////////////////////////////////

template <bool Secure>
class TRpcOverGrpc
{
public:
    static constexpr bool AllowTransportErrors = true;

    static IChannelPtr CreateChannel(const TString& address)
    {
        auto channelConfig = New<NGrpc::TChannelConfig>();
        if (Secure) {
            channelConfig->Credentials = New<NGrpc::TChannelCredentialsConfig>();
            channelConfig->Credentials->PemRootCerts = New<NGrpc::TPemBlobConfig>();
            channelConfig->Credentials->PemRootCerts->Value = RootCert;
            channelConfig->Credentials->PemKeyCertPair = New<NGrpc::TSslPemKeyCertPairConfig>();
            channelConfig->Credentials->PemKeyCertPair->PrivateKey = New<NGrpc::TPemBlobConfig>();
            channelConfig->Credentials->PemKeyCertPair->PrivateKey->Value = ClientKey;
            channelConfig->Credentials->PemKeyCertPair->CertChain = New<NGrpc::TPemBlobConfig>();
            channelConfig->Credentials->PemKeyCertPair->CertChain->Value = ClientCert;
        }
        channelConfig->Address = address;
        return NGrpc::CreateGrpcChannel(channelConfig);
    }

    static IServerPtr CreateServer()
    {
        auto serverAddressConfig = New<NGrpc::TServerAddressConfig>();
        if (Secure) {
            serverAddressConfig->Credentials = New<NGrpc::TServerCredentialsConfig>();
            serverAddressConfig->Credentials->PemRootCerts = New<NGrpc::TPemBlobConfig>();
            serverAddressConfig->Credentials->PemRootCerts->Value = RootCert;
            serverAddressConfig->Credentials->PemKeyCertPairs.push_back(New<NGrpc::TSslPemKeyCertPairConfig>());
            serverAddressConfig->Credentials->PemKeyCertPairs[0]->PrivateKey = New<NGrpc::TPemBlobConfig>();
            serverAddressConfig->Credentials->PemKeyCertPairs[0]->PrivateKey->Value = ServerKey;
            serverAddressConfig->Credentials->PemKeyCertPairs[0]->CertChain = New<NGrpc::TPemBlobConfig>();
            serverAddressConfig->Credentials->PemKeyCertPairs[0]->CertChain->Value = ServerCert;
        }
        serverAddressConfig->Address = DefaultAddress;
        auto serverConfig = New<NGrpc::TServerConfig>();
        serverConfig->Addresses.push_back(serverAddressConfig);
        return NGrpc::CreateServer(serverConfig);
    }
};

////////////////////////////////////////////////////////////////////////////////

// TTcpBusClient creates abstract unix sockets, supported only on Linux.
class TRpcOverUnixDomainImpl
{
public:
    static constexpr bool AllowTransportErrors = false;

    static IBusServerPtr MakeBusServer()
    {
        auto busConfig = TTcpBusServerConfig::CreateUnixDomain("unix_domain");
        return CreateTcpBusServer(busConfig);
    }

    static IChannelPtr CreateChannel(const TString& address)
    {
        auto clientConfig = TTcpBusClientConfig::CreateUnixDomain(
            address == DefaultAddress ? "unix_domain" : address);
        auto client = CreateTcpBusClient(clientConfig);
        return CreateBusChannel(client);
    }
};

////////////////////////////////////////////////////////////////////////////////

using AllTransport = ::testing::Types<
#ifdef _linux_
    TRpcOverBus<TRpcOverUnixDomainImpl>,
#endif
//    TRpcOverBus<TRpcOverBusImpl>,
    TRpcOverGrpc<false>,
    TRpcOverGrpc<true>
>;
using WithoutGrpc = ::testing::Types<
#ifdef _linux_
    TRpcOverBus<TRpcOverUnixDomainImpl>,
#endif
    TRpcOverBus<TRpcOverBusImpl>
>;

template <class TImpl>
using TRpcTest = TTestBase<TImpl>;
template <class TImpl>
using TNotGrpcTest = TTestBase<TImpl>;
TYPED_TEST_CASE(TRpcTest, AllTransport);
TYPED_TEST_CASE(TNotGrpcTest, WithoutGrpc);

////////////////////////////////////////////////////////////////////////////////

TYPED_TEST(TRpcTest, Send)
{
    TMyProxy proxy(this->CreateChannel());
    auto req = proxy.SomeCall();
    req->set_a(42);
    auto rspOrError = req->Invoke().Get();
    EXPECT_TRUE(rspOrError.IsOK()) << ToString(rspOrError);
    const auto& rsp = rspOrError.Value();
    EXPECT_EQ(142, rsp->b());
}

TYPED_TEST(TNotGrpcTest, SendSimple)
{
    TMyProxy proxy(this->CreateChannel());
    auto req = proxy.PassCall();
    auto mutation_id = TGuid::Create();
    req->SetUser("test");
    req->SetMutationId(mutation_id);
    req->SetRetry(true);
    auto rspOrError = req->Invoke().Get();
    EXPECT_TRUE(rspOrError.IsOK()) << ToString(rspOrError);
    const auto& rsp = rspOrError.Value();
    EXPECT_EQ("test", rsp->user());
    EXPECT_EQ(ToString(mutation_id), rsp->mutation_id());
    EXPECT_EQ(true, rsp->retry());
}

TYPED_TEST(TRpcTest, ManyAsyncRequests)
{
    const int RequestCount = 1000;

    std::vector<TFuture<void>> asyncResults;

    TMyProxy proxy(this->CreateChannel());

    for (int i = 0; i < RequestCount; ++i) {
        auto request = proxy.SomeCall();
        request->set_a(i);
        auto asyncResult = request->Invoke().Apply(BIND([=] (TMyProxy::TRspSomeCallPtr rsp) {
            EXPECT_EQ(i + 100, rsp->b());
        }));
        asyncResults.push_back(asyncResult);
    }

    EXPECT_TRUE(Combine(asyncResults).Get().IsOK());
}

TYPED_TEST(TNotGrpcTest, RegularAttachments)
{
    TMyProxy proxy(this->CreateChannel());
    auto req = proxy.RegularAttachments();

    req->Attachments().push_back(SharedRefFromString("Hello"));
    req->Attachments().push_back(SharedRefFromString("from"));
    req->Attachments().push_back(SharedRefFromString("TMyProxy"));

    auto rspOrError = req->Invoke().Get();
    EXPECT_TRUE(rspOrError.IsOK());
    const auto& rsp = rspOrError.Value();

    const auto& attachments = rsp->Attachments();
    EXPECT_EQ(3, attachments.size());
    EXPECT_EQ("Hello_",     StringFromSharedRef(attachments[0]));
    EXPECT_EQ("from_",      StringFromSharedRef(attachments[1]));
    EXPECT_EQ("TMyProxy_",  StringFromSharedRef(attachments[2]));
}

TYPED_TEST(TNotGrpcTest, NullAndEmptyAttachments)
{
    TMyProxy proxy(this->CreateChannel());
    auto req = proxy.NullAndEmptyAttachments();

    req->Attachments().push_back(TSharedRef());
    req->Attachments().push_back(EmptySharedRef);

    auto rspOrError = req->Invoke().Get();
    EXPECT_TRUE(rspOrError.IsOK());
    auto rsp = rspOrError.Value();

    const auto& attachments = rsp->Attachments();
    EXPECT_EQ(2, attachments.size());
    EXPECT_FALSE(attachments[0]);
    EXPECT_TRUE(attachments[1]);
    EXPECT_TRUE(attachments[1].Empty());
}

// Now test different types of errors

TYPED_TEST(TRpcTest, OK)
{
    TMyProxy proxy(this->CreateChannel());
    auto req = proxy.DoNothing();
    auto rspOrError = req->Invoke().Get();
    EXPECT_TRUE(rspOrError.IsOK());
}

TYPED_TEST(TRpcTest, NoAck)
{
    TMyProxy proxy(this->CreateChannel());
    auto req = proxy.DoNothing();
    req->SetRequestAck(false);
    auto rspOrError = req->Invoke().Get();
    EXPECT_TRUE(rspOrError.IsOK());
}

TYPED_TEST(TRpcTest, TransportError)
{
    TMyProxy proxy(this->CreateChannel("localhost:9999"));
    auto req = proxy.DoNothing();
    auto rspOrError = req->Invoke().Get();
    EXPECT_EQ(NRpc::EErrorCode::TransportError, rspOrError.GetCode());
}

TYPED_TEST(TRpcTest, NoService)
{
    TNonExistingServiceProxy proxy(this->CreateChannel());
    auto req = proxy.DoNothing();
    auto rspOrError = req->Invoke().Get();
    EXPECT_EQ(NRpc::EErrorCode::NoSuchService, rspOrError.GetCode());
}

TYPED_TEST(TRpcTest, NoMethod)
{
    TMyProxy proxy(this->CreateChannel());
    auto req = proxy.NotRegistered();
    auto rspOrError = req->Invoke().Get();
    EXPECT_EQ(NRpc::EErrorCode::NoSuchMethod, rspOrError.GetCode());
}

TYPED_TEST(TRpcTest, ClientTimeout)
{
    TMyProxy proxy(this->CreateChannel());
    proxy.SetDefaultTimeout(TDuration::Seconds(0.5));
    auto req = proxy.SlowCall();
    auto rspOrError = req->Invoke().Get();
    EXPECT_TRUE(this->CheckTimeoutCode(rspOrError.GetCode()));
}

TYPED_TEST(TRpcTest, ServerTimeout)
{
    TMyProxy proxy(this->CreateChannel());
    proxy.SetDefaultTimeout(TDuration::Seconds(0.5));
    auto req = proxy.SlowCanceledCall();
    auto rspOrError = req->Invoke().Get();
    EXPECT_TRUE(this->CheckTimeoutCode(rspOrError.GetCode()));
    Sleep(TDuration::Seconds(1));
    EXPECT_TRUE(this->Service_->GetSlowCallCanceled());
}

TYPED_TEST(TRpcTest, ClientCancel)
{
    TMyProxy proxy(this->CreateChannel());
    auto req = proxy.SlowCanceledCall();
    auto asyncRspOrError = req->Invoke();
    Sleep(TDuration::Seconds(0.5));
    EXPECT_FALSE(asyncRspOrError.IsSet());
    asyncRspOrError.Cancel();
    Sleep(TDuration::Seconds(0.1));
    EXPECT_TRUE(asyncRspOrError.IsSet());
    auto rspOrError = asyncRspOrError.Get();
    EXPECT_TRUE(this->CheckCancelCode(rspOrError.GetCode()));
    Sleep(TDuration::Seconds(1));
    EXPECT_TRUE(this->Service_->GetSlowCallCanceled());
}

TYPED_TEST(TRpcTest, SlowCall)
{
    TMyProxy proxy(this->CreateChannel());
    proxy.SetDefaultTimeout(TDuration::Seconds(2.0));
    auto req = proxy.SlowCall();
    auto rspOrError = req->Invoke().Get();
    EXPECT_TRUE(rspOrError.IsOK());
}

TYPED_TEST(TRpcTest, NoReply)
{
    TMyProxy proxy(this->CreateChannel());
    auto req = proxy.NoReply();
    auto rspOrError = req->Invoke().Get();
    EXPECT_EQ(NRpc::EErrorCode::Unavailable, rspOrError.GetCode());
}

TYPED_TEST(TRpcTest, CustomErrorMessage)
{
    TMyProxy proxy(this->CreateChannel());
    auto req = proxy.CustomMessageError();
    auto rspOrError = req->Invoke().Get();
    EXPECT_EQ(NYT::EErrorCode(42), rspOrError.GetCode());
    EXPECT_EQ("Some Error", rspOrError.GetMessage());
}

TYPED_TEST(TRpcTest, ConnectionLost)
{
    TMyProxy proxy(this->CreateChannel());

    auto req = proxy.SlowCanceledCall();
    auto asyncRspOrError = req->Invoke();

    Sleep(TDuration::Seconds(0.5));

    EXPECT_FALSE(asyncRspOrError.IsSet());
    this->Server_->Stop(false);

    Sleep(TDuration::Seconds(0.5));

    EXPECT_TRUE(asyncRspOrError.IsSet());
    auto rspOrError = asyncRspOrError.Get();
    EXPECT_EQ(NRpc::EErrorCode::TransportError, rspOrError.GetCode());
    EXPECT_TRUE(this->Service_->GetSlowCallCanceled());
}

TYPED_TEST(TNotGrpcTest, ProtocolVersionMismatch)
{
    TMyIncorrectProtocolVersionProxy proxy(this->CreateChannel());
    auto req = proxy.SomeCall();
    req->set_a(42);
    auto rspOrError = req->Invoke().Get();
    EXPECT_EQ(NRpc::EErrorCode::ProtocolError, rspOrError.GetCode());
}

TYPED_TEST(TRpcTest, StopWithoutActiveRequests)
{
    auto stopResult = this->Service_->Stop();
    EXPECT_TRUE(stopResult.IsSet());
}

TYPED_TEST(TRpcTest, StopWithActiveRequests)
{
    TMyProxy proxy(this->CreateChannel());
    auto req = proxy.SlowCall();
    auto reqResult = req->Invoke();
    Sleep(TDuration::Seconds(0.5));
    auto stopResult = this->Service_->Stop();
    EXPECT_FALSE(stopResult.IsSet());
    EXPECT_TRUE(reqResult.Get().IsOK());
    Sleep(TDuration::Seconds(0.5));
    EXPECT_TRUE(stopResult.IsSet());
}

TYPED_TEST(TRpcTest, NoMoreRequestsAfterStop)
{
    auto stopResult = this->Service_->Stop();
    EXPECT_TRUE(stopResult.IsSet());
    TMyProxy proxy(this->CreateChannel());
    auto req = proxy.SlowCall();
    auto reqResult = req->Invoke();
    EXPECT_FALSE(reqResult.Get().IsOK());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NRpc
} // namespace NYT
