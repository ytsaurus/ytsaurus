#include <yt/core/test_framework/framework.h>

#include <yt/core/bus/bus.h>
#include <yt/core/bus/config.h>
#include <yt/core/bus/server.h>
#include <yt/core/bus/tcp_client.h>
#include <yt/core/bus/tcp_server.h>

#include <yt/core/crypto/config.h>

#include <yt/core/concurrency/action_queue.h>
#include <yt/core/concurrency/delayed_executor.h>

#include <yt/core/misc/error.h>

#include <yt/core/bus/public.h>

#include <yt/core/rpc/bus/channel.h>
#include <yt/core/rpc/bus/server.h>

#include <yt/core/rpc/client.h>
#include <yt/core/rpc/server.h>
#include <yt/core/rpc/service_detail.h>

#include <yt/core/unittests/proto/rpc_ut.pb.h>

#include <yt/core/rpc/grpc/config.h>
#include <yt/core/rpc/grpc/channel.h>
#include <yt/core/rpc/grpc/server.h>
#include <yt/core/rpc/grpc/proto/grpc.pb.h>

namespace NYT {
namespace NRpc {
namespace {

using namespace NYT::NBus;
using namespace NYT::NRpc::NBus;
using namespace NConcurrency;
using namespace NCrypto;

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
    TMyService(IInvokerPtr invoker, bool secure)
        : TServiceBase(
            invoker,
            TMyProxy::GetDescriptor(),
            NLogging::TLogger("Main"))
        , Secure_(secure)
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
    const bool Secure_;
    bool SlowCallCanceled_ = false;


    virtual void BeforeInvoke(IServiceContext* context) override
    {
        TServiceBase::BeforeInvoke(context);
        if (Secure_) {
            const auto& ext = context->GetRequestHeader().GetExtension(NGrpc::NProto::TSslCredentialsExt::ssl_credentials_ext);
            EXPECT_EQ("localhost", ext.peer_identity());
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

template <bool Secure>
class TRpcOverGrpcImpl;
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
        bool secure = TImpl::Secure;
        Service_ = New<TMyService>(Queue_->GetInvoker(), secure);
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
    static constexpr bool Secure = false;

    static IServerPtr CreateServer()
    {
        auto busServer = MakeBusServer();
        return NRpc::NBus::CreateBusServer(busServer);
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
        return NRpc::NBus::CreateBusChannel(client);
    }

    static IBusServerPtr MakeBusServer()
    {
        auto busConfig = TTcpBusServerConfig::CreateTcp(2000);
        return CreateTcpBusServer(busConfig);
    }
};

////////////////////////////////////////////////////////////////////////////////

/*
 * openssl genrsa -out root_key.pem 2048
 * openssl req -x509 -new -nodes -key root_key.pem -sha256 -days 10000 -out root_cert.pem
 * openssl genrsa -out server_key.pem 2048
 * openssl genrsa -out client_key.pem 2048
 * openssl req -new -key server_key.pem -out server.csr
 * openssl req -new -key client_key.pem -out client.csr
 * openssl x509 -in server.csr -req -days 10000 -out server_cert.pem -CA root_cert.pem -CAkey root_key.pem -CAcreateserial
 * openssl x509 -in client.csr -req -days 10000 -out client_cert.pem -CA root_cert.pem -CAkey root_key.pem -CAserial root_cert.srl
 */
static const TString RootCert(
    "-----BEGIN CERTIFICATE-----\n"
    "MIID9DCCAtygAwIBAgIJAJLU9fgmNTujMA0GCSqGSIb3DQEBCwUAMFkxCzAJBgNV\n"
    "BAYTAlJVMRMwEQYDVQQIEwpTb21lLVN0YXRlMSEwHwYDVQQKExhJbnRlcm5ldCBX\n"
    "aWRnaXRzIFB0eSBMdGQxEjAQBgNVBAMTCWxvY2FsaG9zdDAeFw0xODAzMDQxMzUx\n"
    "MjdaFw00NTA3MjAxMzUxMjdaMFkxCzAJBgNVBAYTAlJVMRMwEQYDVQQIEwpTb21l\n"
    "LVN0YXRlMSEwHwYDVQQKExhJbnRlcm5ldCBXaWRnaXRzIFB0eSBMdGQxEjAQBgNV\n"
    "BAMTCWxvY2FsaG9zdDCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAMEq\n"
    "JYLsNKAnO6uENyLjRww3pITvtEEg8uDi1W+87hZE8XNQ1crhJZDXcMaoWaVLOcQT\n"
    "6x2z5DAnn5/0CUXLrJgrwbfrZ82VwihQIpovPX91bA7Bd5PdlBI5ojtUrY9Fb6xB\n"
    "eAmfsp7z7rKDBheLe7KoNMth4OSHWp5GeHLzp336AB7TA6EQSTd3T7oDrRjdqZTr\n"
    "X35vF0n6+iOMSe5CJYuNX9fd5GkO6mwGV5BzEoUWwqTocfkLa2BfE+pvfsuWleNc\n"
    "sU8vMoAdlkKUrnHbbQ7xuwR+3XhKpRCU+wmzM6Tvm6dnYJhhTck8yxGNCuAfgKu+\n"
    "7k9Ur4rdPXYkSTUMbbcCAwEAAaOBvjCBuzAdBgNVHQ4EFgQUkZyJrYSMi34fw8wk\n"
    "sLSQyzg8a/swgYsGA1UdIwSBgzCBgIAUkZyJrYSMi34fw8wksLSQyzg8a/uhXaRb\n"
    "MFkxCzAJBgNVBAYTAlJVMRMwEQYDVQQIEwpTb21lLVN0YXRlMSEwHwYDVQQKExhJ\n"
    "bnRlcm5ldCBXaWRnaXRzIFB0eSBMdGQxEjAQBgNVBAMTCWxvY2FsaG9zdIIJAJLU\n"
    "9fgmNTujMAwGA1UdEwQFMAMBAf8wDQYJKoZIhvcNAQELBQADggEBADpYkiJ4XsdV\n"
    "w3JzDZCJX644cCzx3/l1N/ItVllVTbFU9MSrleifhBj21t4xUfHT2uhbQ21N6enA\n"
    "Qx24wcLo9IRL61XEkLrTRPo1ZRrF8rwAYLxFgHgWimcocG+c/8++he7tXrjyYzS1\n"
    "JyMKBgQcsrWn+3pCxSLHGuoH4buX3cMqrEepqdThIOTI12YW7xmD7vSguusroRFj\n"
    "OH5RO4hhHIn/tR2G/lHS1u+YG5NyX94v8kN+SfAchZmeb54miANYBGzOFqYRgKs4\n"
    "LfyFanmeXFJaj1M+37Lsm0TlxP6I7fa0Kag6FvlxpYvhblRJzsRHZE5Xe+KZzanV\n"
    "I2TYYgHjI3I=\n"
    "-----END CERTIFICATE-----\n");

static const TString ClientKey(
    "-----BEGIN RSA PRIVATE KEY-----\n"
    "MIIEpAIBAAKCAQEArZpqucOdMlwZyyTWq+Sz3EGXpAX/4nMpH7s/05d9O4tm0MsK\n"
    "QUhUXRzt3VzOfMOb4cXAVwovHxiQ7NZIFBdmeyCHlT0HVkaqC76Tgi53scUMVKtE\n"
    "lXJB5soc8PbFDjT21MOGzL3+Tqy47ecdZhCiaXYeD3exHFd+VDJXvC3O/GDc3/Fo\n"
    "Gwh6iUxkAAa11duUoFfCs3p+XFN216V9jqfkEmf/KU2utMjzSmAvwaGh0WCSTnb2\n"
    "lcByiPJWK6w8yx/CeY9Ks+sjI2hWw43jxUCcSa2pPimGLWgu9TYRiG4jWZln9FLW\n"
    "hskfF/Ra0Xp0ptxrnuih0DTQ+ZxTscNlg27nuwIDAQABAoIBAFRhD6rG52sI1Qim\n"
    "GSlnefx+bSQuPldkvgJMUxOXOClu8kRdy9g7PbYcT4keiMaflO7B3WDw9EJbAGX9\n"
    "KP+K+Ca0gvIIvb4zjocy1COcTlU7f2jP7f/tjxaL+lEswE7Nc4OqnaR6XFcFIMWR\n"
    "Zfqr7yTvYmEGPjGWXTKzXW17nnWQGYiK5IhLyzR+MQowCIVDK8ByJl18FRZOROtn\n"
    "O+Bbm/MCsLsevAJPlKefY8kG/aG6VbrJO0sTvYe/j2QpeSfPOYtSlcDnTdx2Y5za\n"
    "HFo+2mHvurhetl7Ba2gyTGu3XoMHtBXQ8jifyv8s3h+iz94twpsWp6D5CkPUw9oB\n"
    "OOx/ttECgYEA4z3L7mSJQANvodlWfJUKAIWgdO54Vq6yZ9gELaCCoUXZLwHwjw+v\n"
    "3k/WNbCv7lIL/DVzVh/RfFaG4Qe/c/Bu2tgwBv4fcAepvegUznwcY4Q1FB3sPMpm\n"
    "fYcYPOy7jwEO7fvG8rjlZCXo6JuyJJsfyC+z+qWuPSpNgY+lj5MPe3UCgYEAw5LX\n"
    "VZYnoghqMQGAi2CldxQ5Iz4RtZpIMgJH7yfu7jt1b3VGiBChwwbyfYrvPJTBpfP9\n"
    "U05iffC8P8NVVL8KtjNRJLmQftLdssoqCncqdALnBGJ/jRNpxEFOcodReodkmUT/\n"
    "vwQOfQXx0JayeRbUmPKgkEfaqcJL2Y2O41iq4G8CgYEA14kYsb/4EphvvLLZfoca\n"
    "mo4kOGSsDYPbwfU5WVGiNXd73UNYuUjmxdUx13EEHecCaTEFeY3qc6XafvyLUlud\n"
    "ucNOIoPMq8UI8hB8E7HSd23BrpgHJ03O0oddrQPZjnUxhPbHqBdJtKjkdiSfXmso\n"
    "RQdCDZ4yWt+R7i6imUCicbUCgYEApg6iY/tQv5XhhKa/3Jg9JnS3ZyMmqknLjxq8\n"
    "tWX0y7cUqYSsVI+6qfvWHZ7AL3InUp9uszNVEZY8YO+cHo7vq3C7LzGYbPbiYxKg\n"
    "y64PD93/BYwUvVaEcaz5zOj019LqKfGaLThmjOVlQzURaRtnfE5W4ur/0TA2cwxt\n"
    "DMCWpmUCgYBKZhCPxpJmJVFdiM6c5CpsEBdoJ7NpPdDR8xlY7m2Nb38szAFRAFhk\n"
    "gMk6gXG+ObmPd4H6Up2lrpJgH3GDPIoiBZPOJefa0IXAmYCpqPH+HLG2lspuNyFL\n"
    "OY4A1p2EvY8/L6PmPXAURfsE8RTL0y4ww/7mPJTQXsteTawAPDdVKQ==\n"
    "-----END RSA PRIVATE KEY-----\n");

static const TString ClientCert(
    "-----BEGIN CERTIFICATE-----\n"
    "MIIDLjCCAhYCCQCZd28+0jJVLTANBgkqhkiG9w0BAQUFADBZMQswCQYDVQQGEwJS\n"
    "VTETMBEGA1UECBMKU29tZS1TdGF0ZTEhMB8GA1UEChMYSW50ZXJuZXQgV2lkZ2l0\n"
    "cyBQdHkgTHRkMRIwEAYDVQQDEwlsb2NhbGhvc3QwHhcNMTgwMzA0MTM1MjU2WhcN\n"
    "NDUwNzIwMTM1MjU2WjBZMQswCQYDVQQGEwJBVTETMBEGA1UECBMKU29tZS1TdGF0\n"
    "ZTEhMB8GA1UEChMYSW50ZXJuZXQgV2lkZ2l0cyBQdHkgTHRkMRIwEAYDVQQDEwls\n"
    "b2NhbGhvc3QwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQCtmmq5w50y\n"
    "XBnLJNar5LPcQZekBf/icykfuz/Tl307i2bQywpBSFRdHO3dXM58w5vhxcBXCi8f\n"
    "GJDs1kgUF2Z7IIeVPQdWRqoLvpOCLnexxQxUq0SVckHmyhzw9sUONPbUw4bMvf5O\n"
    "rLjt5x1mEKJpdh4Pd7EcV35UMle8Lc78YNzf8WgbCHqJTGQABrXV25SgV8Kzen5c\n"
    "U3bXpX2Op+QSZ/8pTa60yPNKYC/BoaHRYJJOdvaVwHKI8lYrrDzLH8J5j0qz6yMj\n"
    "aFbDjePFQJxJrak+KYYtaC71NhGIbiNZmWf0UtaGyR8X9FrRenSm3Gue6KHQNND5\n"
    "nFOxw2WDbue7AgMBAAEwDQYJKoZIhvcNAQEFBQADggEBAImeUspGIeL24U5sK2PR\n"
    "1BcWUBHtfUtXozaPK/q6WbEMObPxuNenNjnEYdp7b8JT2g91RqYd645wIPGaDAnc\n"
    "EFz3b2piUZIG8YfCCLdntqwrYLxdGuHt/47RoSCZ2WrTZA6j7wP5ldQZTfefq3VC\n"
    "ncz985cJ5AgEOZJmEdcleraoE1ZHb7O/kVxdxA6g93v9n3mm+kVYh3hth2646I8P\n"
    "Bn8Gucf3jySWsN5H74lnp4VaA0xyJh2hC/4e/RnYod7TkXaqKeeLc93suIXgHHKt\n"
    "jGvMhVuIWj3zzRi5e8Z1Ww5uHbiVyo4+GZMuV6w5ePgZpQ+5hUeD8PYf1AqDZet4\n"
    "3SA=\n"
    "-----END CERTIFICATE-----\n");

static const TString ServerKey(
    "-----BEGIN RSA PRIVATE KEY-----\n"
    "MIIEowIBAAKCAQEAzbAyEJFSmPNJ3pLNNSWQVF53Ltof1Wc4JIfvNazl41LjNyuO\n"
    "SQV7+6GVFMIybBBoeWQ58hVJ/d8KxFBf6XIV6uGH9WtN38hWrxR6UEGkHxpUSfvg\n"
    "TZ2FSsusus5sYDXjW+liQg5P9X/O69z/vmrIuyS8GckNq4/sA+Pw5GgCWDS05e72\n"
    "N8r6DG7UlzKm5ynCGI8pRh/EdmxHTP4G8bEKF25x4FRy3Mg7bAaif9owliC2+BLI\n"
    "IRNMtZs9BWp0U8GzEv2wY8xzkJEFD37xBiwHOWDj9KAmJpXQMM48PoXgvQsUo0ed\n"
    "/a+GHvumeb3tBtsqLALhLFQBEFykA9X4SF93jwIDAQABAoIBAQC488Bw6VuuMMWx\n"
    "n6tqKLbZRoBA3t5VFBWFs73DNA8bE8NALqgovQe5Qpg9LEoOpcprrVX1enMoFtEl\n"
    "qWg1D+Lpa5bHdY92tDxN/knltMCRPymfxR7ya7wZf394EnmdIZepY/h4kUoQ5LX5\n"
    "nKVSYc7RiLyjKwhhxm5hKSvJFkVVbaKvb9jFPEpYJHNWktl9Hh6XLs/DQLZwEVy0\n"
    "rR7KSV00XyNPtMlt6EBXLW7/ysYBiDdcGZ+lIp36fDkoC+kmfbNxsmsEO7x/63NW\n"
    "yCmhj4qz9hELbuOMNyoX0jzWMXdfEba/t/Gk7klB1/bQZ8VBn4Nd9PTEPHFLhNG2\n"
    "s/bQoH3RAoGBAOguUWbVar200VcPwnRjDg2Gw+N+xTR5ONvanIsJaf6xBW8Ymtsl\n"
    "J6GDJrJ391L0Zs2+fxLXDUebS8CF8CghL1KtqZxoTSwjBz8G4kn3DKlyZgNJZgyi\n"
    "GppY4ttaP1ys1LwO/xzPUJb9pqm84KDjE9JL1czv3Psk5PVzxV/PQlyzAoGBAOLK\n"
    "HElPA4rWw79AW9Kzr9mykZzyqalvAmobz8Q/nINnVGUcQu6UY5vDQ6KCOg2vbTl1\n"
    "shDrzEyD/mityBZWUgFiKp+KEYjD5CKE8XuryM3MHr9Dvb+zt2JMC6TVBrYJNG91\n"
    "OnMjGACRJ0i5SoB2kxiruTwyc2bzWyB6Dw9TfN+1AoGACHwg12w3MWWZPOBDj/NK\n"
    "wS3KnNa2KDvB2y77B425hOg9NZkll5qc/ycG1ADUVgC+fQhYJn0bbCF9vDRo2V6V\n"
    "FyVnjGK3Z0SEcEY1INTZbpvSpI4bH50Q8dELwU5kAGQEhjbaFdhxroLog012vApw\n"
    "YAALeSjO35Kyl1G6xcySNUcCgYBX+rAegFiPc+FcQEte4fZGLc/vYvQOltII9+ER\n"
    "8Nt23o8O6nfMtiQuOQHz+TEsPfHRaKc7iT4oMMxxL3l/sNz/TGXcnmNO+y91dL15\n"
    "jJrJu3XyHQVvaPirWXTq7Pk9hTSiSIf0Qpj9H1JuE/OjAlzuJTAm+itqtN2VK8TL\n"
    "3UeEQQKBgA61gNqGc8uCm58vg76qjMw6dlxBrpjWxYC5QsNh/OUITtWXqKiwTThE\n"
    "wkLMtumpDoioIp/cv8xyV7yvdNM0pxB5UtXBK/3P91lKbiyIfpertqMNxs5XzoeG\n"
    "CyxY8hFTw3FSk+UYdAAm5qYabGY1DiuvyD1yVAX9aWjAHdbP3H5O\n"
    "-----END RSA PRIVATE KEY-----\n");

static const TString ServerCert(
    "-----BEGIN CERTIFICATE-----\n"
    "MIIDLjCCAhYCCQCZd28+0jJVLDANBgkqhkiG9w0BAQUFADBZMQswCQYDVQQGEwJS\n"
    "VTETMBEGA1UECBMKU29tZS1TdGF0ZTEhMB8GA1UEChMYSW50ZXJuZXQgV2lkZ2l0\n"
    "cyBQdHkgTHRkMRIwEAYDVQQDEwlsb2NhbGhvc3QwHhcNMTgwMzA0MTM1MjUwWhcN\n"
    "NDUwNzIwMTM1MjUwWjBZMQswCQYDVQQGEwJBVTETMBEGA1UECBMKU29tZS1TdGF0\n"
    "ZTEhMB8GA1UEChMYSW50ZXJuZXQgV2lkZ2l0cyBQdHkgTHRkMRIwEAYDVQQDEwls\n"
    "b2NhbGhvc3QwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDNsDIQkVKY\n"
    "80neks01JZBUXncu2h/VZzgkh+81rOXjUuM3K45JBXv7oZUUwjJsEGh5ZDnyFUn9\n"
    "3wrEUF/pchXq4Yf1a03fyFavFHpQQaQfGlRJ++BNnYVKy6y6zmxgNeNb6WJCDk/1\n"
    "f87r3P++asi7JLwZyQ2rj+wD4/DkaAJYNLTl7vY3yvoMbtSXMqbnKcIYjylGH8R2\n"
    "bEdM/gbxsQoXbnHgVHLcyDtsBqJ/2jCWILb4EsghE0y1mz0FanRTwbMS/bBjzHOQ\n"
    "kQUPfvEGLAc5YOP0oCYmldAwzjw+heC9CxSjR539r4Ye+6Z5ve0G2yosAuEsVAEQ\n"
    "XKQD1fhIX3ePAgMBAAEwDQYJKoZIhvcNAQEFBQADggEBAJ1bjP+J+8MgSeHvpCES\n"
    "qo49l8JgpFV9h/1dUgz2fYhrVy7QCp8/3THoZcjErKYyzTdOlTzCy1OB4sRNLBiy\n"
    "ftGGTm1KHWal9CNMwAN00+ebhwdqKjNCWViI45o5OSfPWUvGAkwxUENrOqLoGBvR\n"
    "cVvvMIV5KeaZLTtvrPzfVCMq/B41Mu5ZslDZOTRmSpVlbxmFjUq3WM+wf1sLu2cw\n"
    "DDk8O2UQpxJeiowu9XBkQCEkvxU3/5bPBvY/+3sikj8IqaknakEXBKH1e/ZTN3/l\n"
    "F6/pV9FE34DC9mIlzIFQyMGKJd4cju6970Pv3blQabuNHJTd570JdMBYbUGJp/mI\n"
    "6sI=\n"
    "-----END CERTIFICATE-----\n");

////////////////////////////////////////////////////////////////////////////////

template <bool EnableSsl>
class TRpcOverGrpcImpl
{
public:
    static constexpr bool AllowTransportErrors = true;
    static constexpr bool Secure = EnableSsl;

    static IChannelPtr CreateChannel(const TString& address)
    {
        auto channelConfig = New<NGrpc::TChannelConfig>();
        if (EnableSsl) {
            channelConfig->Credentials = New<NGrpc::TChannelCredentialsConfig>();
            channelConfig->Credentials->PemRootCerts = New<TPemBlobConfig>();
            channelConfig->Credentials->PemRootCerts->Value = RootCert;
            channelConfig->Credentials->PemKeyCertPair = New<NGrpc::TSslPemKeyCertPairConfig>();
            channelConfig->Credentials->PemKeyCertPair->PrivateKey = New<TPemBlobConfig>();
            channelConfig->Credentials->PemKeyCertPair->PrivateKey->Value = ClientKey;
            channelConfig->Credentials->PemKeyCertPair->CertChain = New<TPemBlobConfig>();
            channelConfig->Credentials->PemKeyCertPair->CertChain->Value = ClientCert;
        }
        channelConfig->Address = address;
        return NGrpc::CreateGrpcChannel(channelConfig);
    }

    static IServerPtr CreateServer()
    {
        auto serverAddressConfig = New<NGrpc::TServerAddressConfig>();
        if (EnableSsl) {
            serverAddressConfig->Credentials = New<NGrpc::TServerCredentialsConfig>();
            serverAddressConfig->Credentials->PemRootCerts = New<TPemBlobConfig>();
            serverAddressConfig->Credentials->PemRootCerts->Value = RootCert;
            serverAddressConfig->Credentials->PemKeyCertPairs.push_back(New<NGrpc::TSslPemKeyCertPairConfig>());
            serverAddressConfig->Credentials->PemKeyCertPairs[0]->PrivateKey = New<TPemBlobConfig>();
            serverAddressConfig->Credentials->PemKeyCertPairs[0]->PrivateKey->Value = ServerKey;
            serverAddressConfig->Credentials->PemKeyCertPairs[0]->CertChain = New<TPemBlobConfig>();
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
        return NRpc::NBus::CreateBusChannel(client);
    }
};

////////////////////////////////////////////////////////////////////////////////

using AllTransport = ::testing::Types<
#ifdef _linux_
    TRpcOverBus<TRpcOverUnixDomainImpl>,
#endif
//    TRpcOverBus<TRpcOverBusImpl>,
    TRpcOverGrpcImpl<false>,
    TRpcOverGrpcImpl<true>
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

//template <>
//const bool TRpcOverBus<TRpcOverBusImpl>::Secure;
//template <>
//const bool TRpcOverBus<TRpcOverUnixDomainImpl>::Secure;
//template <>
//const bool TRpcOverBus<TRpcOverGrpcImpl<false>>::Secure;
//template <>
//const bool TRpcOverBus<TRpcOverGrpcImpl<true>>::Secure;
//
//////////////////////////////////////////////////////////////////////////////////
//
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

    Sleep(TDuration::Seconds(2));

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
