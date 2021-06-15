#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/bus/bus.h>
#include <yt/yt/core/bus/server.h>

#include <yt/yt/core/bus/tcp/config.h>
#include <yt/yt/core/bus/tcp/client.h>
#include <yt/yt/core/bus/tcp/server.h>

#include <yt/yt/core/crypto/config.h>

#include <yt/yt/core/concurrency/thread_pool.h>
#include <yt/yt/core/concurrency/delayed_executor.h>

#include <yt/yt/core/bus/public.h>

#include <yt/yt/core/misc/fs.h>

#include <yt/yt/core/rpc/bus/channel.h>
#include <yt/yt/core/rpc/bus/server.h>

#include <yt/yt/core/rpc/client.h>
#include <yt/yt/core/rpc/retrying_channel.h>
#include <yt/yt/core/rpc/caching_channel_factory.h>
#include <yt/yt/core/rpc/static_channel_factory.h>
#include <yt/yt/core/rpc/server.h>
#include <yt/yt/core/rpc/local_server.h>
#include <yt/yt/core/rpc/local_channel.h>
#include <yt/yt/core/rpc/service_detail.h>
#include <yt/yt/core/rpc/stream.h>

#include <yt/yt/core/rpc/unittests/lib/my_service.pb.h>
#include <yt/yt/core/rpc/unittests/lib/my_service.h>

#include <yt/yt/core/rpc/grpc/config.h>
#include <yt/yt/core/rpc/grpc/channel.h>
#include <yt/yt/core/rpc/grpc/server.h>
#include <yt/yt/core/rpc/grpc/proto/grpc.pb.h>

#include <yt/yt/core/misc/error.h>
#include <yt/yt/core/yson/string.h>
#include <yt/yt/core/ytree/fluent.h>

#include <library/cpp/testing/common/network.h>

#include <random>

namespace NYT::NRpc {
namespace {

using namespace NYT::NBus;
using namespace NYT::NRpc::NBus;
using namespace NYT::NYTree;
using namespace NYT::NYson;
using namespace NConcurrency;
using namespace NCrypto;
using namespace NYTAlloc;

////////////////////////////////////////////////////////////////////////////////

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
        Port_ = NTesting::GetFreePort();
        Address_ = Format("localhost:%v", Port_);

        Server_ = CreateServer(Port_);
        WorkerPool_ = New<TThreadPool>(4, "Worker");
        bool secure = TImpl::Secure;
        Service_ = CreateMyService(WorkerPool_->GetInvoker(), secure);
        Server_->RegisterService(Service_);
        Server_->Start();
    }

    virtual void TearDown() override final
    {
        Server_->Stop().Get().ThrowOnError();
        Server_.Reset();
    }

    IServerPtr CreateServer(ui16 port)
    {
        return TImpl::CreateServer(port);
    }

    IChannelPtr CreateChannel(const std::optional<TString>& address = std::nullopt)
    {
        if (address) {
            return TImpl::CreateChannel(*address, Address_);
        } else {
            return TImpl::CreateChannel(Address_, Address_);
        }
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
    NTesting::TPortHolder Port_;
    TString Address_;

    NConcurrency::TThreadPoolPtr WorkerPool_;
    IMyServicePtr Service_;
    IServerPtr Server_;
};

////////////////////////////////////////////////////////////////////////////////

template <class TImpl>
class TRpcOverBus
{
public:
    static constexpr bool AllowTransportErrors = false;
    static constexpr bool Secure = false;

    static IServerPtr CreateServer(ui16 port)
    {
        auto busServer = MakeBusServer(port);
        return NRpc::NBus::CreateBusServer(busServer);
    }

    static IChannelPtr CreateChannel(const TString& address, const TString& serverAddress)
    {
        return TImpl::CreateChannel(address, serverAddress);
    }

    static IBusServerPtr MakeBusServer(ui16 port)
    {
        return TImpl::MakeBusServer(port);
    }
};

////////////////////////////////////////////////////////////////////////////////

template <bool ForceTcp>
class TRpcOverBusImpl
{
public:
    static IChannelPtr CreateChannel(const TString& address, const TString& /* serverAddress */)
    {
        TString network = ForceTcp ? TString("non-local") : DefaultNetworkName;
        auto client = CreateTcpBusClient(TTcpBusClientConfig::CreateTcp(address, network));
        return NRpc::NBus::CreateBusChannel(client);
    }

    static IBusServerPtr MakeBusServer(ui16 port)
    {
        auto busConfig = TTcpBusServerConfig::CreateTcp(port);
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

    static IChannelPtr CreateChannel(const TString& address, const TString& /* serverAddress */)
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

    static IServerPtr CreateServer(ui16 port)
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

        auto address = Format("localhost:%v", port);

        serverAddressConfig->Address = address;
        auto serverConfig = New<NGrpc::TServerConfig>();
        serverConfig->Addresses.push_back(serverAddressConfig);
        return NGrpc::CreateServer(serverConfig);
    }
};

////////////////////////////////////////////////////////////////////////////////

// TRpcOverUnixDomainImpl creates unix domain sockets, supported only on Linux.
class TRpcOverUnixDomainImpl
{
public:
    static IBusServerPtr MakeBusServer(ui16 /*port*/)
    {
        auto busConfig = TTcpBusServerConfig::CreateUnixDomain("./socket");
        return CreateTcpBusServer(busConfig);
    }

    static IChannelPtr CreateChannel(const TString& address, const TString& serverAddress)
    {
        auto clientConfig = TTcpBusClientConfig::CreateUnixDomain(
            address == serverAddress ? "./socket" : address);
        auto client = CreateTcpBusClient(clientConfig);
        return NRpc::NBus::CreateBusChannel(client);
    }
};

////////////////////////////////////////////////////////////////////////////////

using TAllTransports = ::testing::Types<
#ifdef _linux_
    TRpcOverBus<TRpcOverUnixDomainImpl>,
    TRpcOverBus<TRpcOverBusImpl<true>>,
#endif
    TRpcOverBus<TRpcOverBusImpl<false>>,
    TRpcOverGrpcImpl<false>,
    TRpcOverGrpcImpl<true>
>;

using TWithoutGrpc = ::testing::Types<
#ifdef _linux_
    TRpcOverBus<TRpcOverUnixDomainImpl>,
    TRpcOverBus<TRpcOverBusImpl<true>>,
#endif
    TRpcOverBus<TRpcOverBusImpl<false>>
>;

template <class TImpl>
using TRpcTest = TTestBase<TImpl>;
template <class TImpl>
using TNotGrpcTest = TTestBase<TImpl>;
TYPED_TEST_SUITE(TRpcTest, TAllTransports);
TYPED_TEST_SUITE(TNotGrpcTest, TWithoutGrpc);

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

TYPED_TEST(TRpcTest, RetryingSend)
{
    auto config = New<TRetryingChannelConfig>();
    config->Load(ConvertTo<INodePtr>(TYsonString(TStringBuf(
        "{retry_backoff_time=10}"))));

    IChannelPtr channel = CreateRetryingChannel(
        std::move(config),
        this->CreateChannel());

    {
        TMyProxy proxy(channel);
        auto req = proxy.FlakyCall();
        auto rspOrError = req->Invoke().Get();
        EXPECT_TRUE(rspOrError.IsOK()) << ToString(rspOrError);
    }

    // Channel must be asynchronously deleted after response handling finished.
    // In particular, all possible cyclic dependencies must be resolved.
    WaitForPredicate([&channel] {
        return channel->GetRefCount() == 1;
    });
}

TYPED_TEST(TRpcTest, UserTag)
{
    TMyProxy proxy(this->CreateChannel());
    auto req = proxy.PassCall();
    req->SetUser("test-user");
    req->SetUserTag("test-user-tag");
    auto rspOrError = req->Invoke().Get();
    EXPECT_TRUE(rspOrError.IsOK()) << ToString(rspOrError);
    const auto& rsp = rspOrError.Value();
    EXPECT_EQ(req->GetUser(), rsp->user());
    EXPECT_EQ(req->GetUserTag(), rsp->user_tag());
}

TYPED_TEST(TNotGrpcTest, SendSimple)
{
    TMyProxy proxy(this->CreateChannel());
    auto req = proxy.PassCall();
    req->SetUser("test-user");
    req->SetMutationId(TGuid::Create());
    req->SetRetry(true);
    auto rspOrError = req->Invoke().Get();
    EXPECT_TRUE(rspOrError.IsOK()) << ToString(rspOrError);
    const auto& rsp = rspOrError.Value();
    EXPECT_EQ(req->GetUser(), rsp->user());
    EXPECT_FALSE(rsp->has_user_tag());
    EXPECT_EQ(req->GetMutationId(), NYT::FromProto<TMutationId>(rsp->mutation_id()));
    EXPECT_EQ(true, rsp->retry());
}

TYPED_TEST(TNotGrpcTest, StreamingEcho)
{
    TMyProxy proxy(this->CreateChannel());
    proxy.SetDefaultRequestCodec(NCompression::ECodec::Lz4);
    proxy.SetDefaultResponseCodec(NCompression::ECodec::QuickLz);
    proxy.SetDefaultEnableLegacyRpcCodecs(false);

    const int AttachmentCount = 30;
    const ssize_t AttachmentSize = 2_MB;

    std::mt19937 randomGenerator;
    std::uniform_int_distribution<char> distribution(std::numeric_limits<char>::min(), std::numeric_limits<char>::max());

    std::vector<TSharedRef> attachments;

    for (int i = 0; i < AttachmentCount; ++i) {
        auto data = TSharedMutableRef::Allocate(AttachmentSize);
        for (size_t j = 0; j < AttachmentSize; ++j) {
            data[j] = distribution(randomGenerator);
        }
        attachments.push_back(std::move(data));
    }

    for (bool delayed : {false, true}) {
        auto req = proxy.StreamingEcho();
        req->set_delayed(delayed);
        req->SetHeavy(true);
        auto asyncInvokeResult = req->Invoke();

        std::vector<TSharedRef> receivedAttachments;

        for (const auto& sentData : attachments) {
            WaitFor(req->GetRequestAttachmentsStream()->Write(sentData))
                .ThrowOnError();

            if (!delayed) {
                auto receivedData = WaitFor(req->GetResponseAttachmentsStream()->Read())
                    .ValueOrThrow();
                receivedAttachments.push_back(std::move(receivedData));
            }
        }

        auto asyncCloseResult = req->GetRequestAttachmentsStream()->Close();
        EXPECT_FALSE(asyncCloseResult.IsSet());

        if (delayed) {
            for (int i = 0; i < AttachmentCount; ++i) {
                auto receivedData = WaitFor(req->GetResponseAttachmentsStream()->Read())
                    .ValueOrThrow();
                ASSERT_TRUE(receivedData);
                receivedAttachments.push_back(std::move(receivedData));
            }
        }

        {
            auto receivedData = WaitFor(req->GetResponseAttachmentsStream()->Read())
                .ValueOrThrow();
            ASSERT_FALSE(receivedData);
        }

        for (int i = 0; i < AttachmentCount; ++i) {
            EXPECT_TRUE(TRef::AreBitwiseEqual(attachments[i], receivedAttachments[i]));
        }

        WaitFor(asyncCloseResult)
            .ThrowOnError();

        auto rsp = WaitFor(asyncInvokeResult)
            .ValueOrThrow();

        EXPECT_EQ(AttachmentCount * AttachmentSize, rsp->total_size());
    }
}

TYPED_TEST(TNotGrpcTest, ClientStreamsAborted)
{
    TMyProxy proxy(this->CreateChannel());
    auto req = proxy.StreamingEcho();
    req->SetTimeout(TDuration::MilliSeconds(100));

    auto rspOrError = WaitFor(req->Invoke());
    EXPECT_EQ(NYT::EErrorCode::Timeout, rspOrError.GetCode());

    EXPECT_THROW({
        WaitFor(req->GetRequestAttachmentsStream()->Write(TSharedMutableRef::Allocate(100)))
            .ThrowOnError();
    }, TErrorException);

    EXPECT_THROW({
        WaitFor(req->GetResponseAttachmentsStream()->Read())
            .ThrowOnError();
    }, TErrorException);
}

TYPED_TEST(TNotGrpcTest, ServerStreamsAborted)
{
    TMyProxy proxy(this->CreateChannel());
    auto req = proxy.ServerStreamsAborted();
    req->SetTimeout(TDuration::MilliSeconds(100));

    auto rspOrError = WaitFor(req->Invoke());
    EXPECT_EQ(NYT::EErrorCode::Timeout, rspOrError.GetCode());

    WaitFor(this->Service_->GetServerStreamsAborted())
        .ThrowOnError();
}

TYPED_TEST(TNotGrpcTest, ClientNotReading)
{
    TMyProxy proxy(this->CreateChannel());
    proxy.DefaultServerAttachmentsStreamingParameters().WriteTimeout = TDuration::MilliSeconds(250);

    for (auto sleep : {false, true}) {
        auto expectedErrorCode = sleep ? NYT::EErrorCode::Timeout : NYT::EErrorCode::OK;

        auto req = proxy.StreamingEcho();
        req->set_delayed(true);
        auto invokeResult = req->Invoke();

        WaitFor(req->GetRequestAttachmentsStream()->Write(TSharedRef::FromString("hello")))
            .ThrowOnError();
        WaitFor(req->GetRequestAttachmentsStream()->Close())
            .ThrowOnError();
        WaitFor(req->GetResponseAttachmentsStream()->Read())
            .ThrowOnError();

        if (sleep) {
            Sleep(TDuration::MilliSeconds(750));
        }

        auto streamError = static_cast<TError>(
            WaitFor(req->GetResponseAttachmentsStream()->Read()));
        EXPECT_EQ(expectedErrorCode, streamError.GetCode());
        auto rspOrError = WaitFor(invokeResult);
        EXPECT_EQ(expectedErrorCode, rspOrError.GetCode());
    }
}

TYPED_TEST(TNotGrpcTest, ClientNotWriting)
{
    TMyProxy proxy(this->CreateChannel());
    proxy.DefaultServerAttachmentsStreamingParameters().ReadTimeout = TDuration::MilliSeconds(250);

    for (auto sleep : {false, true}) {
        auto expectedErrorCode = sleep ? NYT::EErrorCode::Timeout : NYT::EErrorCode::OK;

        auto req = proxy.StreamingEcho();
        auto invokeResult = req->Invoke();

        WaitFor(req->GetRequestAttachmentsStream()->Write(TSharedRef::FromString("hello")))
            .ThrowOnError();
        WaitFor(req->GetResponseAttachmentsStream()->Read())
            .ThrowOnError();

        if (sleep) {
            Sleep(TDuration::MilliSeconds(750));
        }

        auto closeError = WaitFor(req->GetRequestAttachmentsStream()->Close());
        auto readError = static_cast<TError>(
            WaitFor(req->GetResponseAttachmentsStream()->Read()));

        EXPECT_EQ(expectedErrorCode, closeError.GetCode());
        EXPECT_EQ(expectedErrorCode, readError.GetCode());
        auto rspOrError = WaitFor(invokeResult);
        EXPECT_EQ(expectedErrorCode, rspOrError.GetCode());
    }
}

TYPED_TEST(TNotGrpcTest, ServerNotReading)
{
    TMyProxy proxy(this->CreateChannel());
    proxy.DefaultClientAttachmentsStreamingParameters().WriteTimeout = TDuration::MilliSeconds(250);

    for (auto sleep : {false, true}) {
        auto expectedStreamErrorCode = sleep ? NYT::EErrorCode::Timeout : NYT::EErrorCode::OK;
        auto expectedInvokeErrorCode = sleep ? NYT::EErrorCode::Canceled : NYT::EErrorCode::OK;

        auto req = proxy.ServerNotReading();
        req->set_sleep(sleep);
        auto invokeResult = req->Invoke();

        auto data = TSharedRef::FromString("hello");
        WaitFor(req->GetRequestAttachmentsStream()->Write(data))
            .ThrowOnError();

        auto streamError = WaitFor(req->GetRequestAttachmentsStream()->Close());
        EXPECT_EQ(expectedStreamErrorCode, streamError.GetCode());
        auto rspOrError = WaitFor(invokeResult);
        EXPECT_EQ(expectedInvokeErrorCode, rspOrError.GetCode());
    }

    WaitFor(this->Service_->GetSlowCallCanceled())
        .ThrowOnError();
}

TYPED_TEST(TNotGrpcTest, ServerNotWriting)
{
    TMyProxy proxy(this->CreateChannel());
    proxy.DefaultClientAttachmentsStreamingParameters().ReadTimeout = TDuration::MilliSeconds(250);

    for (auto sleep : {false, true}) {
        auto expectedStreamErrorCode = sleep ? NYT::EErrorCode::Timeout : NYT::EErrorCode::OK;
        auto expectedInvokeErrorCode = sleep ? NYT::EErrorCode::Canceled : NYT::EErrorCode::OK;

        auto req = proxy.ServerNotWriting();
        req->set_sleep(sleep);
        auto invokeResult = req->Invoke();

        WaitFor(req->GetResponseAttachmentsStream()->Read())
            .ThrowOnError();

        auto streamError = WaitFor(req->GetResponseAttachmentsStream()->Read());
        EXPECT_EQ(expectedStreamErrorCode, streamError.GetCode());
        auto rspOrError = WaitFor(invokeResult);
        EXPECT_EQ(expectedInvokeErrorCode, rspOrError.GetCode());
    }

    WaitFor(this->Service_->GetSlowCallCanceled())
        .ThrowOnError();
}

TYPED_TEST(TNotGrpcTest, LaggyStreamingRequest)
{
    TMyProxy proxy(this->CreateChannel());
    proxy.DefaultServerAttachmentsStreamingParameters().ReadTimeout = TDuration::MilliSeconds(500);
    proxy.DefaultClientAttachmentsStreamingParameters().WriteTimeout = TDuration::MilliSeconds(500);

    auto req = proxy.StreamingEcho();
    req->SetHeavy(true);
    req->SetSendDelay(TDuration::MilliSeconds(250));
    req->SetTimeout(TDuration::Seconds(2));
    auto invokeResult = req->Invoke();

    WaitFor(req->GetRequestAttachmentsStream()->Close())
        .ThrowOnError();
    WaitFor(ExpectEndOfStream(req->GetResponseAttachmentsStream()))
        .ThrowOnError();
    WaitFor(invokeResult)
        .ThrowOnError();
}

TYPED_TEST(TNotGrpcTest, VeryLaggyStreamingRequest)
{
    auto configText = TString(R"({
        services = {
            MyService = {
                pending_payloads_timeout = 250;
            };
        };
    })");
    auto config = ConvertTo<TServerConfigPtr>(TYsonString(configText));
    this->Server_->Configure(config);

    TMyProxy proxy(this->CreateChannel());
    proxy.DefaultServerAttachmentsStreamingParameters().ReadTimeout = TDuration::MilliSeconds(500);

    auto start = Now();

    auto req = proxy.StreamingEcho();
    req->SetHeavy(true);
    req->SetSendDelay(TDuration::MilliSeconds(500));
    auto invokeResult = req->Invoke();

    auto closeError = WaitFor(req->GetRequestAttachmentsStream()->Close());
    EXPECT_EQ(NYT::EErrorCode::Timeout, closeError.GetCode());
    auto streamError = WaitFor(req->GetResponseAttachmentsStream()->Read());
    EXPECT_EQ(NYT::EErrorCode::Timeout, streamError.GetCode());
    auto rspOrError = WaitFor(invokeResult);
    EXPECT_EQ(NYT::EErrorCode::Timeout, rspOrError.GetCode());

    auto end = Now();
    int duration = (end - start).MilliSeconds();
    EXPECT_LE(duration, 2000);
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

    EXPECT_TRUE(AllSucceeded(asyncResults).Get().IsOK());
}

TYPED_TEST(TRpcTest, RegularAttachments)
{
    TMyProxy proxy(this->CreateChannel());
    auto req = proxy.RegularAttachments();

    req->Attachments().push_back(TSharedRef::FromString("Hello"));
    req->Attachments().push_back(TSharedRef::FromString("from"));
    req->Attachments().push_back(TSharedRef::FromString("TMyProxy"));

    auto rspOrError = req->Invoke().Get();
    EXPECT_TRUE(rspOrError.IsOK());
    const auto& rsp = rspOrError.Value();

    const auto& attachments = rsp->Attachments();
    EXPECT_EQ(3u, attachments.size());
    EXPECT_EQ("Hello_",     StringFromSharedRef(attachments[0]));
    EXPECT_EQ("from_",      StringFromSharedRef(attachments[1]));
    EXPECT_EQ("TMyProxy_",  StringFromSharedRef(attachments[2]));
}

TYPED_TEST(TRpcTest, NullAndEmptyAttachments)
{
    TMyProxy proxy(this->CreateChannel());
    auto req = proxy.NullAndEmptyAttachments();

    req->Attachments().push_back(TSharedRef());
    req->Attachments().push_back(TSharedRef::MakeEmpty());

    auto rspOrError = req->Invoke().Get();
    EXPECT_TRUE(rspOrError.IsOK());
    auto rsp = rspOrError.Value();

    const auto& attachments = rsp->Attachments();
    EXPECT_EQ(2u, attachments.size());
    EXPECT_FALSE(attachments[0]);
    EXPECT_TRUE(attachments[1]);
    EXPECT_TRUE(attachments[1].Empty());
}

TYPED_TEST(TNotGrpcTest, Compression)
{
    const auto requestCodecId = NCompression::ECodec::QuickLz;
    const auto responseCodecId = NCompression::ECodec::Snappy;

    TString message("This is a message string.");
    std::vector<TString> attachmentStrings({
        "This is an attachment string.",
        "640K ought to be enough for anybody.",
        "According to all known laws of aviation, there is no way that a bee should be able to fly."
    });

    TMyProxy proxy(this->CreateChannel());
    proxy.SetDefaultRequestCodec(requestCodecId);
    proxy.SetDefaultResponseCodec(responseCodecId);
    proxy.SetDefaultEnableLegacyRpcCodecs(false);

    auto req = proxy.Compression();
    req->set_request_codec(static_cast<int>(requestCodecId));
    req->set_message(message);
    for (const auto& attachmentString : attachmentStrings) {
        req->Attachments().push_back(TSharedRef::FromString(attachmentString));
    }

    auto rspOrError = req->Invoke().Get();
    rspOrError.ThrowOnError();
    EXPECT_TRUE(rspOrError.IsOK());
    auto rsp = rspOrError.Value();

    EXPECT_TRUE(rsp->message() == message);
    EXPECT_TRUE(rsp->GetResponseMessage().Size() >= 2);
    const auto& serializedResponseBody = SerializeProtoToRefWithCompression(*rsp, responseCodecId);
    const auto& compressedResponseBody = rsp->GetResponseMessage()[1];
    EXPECT_TRUE(TRef::AreBitwiseEqual(compressedResponseBody, serializedResponseBody));

    const auto& attachments = rsp->Attachments();
    EXPECT_TRUE(attachments.size() == attachmentStrings.size());
    EXPECT_TRUE(rsp->GetResponseMessage().Size() == attachments.size() + 2);
    auto* responseCodec = NCompression::GetCodec(responseCodecId);
    for (int i = 0; i < std::ssize(attachments); ++i) {
        EXPECT_TRUE(StringFromSharedRef(attachments[i]) == attachmentStrings[i]);
        auto compressedAttachment = responseCodec->Compress(attachments[i]);
        EXPECT_TRUE(TRef::AreBitwiseEqual(rsp->GetResponseMessage()[i + 2], compressedAttachment));
    }
}

#if !defined(_asan_enabled_) && !defined(_msan_enabled_)

TYPED_TEST(TRpcTest, ResponseMemoryTag)
{
    constexpr TMemoryTag TestMemoryTag = 1234;
    auto initialMemoryUsage = GetMemoryUsageForTag(TestMemoryTag);

    std::vector<TMyProxy::TRspPassCallPtr> rsps;
    {
        TMyProxy proxy(this->CreateChannel());
        TString longString(1000, 'a');

        NYTAlloc::TMemoryTagGuard guard(TestMemoryTag);

        for (int i = 0; i < 100; ++i) {
            auto req = proxy.PassCall();
            req->SetUser(longString);
            req->SetMutationId(TGuid::Create());
            req->SetRetry(false);
            auto err = req->Invoke().Get();
            rsps.push_back(err.ValueOrThrow());
        }
    }

    EXPECT_GE(GetMemoryUsageForTag(TestMemoryTag) - initialMemoryUsage, 100'000u);
}

#endif

TYPED_TEST(TNotGrpcTest, RequestBytesThrottling)
{
    auto configText = TString(R"({
        services = {
            MyService = {
                methods = {
                    RequestBytesThrottledCall = {
                        request_bytes_throttler = {
                            limit = 1000000;
                        }
                    }
                }
            };
        };
    })");
    auto config = ConvertTo<TServerConfigPtr>(TYsonString(configText));
    this->Server_->Configure(config);

    TMyProxy proxy(this->CreateChannel());

    auto makeCall = [&] {
        auto req = proxy.RequestBytesThrottledCall();
        req->Attachments().push_back(TSharedMutableRef::Allocate(100'000));
        return req->Invoke().AsVoid();
    };

    std::vector<TFuture<void>> futures;
    for (int i = 0; i < 30; ++i) {
        futures.push_back(makeCall());
    }

    NProfiling::TWallTimer timer;
    EXPECT_TRUE(AllSucceeded(std::move(futures)).Get().IsOK());
    EXPECT_LE(std::abs(static_cast<i64>(timer.GetElapsedTime().MilliSeconds()) - 3000), 200);
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
    req->SetAcknowledgementTimeout(std::nullopt);
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

// NB: Realms are not supported in RPC over GRPC.
TYPED_TEST(TNotGrpcTest, UnknownRealm)
{
    TMyProxy proxy(this->CreateChannel());
    auto req = proxy.DoNothing();
    ToProto(req->Header().mutable_realm_id(), TGuid::FromString("1-2-3-4"));
    auto rspOrError = req->Invoke().Get();
    EXPECT_EQ(NRpc::EErrorCode::NoSuchService, rspOrError.GetCode());
    EXPECT_TRUE(rspOrError.FindMatching(NRpc::EErrorCode::UnknownRealm));
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
    WaitFor(this->Service_->GetSlowCallCanceled())
        .ThrowOnError();
}

TYPED_TEST(TRpcTest, ClientCancel)
{
    TMyProxy proxy(this->CreateChannel());
    auto req = proxy.SlowCanceledCall();
    auto asyncRspOrError = req->Invoke();
    Sleep(TDuration::Seconds(0.5));
    EXPECT_FALSE(asyncRspOrError.IsSet());
    asyncRspOrError.Cancel(TError("Error"));
    Sleep(TDuration::Seconds(0.1));
    EXPECT_TRUE(asyncRspOrError.IsSet());
    auto rspOrError = asyncRspOrError.Get();
    EXPECT_TRUE(this->CheckCancelCode(rspOrError.GetCode()));
    WaitFor(this->Service_->GetSlowCallCanceled())
        .ThrowOnError();
}

TYPED_TEST(TRpcTest, SlowCall)
{
    TMyProxy proxy(this->CreateChannel());
    proxy.SetDefaultTimeout(TDuration::Seconds(2.0));
    auto req = proxy.SlowCall();
    auto rspOrError = req->Invoke().Get();
    EXPECT_TRUE(rspOrError.IsOK());
}

TYPED_TEST(TRpcTest, RequestQueueSizeLimit)
{
    TMyProxy proxy(this->CreateChannel());
    std::vector<TFuture<void>> futures;
    for (int i = 0; i < 30; ++i) {
        auto req = proxy.SlowCall();
        futures.push_back(req->Invoke().AsVoid());
    }
    Sleep(TDuration::MilliSeconds(100));
    {
        auto req = proxy.SlowCall();
        EXPECT_EQ(NRpc::EErrorCode::RequestQueueSizeLimitExceeded, req->Invoke().Get().GetCode());
    }
    EXPECT_TRUE(AllSucceeded(std::move(futures)).Get().IsOK());
}

TYPED_TEST(TRpcTest, ConcurrencyLimit)
{
    TMyProxy proxy(this->CreateChannel());
    std::vector<TFuture<void>> futures;
    for (int i = 0; i < 10; ++i) {
        auto req = proxy.SlowCall();
        futures.push_back(req->Invoke().AsVoid());
    }

    Sleep(TDuration::MilliSeconds(100));

    TFuture<void> backlogFuture;
    {
        auto req = proxy.SlowCall();
        backlogFuture = req->Invoke().AsVoid();
    }

    EXPECT_TRUE(AllSucceeded(std::move(futures)).Get().IsOK());

    Sleep(TDuration::MilliSeconds(400));
    EXPECT_FALSE(backlogFuture.IsSet());

    EXPECT_TRUE(backlogFuture.Get().IsOK());
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
    WaitFor(this->Service_->GetSlowCallCanceled())
        .ThrowOnError();
}

TYPED_TEST(TNotGrpcTest, ProtocolVersionMismatch)
{
    TMyIncorrectProtocolVersionProxy proxy(this->CreateChannel());
    auto req = proxy.SomeCall();
    req->set_a(42);
    auto rspOrError = req->Invoke().Get();
    EXPECT_EQ(NRpc::EErrorCode::ProtocolError, rspOrError.GetCode());
}

TYPED_TEST(TNotGrpcTest, RequiredServerFeatureSupported)
{
    TMyProxy proxy(this->CreateChannel());
    auto req = proxy.PassCall();
    req->RequireServerFeature(EMyFeature::Great);
    auto rspOrError = req->Invoke().Get();
    EXPECT_TRUE(rspOrError.IsOK()) << ToString(rspOrError);
}

TYPED_TEST(TNotGrpcTest, RequiredServerFeatureNotSupported)
{
    TMyProxy proxy(this->CreateChannel());
    auto req = proxy.PassCall();
    req->RequireServerFeature(EMyFeature::Cool);
    auto rspOrError = req->Invoke().Get();
    EXPECT_EQ(NRpc::EErrorCode::UnsupportedServerFeature, rspOrError.GetCode());
    EXPECT_EQ(static_cast<int>(EMyFeature::Cool), rspOrError.Attributes().Get<int>(FeatureIdAttributeKey));
    EXPECT_EQ(ToString(EMyFeature::Cool), rspOrError.Attributes().Get<TString>(FeatureNameAttributeKey));
}

TYPED_TEST(TNotGrpcTest, RequiredClientFeatureSupported)
{
    TMyProxy proxy(this->CreateChannel());
    auto req = proxy.RequireCoolFeature();
    req->DeclareClientFeature(EMyFeature::Cool);
    auto rspOrError = req->Invoke().Get();
    EXPECT_TRUE(rspOrError.IsOK()) << ToString(rspOrError);
}

TYPED_TEST(TNotGrpcTest, RequiredClientFeatureNotSupported)
{
    TMyProxy proxy(this->CreateChannel());
    auto req = proxy.RequireCoolFeature();
    req->DeclareClientFeature(EMyFeature::Great);
    auto rspOrError = req->Invoke().Get();
    EXPECT_EQ(NRpc::EErrorCode::UnsupportedClientFeature, rspOrError.GetCode());
    EXPECT_EQ(static_cast<int>(EMyFeature::Cool), rspOrError.Attributes().Get<int>(FeatureIdAttributeKey));
    EXPECT_EQ(ToString(EMyFeature::Cool), rspOrError.Attributes().Get<TString>(FeatureNameAttributeKey));
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

class TAttachmentsInputStreamTest
    : public ::testing::Test
{
protected:
    TAttachmentsInputStreamPtr CreateStream(std::optional<TDuration> timeout = {})
    {
        return New<TAttachmentsInputStream>(
            BIND([=] {}),
            nullptr,
            timeout);
    }

    static TStreamingPayload MakePayload(int sequenceNumber, std::vector<TSharedRef> attachments)
    {
        return TStreamingPayload{
            NCompression::ECodec::None,
            EMemoryZone::Normal,
            sequenceNumber,
            std::move(attachments)
        };
    }
};

TEST_F(TAttachmentsInputStreamTest, AbortPropagatesToRead)
{
    auto stream = CreateStream();

    auto future = stream->Read();
    EXPECT_FALSE(future.IsSet());
    stream->Abort(TError("oops"));
    EXPECT_TRUE(future.IsSet());
    EXPECT_FALSE(future.Get().IsOK());
}

TEST_F(TAttachmentsInputStreamTest, EnqueueBeforeRead)
{
    auto stream = CreateStream();

    auto payload = TSharedRef::FromString("payload");
    stream->EnqueuePayload(MakePayload(0, std::vector<TSharedRef>{payload}));

    auto future = stream->Read();
    EXPECT_TRUE(future.IsSet());
    EXPECT_TRUE(TRef::AreBitwiseEqual(payload, future.Get().ValueOrThrow()));
    EXPECT_EQ(7, stream->GetFeedback().ReadPosition);
}

TEST_F(TAttachmentsInputStreamTest, ReadBeforeEnqueue)
{
    auto stream = CreateStream();

    auto future = stream->Read();
    EXPECT_FALSE(future.IsSet());

    auto payload = TSharedRef::FromString("payload");
    stream->EnqueuePayload(MakePayload(0, std::vector<TSharedRef>{payload}));

    EXPECT_TRUE(future.IsSet());
    EXPECT_TRUE(TRef::AreBitwiseEqual(payload, future.Get().ValueOrThrow()));
    EXPECT_EQ(7, stream->GetFeedback().ReadPosition);
}

TEST_F(TAttachmentsInputStreamTest, CloseBeforeRead)
{
    auto stream = CreateStream();

    auto payload = TSharedRef::FromString("payload");
    stream->EnqueuePayload(MakePayload(0, {payload}));
    stream->EnqueuePayload(MakePayload(1, {TSharedRef()}));

    auto future1 = stream->Read();
    EXPECT_TRUE(future1.IsSet());
    EXPECT_TRUE(TRef::AreBitwiseEqual(payload, future1.Get().ValueOrThrow()));
    EXPECT_EQ(7, stream->GetFeedback().ReadPosition);

    auto future2 = stream->Read();
    EXPECT_TRUE(future2.IsSet());
    EXPECT_TRUE(!future2.Get().ValueOrThrow());
    EXPECT_EQ(8, stream->GetFeedback().ReadPosition);
}

TEST_F(TAttachmentsInputStreamTest, Reordering)
{
    auto stream = CreateStream();

    auto payload1 = TSharedRef::FromString("payload1");
    auto payload2 = TSharedRef::FromString("payload2");

    stream->EnqueuePayload(MakePayload(1, {payload2}));
    stream->EnqueuePayload(MakePayload(0, {payload1}));

    auto future1 = stream->Read();
    EXPECT_TRUE(future1.IsSet());
    EXPECT_TRUE(TRef::AreBitwiseEqual(payload1, future1.Get().ValueOrThrow()));
    EXPECT_EQ(8, stream->GetFeedback().ReadPosition);

    auto future2 = stream->Read();
    EXPECT_TRUE(future2.IsSet());
    EXPECT_TRUE(TRef::AreBitwiseEqual(payload2, future2.Get().ValueOrThrow()));
    EXPECT_EQ(16, stream->GetFeedback().ReadPosition);
}

TEST_F(TAttachmentsInputStreamTest, EmptyAttachmentReadPosition)
{
    auto stream = CreateStream();
    stream->EnqueuePayload(MakePayload(0, {TSharedMutableRef::Allocate(0)}));
    EXPECT_EQ(0, stream->GetFeedback().ReadPosition);
    auto future = stream->Read();
    EXPECT_TRUE(future.IsSet());
    EXPECT_EQ(0u, future.Get().ValueOrThrow().size());
    EXPECT_EQ(1, stream->GetFeedback().ReadPosition);
}

TEST_F(TAttachmentsInputStreamTest, Close)
{
    auto stream = CreateStream();
    stream->EnqueuePayload(MakePayload(0, {TSharedRef()}));
    auto future = stream->Read();
    EXPECT_TRUE(future.IsSet());
    EXPECT_FALSE(future.Get().ValueOrThrow());
}

TEST_F(TAttachmentsInputStreamTest, Timeout)
{
    auto stream = CreateStream(TDuration::MilliSeconds(100));
    auto future = stream->Read();
    auto error = future.Get();
    EXPECT_FALSE(error.IsOK());
    EXPECT_EQ(NYT::EErrorCode::Timeout, error.GetCode());
}

////////////////////////////////////////////////////////////////////////////////

class TAttachmentsOutputStreamTest
    : public ::testing::Test
{
protected:
    int PullCallbackCounter_;

    TAttachmentsOutputStreamPtr CreateStream(
        ssize_t windowSize,
        std::optional<TDuration> timeout = {})
    {
        PullCallbackCounter_ = 0;
        return New<TAttachmentsOutputStream>(
            EMemoryZone::Normal,
            NCompression::ECodec::None,
            nullptr,
            BIND([=] {
                ++PullCallbackCounter_;
            }),
            windowSize,
            timeout);
    }
};

TEST_F(TAttachmentsOutputStreamTest, NullPull)
{
    auto stream = CreateStream(100);
    EXPECT_FALSE(stream->TryPull());
}

TEST_F(TAttachmentsOutputStreamTest, SinglePull)
{
    auto stream = CreateStream(100);

    auto payload = TSharedRef::FromString("payload");
    auto future = stream->Write(payload);
    EXPECT_EQ(1, PullCallbackCounter_);
    EXPECT_TRUE(future.IsSet());
    EXPECT_TRUE(future.Get().IsOK());

    auto result = stream->TryPull();
    EXPECT_TRUE(result);
    EXPECT_EQ(0, result->SequenceNumber);
    EXPECT_EQ(1u, result->Attachments.size());
    EXPECT_TRUE(TRef::AreBitwiseEqual(payload, result->Attachments[0]));
}

TEST_F(TAttachmentsOutputStreamTest, MultiplePull)
{
    auto stream = CreateStream(100);

    std::vector<TSharedRef> payloads;
    for (int i = 0; i < 10; ++i) {
        auto payload = TSharedRef::FromString("payload" + ToString(i));
        payloads.push_back(payload);
        auto future = stream->Write(payload);
        EXPECT_EQ(i + 1, PullCallbackCounter_);
        EXPECT_TRUE(future.IsSet());
        EXPECT_TRUE(future.Get().IsOK());
    }

    auto result = stream->TryPull();
    EXPECT_TRUE(result);
    EXPECT_EQ(0, result->SequenceNumber);
    EXPECT_EQ(10u, result->Attachments.size());
    for (size_t i = 0; i < 10; ++i) {
        EXPECT_TRUE(TRef::AreBitwiseEqual(payloads[i], result->Attachments[i]));
    }
}

TEST_F(TAttachmentsOutputStreamTest, Backpressure)
{
    auto stream = CreateStream(5);

    auto payload1 = TSharedRef::FromString("abc");
    auto future1 = stream->Write(payload1);
    EXPECT_TRUE(future1.IsSet());
    EXPECT_TRUE(future1.Get().IsOK());
    EXPECT_EQ(1, PullCallbackCounter_);

    auto payload2 = TSharedRef::FromString("def");
    auto future2 = stream->Write(payload2);
    EXPECT_FALSE(future2.IsSet());
    EXPECT_EQ(2, PullCallbackCounter_);

    auto result1 = stream->TryPull();
    EXPECT_TRUE(result1);
    EXPECT_EQ(0, result1->SequenceNumber);
    EXPECT_EQ(1u, result1->Attachments.size());
    EXPECT_TRUE(TRef::AreBitwiseEqual(payload1, result1->Attachments[0]));

    EXPECT_FALSE(future2.IsSet());

    stream->HandleFeedback({3});

    EXPECT_EQ(3, PullCallbackCounter_);

    EXPECT_TRUE(future1.IsSet());
    EXPECT_TRUE(future1.Get().IsOK());

    EXPECT_TRUE(future2.IsSet());
    EXPECT_TRUE(future2.Get().IsOK());

    auto payload3 = TSharedRef::FromString("x");
    auto future3 = stream->Write(payload3);
    EXPECT_TRUE(future3.IsSet());
    EXPECT_TRUE(future3.Get().IsOK());
    EXPECT_EQ(4, PullCallbackCounter_);

    auto result2 = stream->TryPull();
    EXPECT_TRUE(result2);
    EXPECT_EQ(2u, result2->Attachments.size());
    EXPECT_TRUE(TRef::AreBitwiseEqual(payload2, result2->Attachments[0]));
    EXPECT_TRUE(TRef::AreBitwiseEqual(payload3, result2->Attachments[1]));
}

TEST_F(TAttachmentsOutputStreamTest, Abort1)
{
    auto stream = CreateStream(5);

    auto payload1 = TSharedRef::FromString("abcabc");
    auto future1 = stream->Write(payload1);
    EXPECT_FALSE(future1.IsSet());

    auto future2 = stream->Close();
    EXPECT_FALSE(future1.IsSet());

    stream->Abort(TError("oops"));

    EXPECT_TRUE(future1.IsSet());
    EXPECT_FALSE(future1.Get().IsOK());

    EXPECT_TRUE(future2.IsSet());
    EXPECT_FALSE(future2.Get().IsOK());
}

TEST_F(TAttachmentsOutputStreamTest, Abort2)
{
    auto stream = CreateStream(5);

    auto payload1 = TSharedRef::FromString("abcabc");
    auto future1 = stream->Write(payload1);
    EXPECT_FALSE(future1.IsSet());

    stream->Abort(TError("oops"));

    EXPECT_TRUE(future1.IsSet());
    EXPECT_FALSE(future1.Get().IsOK());

    auto future2 = stream->Close();
    EXPECT_TRUE(future2.IsSet());
    EXPECT_FALSE(future2.Get().IsOK());
}

TEST_F(TAttachmentsOutputStreamTest, Close1)
{
    auto stream = CreateStream(5);

    auto future = stream->Close();
    EXPECT_FALSE(future.IsSet());
    EXPECT_EQ(1, PullCallbackCounter_);

    auto result = stream->TryPull();
    EXPECT_TRUE(result);
    EXPECT_EQ(0, result->SequenceNumber);
    EXPECT_EQ(1u, result->Attachments.size());
    EXPECT_FALSE(result->Attachments[0]);

    stream->HandleFeedback({1});

    EXPECT_TRUE(future.IsSet());
    EXPECT_TRUE(future.Get().IsOK());
}

TEST_F(TAttachmentsOutputStreamTest, Close2)
{
    auto stream = CreateStream(5);

    auto payload = TSharedRef::FromString("abc");
    auto future1 = stream->Write(payload);
    EXPECT_TRUE(future1.IsSet());
    EXPECT_TRUE(future1.Get().IsOK());
    EXPECT_EQ(1, PullCallbackCounter_);

    auto future2 = stream->Close();
    EXPECT_FALSE(future2.IsSet());
    EXPECT_EQ(2, PullCallbackCounter_);

    auto result = stream->TryPull();
    EXPECT_TRUE(result);
    EXPECT_EQ(0, result->SequenceNumber);
    EXPECT_EQ(2u, result->Attachments.size());
    EXPECT_TRUE(TRef::AreBitwiseEqual(payload, result->Attachments[0]));
    EXPECT_FALSE(result->Attachments[1]);

    stream->HandleFeedback({3});

    EXPECT_FALSE(future2.IsSet());

    stream->HandleFeedback({4});

    EXPECT_TRUE(future2.IsSet());
    EXPECT_TRUE(future2.Get().IsOK());
}

TEST_F(TAttachmentsOutputStreamTest, WriteTimeout)
{
    auto stream = CreateStream(5, TDuration::MilliSeconds(100));

    auto payload = TSharedRef::FromString("abc");

    auto future1 = stream->Write(payload);
    EXPECT_TRUE(future1.IsSet());
    EXPECT_TRUE(future1.Get().IsOK());

    auto future2 = stream->Write(payload);
    EXPECT_FALSE(future2.IsSet());
    auto error = future2.Get();
    EXPECT_FALSE(error.IsOK());
    EXPECT_EQ(NYT::EErrorCode::Timeout, error.GetCode());
}

TEST_F(TAttachmentsOutputStreamTest, CloseTimeout)
{
    auto stream = CreateStream(5, TDuration::MilliSeconds(100));

    auto future = stream->Close();
    EXPECT_FALSE(future.IsSet());
    auto error = future.Get();
    EXPECT_FALSE(error.IsOK());
    EXPECT_EQ(NYT::EErrorCode::Timeout, error.GetCode());
}

TEST_F(TAttachmentsOutputStreamTest, CloseTimeout2)
{
    auto stream = CreateStream(10, TDuration::MilliSeconds(100));

    auto payload = TSharedRef::FromString("abc");

    auto future1 = stream->Write(payload);
    EXPECT_TRUE(future1.IsSet());
    EXPECT_TRUE(future1.Get().IsOK());

    auto future2 = stream->Write(payload);
    EXPECT_TRUE(future2.IsSet());
    EXPECT_TRUE(future2.Get().IsOK());

    auto future3 = stream->Close();
    EXPECT_FALSE(future3.IsSet());

    stream->HandleFeedback({3});

    EXPECT_FALSE(future3.IsSet());

    Sleep(TDuration::MilliSeconds(500));

    ASSERT_TRUE(future3.IsSet());
    auto error = future3.Get();
    EXPECT_FALSE(error.IsOK());
    EXPECT_EQ(NYT::EErrorCode::Timeout, error.GetCode());
}

////////////////////////////////////////////////////////////////////////////////

TEST(TCachingChannelFactoryTest, IdleChannels)
{
    class TChannelFactory
        : public IChannelFactory
    {
    public:
        virtual IChannelPtr CreateChannel(const TString& /*address*/) override
        {
            return CreateLocalChannel(Server_);
        }

        virtual IChannelPtr CreateChannel(const TAddressWithNetwork& addressWithNetwork) override
        {
            return CreateChannel(addressWithNetwork.Address);
        }

    private:
        const IServerPtr Server_ = CreateLocalServer();
    };

    auto factory = New<TChannelFactory>();
    auto cachingFactory = CreateCachingChannelFactory(factory, TDuration::MilliSeconds(500));
    auto channel = cachingFactory->CreateChannel("");
    EXPECT_EQ(channel, cachingFactory->CreateChannel(""));

    Sleep(TDuration::MilliSeconds(1000));
    EXPECT_EQ(channel, cachingFactory->CreateChannel(""));

    auto weakChannel = MakeWeak(channel);
    channel.Reset();

    Sleep(TDuration::MilliSeconds(1000));
    EXPECT_TRUE(weakChannel.IsExpired());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NRpc
