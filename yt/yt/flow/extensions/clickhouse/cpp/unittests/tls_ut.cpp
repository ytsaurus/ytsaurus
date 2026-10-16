#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/flow/extensions/clickhouse/cpp/sink.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>

#include <library/cpp/testing/common/env.h>

#include <contrib/libs/clickhouse-cpp/clickhouse/exceptions.h>

#include <openssl/err.h>
#include <openssl/ssl.h>

#include <util/generic/strbuf.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <optional>
#include <thread>

namespace NYT::NFlow {
namespace {

class TFirstByteServer
{
public:
    TFirstByteServer()
    {
        ListenerFd_ = socket(AF_INET, SOCK_STREAM, 0);
        YT_VERIFY(ListenerFd_ >= 0);

        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        address.sin_port = 0;
        YT_VERIFY(bind(ListenerFd_, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == 0);
        YT_VERIFY(listen(ListenerFd_, /*backlog*/ 1) == 0);

        socklen_t addressLength = sizeof(address);
        YT_VERIFY(getsockname(ListenerFd_, reinterpret_cast<sockaddr*>(&address), &addressLength) == 0);
        Port_ = ntohs(address.sin_port);

        Thread_ = std::thread([this] {
            AcceptOnce();
        });
    }

    ~TFirstByteServer()
    {
        if (Thread_.joinable()) {
            Thread_.join();
        }
        close(ListenerFd_);
    }

    ui16 GetPort() const
    {
        return Port_;
    }

    std::optional<unsigned char> WaitForFirstByte()
    {
        Thread_.join();
        return FirstByte_;
    }

private:
    int ListenerFd_;
    ui16 Port_;
    std::thread Thread_;
    std::optional<unsigned char> FirstByte_;

    void AcceptOnce()
    {
        int connectionFd = accept(ListenerFd_, nullptr, nullptr);
        if (connectionFd < 0) {
            return;
        }

        unsigned char byte;
        if (recv(connectionFd, &byte, 1, MSG_WAITALL) == 1) {
            FirstByte_ = byte;
        }
        close(connectionFd);
    }
};

class TFakeTlsServer
{
public:
    TFakeTlsServer()
    {
        ListenerFd_ = socket(AF_INET, SOCK_STREAM, 0);
        YT_VERIFY(ListenerFd_ >= 0);

        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        address.sin_port = 0;
        YT_VERIFY(bind(ListenerFd_, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == 0);
        YT_VERIFY(listen(ListenerFd_, /*backlog*/ 1) == 0);

        socklen_t addressLength = sizeof(address);
        YT_VERIFY(getsockname(ListenerFd_, reinterpret_cast<sockaddr*>(&address), &addressLength) == 0);
        Port_ = ntohs(address.sin_port);

        auto certificatePath = ArcadiaFromCurrentLocation(__SOURCE_FILE__, "testdata/server.crt");
        auto privateKeyPath = ArcadiaFromCurrentLocation(__SOURCE_FILE__, "testdata/server.key");

        Thread_ = std::thread([this, certificatePath, privateKeyPath] {
            AcceptOnce(certificatePath, privateKeyPath);
        });
    }

    ~TFakeTlsServer()
    {
        Thread_.join();
        close(ListenerFd_);
    }

    ui16 GetPort() const
    {
        return Port_;
    }

private:
    int ListenerFd_;
    ui16 Port_;
    std::thread Thread_;

    void AcceptOnce(const TString& certificatePath, const TString& privateKeyPath)
    {
        int connectionFd = accept(ListenerFd_, nullptr, nullptr);
        if (connectionFd < 0) {
            return;
        }

        auto* context = SSL_CTX_new(TLS_server_method());
        YT_VERIFY(context);
        YT_VERIFY(SSL_CTX_use_certificate_file(context, certificatePath.c_str(), SSL_FILETYPE_PEM) == 1);
        YT_VERIFY(SSL_CTX_use_PrivateKey_file(context, privateKeyPath.c_str(), SSL_FILETYPE_PEM) == 1);

        auto* ssl = SSL_new(context);
        YT_VERIFY(ssl);
        SSL_set_fd(ssl, connectionFd);

        // The peer may reject the certificate before or during the handshake;
        // that is a valid outcome for the "untrusted certificate" test case.
        if (SSL_accept(ssl) == 1) {
            char buffer[256];
            SSL_read(ssl, buffer, sizeof(buffer));
            SSL_shutdown(ssl);
        }

        SSL_free(ssl);
        SSL_CTX_free(context);
        close(connectionFd);
    }
};

TCommonClickHouseSinkParametersPtr MakeTlsParameters(ui16 port, bool skipVerification, const std::vector<std::string>& caFiles)
{
    auto parameters = New<TCommonClickHouseSinkParameters>();
    // The checked-in certificate's SAN only covers the DNS name "localhost";
    // clickhouse-cpp verifies the host via SSL_set1_host, which matches DNS
    // SAN entries, not IP SAN entries.
    parameters->Host = "localhost";
    parameters->Port = port;
    parameters->EnableTls = true;
    parameters->TlsSkipVerification = skipVerification;
    parameters->TlsCaFiles = caFiles;
    return parameters;
}

// clickhouse-cpp uses OpenSSLError for both verification failures and
// post-handshake closes; only the message distinguishes them.
bool IsCertificateVerificationFailure(const std::exception& ex)
{
    return TStringBuf(ex.what()).Contains("X509_v error");
}

// clickhouse-cpp leaks SSL_get_peer_certificate()'s owned reference while formatting a
// verification failure, so these tests cover TLS on the wire and the verification default
// separately instead of exercising that upstream leak under LSan.
TEST(TClickHouseSinkTlsTest, EnableTlsSendsTlsRecordOnTheWire)
{
    TFirstByteServer server;
    auto parameters = MakeTlsParameters(server.GetPort(), /*skipVerification*/ true, /*caFiles*/ {});

    try {
        clickhouse::Client client(MakeClientOptions(*parameters, ResolveShards(*parameters).front(), TDuration::Seconds(5)));
    } catch (const std::exception&) {
        // Expected: the fake server never completes any protocol.
    }

    // 0x16 is the TLS record type for a handshake message (ClientHello).
    EXPECT_EQ(server.WaitForFirstByte(), 0x16);
}

TEST(TClickHouseSinkTlsTest, DisabledTlsSendsNativeProtocolHelloOnTheWire)
{
    TFirstByteServer server;
    auto parameters = New<TCommonClickHouseSinkParameters>();
    parameters->Host = "localhost";
    parameters->Port = server.GetPort();

    try {
        clickhouse::Client client(MakeClientOptions(*parameters, ResolveShards(*parameters).front(), TDuration::Seconds(5)));
    } catch (const std::exception&) {
        // Expected: the fake server never completes any protocol.
    }

    // 0x00 is ClientCodes::Hello, the first varint-encoded byte of the
    // native ClickHouse wire protocol.
    EXPECT_EQ(server.WaitForFirstByte(), 0x00);
}

TEST(TClickHouseSinkTlsTest, DefaultTlsOptionsRequireCertificateVerification)
{
    auto parameters = New<TCommonClickHouseSinkParameters>();
    parameters->Host = "localhost";
    parameters->EnableTls = true;

    auto options = MakeClientOptions(*parameters, ResolveShards(*parameters).front(), TDuration::Seconds(5));

    ASSERT_TRUE(options.ssl_options.has_value());
    EXPECT_FALSE(options.ssl_options->skip_verification);
    EXPECT_TRUE(options.ssl_options->path_to_ca_files.empty());
    EXPECT_TRUE(options.ssl_options->path_to_ca_directory.empty());
}

TEST(TClickHouseSinkTlsTest, MultiEndpointClientUsesSharedTlsOptions)
{
    auto parameters = New<TCommonClickHouseSinkParameters>();
    parameters->ShardHosts = {{"a", {"primary.clickhouse", "replica-1.clickhouse", "replica-2.clickhouse"}}};
    parameters->Port = 9440;
    parameters->EnableTls = true;
    parameters->TlsSkipVerification = true;
    parameters->TlsCaFiles = {"first-ca.pem", "second-ca.pem"};
    parameters->TlsCaDirectory = "ca-directory";

    auto options = MakeClientOptions(*parameters, ResolveShards(*parameters).front(), TDuration::Seconds(5));

    EXPECT_EQ(options.host, "primary.clickhouse");
    EXPECT_EQ(
        options.endpoints,
        (std::vector<clickhouse::Endpoint>{
            {.host = "replica-1.clickhouse", .port = 9440},
            {.host = "replica-2.clickhouse", .port = 9440},
        }));
    ASSERT_TRUE(options.ssl_options.has_value());
    EXPECT_TRUE(options.ssl_options->skip_verification);
    EXPECT_EQ(options.ssl_options->path_to_ca_files, (std::vector<std::string>{"first-ca.pem", "second-ca.pem"}));
    EXPECT_EQ(options.ssl_options->path_to_ca_directory, "ca-directory");
}

TEST(TClickHouseSinkTlsTest, TrustedCaFileAllowsHandshakeToComplete)
{
    TFakeTlsServer server;
    auto certificatePath = ArcadiaFromCurrentLocation(__SOURCE_FILE__, "testdata/server.crt");
    auto parameters = MakeTlsParameters(server.GetPort(), /*skipVerification*/ false, /*caFiles*/ {certificatePath.c_str()});

    EXPECT_THROW(
        {
            try {
                clickhouse::Client client(MakeClientOptions(*parameters, ResolveShards(*parameters).front(), TDuration::Seconds(5)));
            } catch (const std::exception& ex) {
                EXPECT_FALSE(IsCertificateVerificationFailure(ex)) << "Unexpected TLS-layer failure: " << ex.what();
                throw;
            }
        },
        clickhouse::Error);
}

TEST(TClickHouseSinkTlsTest, SkipVerificationAllowsUntrustedCertificate)
{
    TFakeTlsServer server;
    auto parameters = MakeTlsParameters(server.GetPort(), /*skipVerification*/ true, /*caFiles*/ {});

    EXPECT_THROW(
        {
            try {
                clickhouse::Client client(MakeClientOptions(*parameters, ResolveShards(*parameters).front(), TDuration::Seconds(5)));
            } catch (const std::exception& ex) {
                EXPECT_FALSE(IsCertificateVerificationFailure(ex)) << "Unexpected TLS-layer failure: " << ex.what();
                throw;
            }
        },
        clickhouse::Error);
}

TEST(TClickHouseSinkTlsTest, TlsParametersWithoutEnableTlsAreRejected)
{
    EXPECT_THROW(
        ConvertTo<TCommonClickHouseSinkParametersPtr>(NYTree::BuildYsonNodeFluently()
                .BeginMap()
                .Item("host")
                .Value("localhost")
                .Item("table")
                .Value("t")
                .Item("tls_skip_verification")
                .Value(true)
                .EndMap()),
        std::exception);
}

TEST(TClickHouseSinkTlsTest, TlsParametersWithEnableTlsAreAccepted)
{
    auto parameters = ConvertTo<TCommonClickHouseSinkParametersPtr>(NYTree::BuildYsonNodeFluently()
            .BeginMap()
            .Item("host")
            .Value("localhost")
            .Item("table")
            .Value("t")
            .Item("enable_tls")
            .Value(true)
            .Item("tls_skip_verification")
            .Value(true)
            .EndMap());

    EXPECT_TRUE(parameters->EnableTls);
    EXPECT_TRUE(parameters->TlsSkipVerification);
}

} // namespace
} // namespace NYT::NFlow
