#include <yt/yt/flow/library/cpp/misc/self_signed_certificate.h>

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/bus/bus.h>
#include <yt/yt/core/bus/client.h>
#include <yt/yt/core/bus/message_handler.h>
#include <yt/yt/core/bus/server.h>

#include <yt/yt/core/bus/tcp/client.h>
#include <yt/yt/core/bus/tcp/config.h>
#include <yt/yt/core/bus/tcp/server.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/crypto/config.h>
#include <yt/yt/core/crypto/tls.h>

#include <library/cpp/testing/common/network.h>

#include <openssl/pem.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>

namespace NYT::NFlow {
namespace {

using namespace NBus;
using namespace NConcurrency;
using namespace NCrypto;

////////////////////////////////////////////////////////////////////////////////

TSelfSignedCertificateOptions MakeOptions()
{
    return {
        .CommonName = "yt-flow-controller-test",
        .IPAddresses = {"127.0.0.1", "::1"},
        .DnsNames = {"localhost"},
        .Validity = TDuration::Days(30),
    };
}

TX509Ptr ReadCertificate(const std::string& pem)
{
    TBioPtr bio(BIO_new_mem_buf(pem.data(), pem.size()));
    TX509Ptr certificate(PEM_read_bio_X509(bio.get(), /*x*/ nullptr, /*cb*/ nullptr, /*u*/ nullptr));
    YT_VERIFY(certificate);
    return certificate;
}

TEvpPKeyPtr ReadPrivateKey(const std::string& pem)
{
    TBioPtr bio(BIO_new_mem_buf(pem.data(), pem.size()));
    TEvpPKeyPtr key(PEM_read_bio_PrivateKey(bio.get(), /*x*/ nullptr, /*cb*/ nullptr, /*u*/ nullptr));
    YT_VERIFY(key);
    return key;
}

//! Verifies the certificate for TLS server usage with the certificate itself as the only trust anchor.
int VerifyPinned(const TX509Ptr& certificate, const TX509Ptr& anchor)
{
    std::unique_ptr<X509_STORE, decltype(&X509_STORE_free)> store(X509_STORE_new(), &X509_STORE_free);
    X509_STORE_add_cert(store.get(), anchor.get());
    std::unique_ptr<X509_STORE_CTX, decltype(&X509_STORE_CTX_free)> ctx(X509_STORE_CTX_new(), &X509_STORE_CTX_free);
    X509_STORE_CTX_init(ctx.get(), store.get(), certificate.get(), /*chain*/ nullptr);
    X509_STORE_CTX_set_purpose(ctx.get(), X509_PURPOSE_SSL_SERVER);
    X509_STORE_CTX_set_flags(ctx.get(), X509_V_FLAG_X509_STRICT);
    X509_verify_cert(ctx.get());
    return X509_STORE_CTX_get_error(ctx.get());
}

TPemBlobConfigPtr MakePemBlob(std::string value)
{
    auto blob = New<TPemBlobConfig>();
    blob->Value = std::move(value);
    return blob;
}

class TEmptyBusHandler
    : public IMessageHandler
{
public:
    void HandleMessage(
        TSharedRefArray /*message*/,
        IBusPtr /*replyBus*/,
        IDirectPlacementTransferPtr /*transfer*/,
        TPacketId /*packetId*/) noexcept override
    { }
};

////////////////////////////////////////////////////////////////////////////////

TEST(TSelfSignedCertificateTest, CertificateContent)
{
    auto result = GenerateSelfSignedCertificate(MakeOptions());
    auto certificate = ReadCertificate(result.CertificatePem);
    auto key = ReadPrivateKey(result.PrivateKeyPem);

    EXPECT_EQ(X509_get_version(certificate.get()), 2);
    EXPECT_EQ(EVP_PKEY_id(key.get()), EVP_PKEY_EC);
    EXPECT_EQ(X509_check_private_key(certificate.get(), key.get()), 1);
    EXPECT_EQ(result.CertificateSha256, GetFingerprintSHA256(certificate));
    EXPECT_EQ(ssize(result.CertificateSha256), 64);

    std::string commonName(64, '\0');
    int commonNameSize = X509_NAME_get_text_by_NID(
        X509_get_subject_name(certificate.get()),
        NID_commonName,
        commonName.data(),
        ssize(commonName));
    commonName.resize(commonNameSize);
    EXPECT_EQ(commonName, "yt-flow-controller-test");
    EXPECT_EQ(X509_NAME_cmp(X509_get_subject_name(certificate.get()), X509_get_issuer_name(certificate.get())), 0);

    EXPECT_EQ(X509_check_ca(certificate.get()), 0);
    EXPECT_TRUE(X509_get_extension_flags(certificate.get()) & EXFLAG_BCONS);
    EXPECT_EQ(X509_get_key_usage(certificate.get()), static_cast<ui32>(KU_DIGITAL_SIGNATURE));
    EXPECT_EQ(X509_get_extended_key_usage(certificate.get()), static_cast<ui32>(XKU_SSL_SERVER));

    EXPECT_EQ(X509_check_ip_asc(certificate.get(), "127.0.0.1", /*flags*/ 0), 1);
    EXPECT_EQ(X509_check_ip_asc(certificate.get(), "::1", /*flags*/ 0), 1);
    EXPECT_EQ(X509_check_ip_asc(certificate.get(), "127.0.0.2", /*flags*/ 0), 0);
    EXPECT_EQ(X509_check_host(certificate.get(), "localhost", /*chklen*/ 0, /*flags*/ 0, /*peername*/ nullptr), 1);

    auto now = TInstant::Now();
    int days = 0;
    int seconds = 0;
    ASN1_TIME_diff(&days, &seconds, /*from*/ nullptr, X509_get0_notBefore(certificate.get()));
    auto notBeforeOffset = TDuration::Days(-days) + TDuration::Seconds(-seconds);
    EXPECT_GE(notBeforeOffset, TDuration::Minutes(59));
    EXPECT_LE(notBeforeOffset, TDuration::Minutes(61));
    ASN1_TIME_diff(&days, &seconds, /*from*/ nullptr, X509_get0_notAfter(certificate.get()));
    auto notAfter = now + TDuration::Days(days) + TDuration::Seconds(seconds);
    EXPECT_GE(notAfter, now + TDuration::Days(30) - TDuration::Minutes(1));
    EXPECT_LE(notAfter, now + TDuration::Days(30) + TDuration::Minutes(1));

    EXPECT_EQ(VerifyPinned(certificate, certificate), X509_V_OK);
}

TEST(TSelfSignedCertificateTest, EveryCertificateIsUnique)
{
    auto first = GenerateSelfSignedCertificate(MakeOptions());
    auto second = GenerateSelfSignedCertificate(MakeOptions());
    EXPECT_NE(first.PrivateKeyPem, second.PrivateKeyPem);
    EXPECT_NE(first.CertificateSha256, second.CertificateSha256);

    auto firstCertificate = ReadCertificate(first.CertificatePem);
    auto secondCertificate = ReadCertificate(second.CertificatePem);
    EXPECT_NE(
        ASN1_INTEGER_cmp(X509_get_serialNumber(firstCertificate.get()), X509_get_serialNumber(secondCertificate.get())),
        0);
    EXPECT_EQ(VerifyPinned(firstCertificate, secondCertificate), X509_V_ERR_DEPTH_ZERO_SELF_SIGNED_CERT);
}

TEST(TSelfSignedCertificateTest, InvalidIPAddress)
{
    auto options = MakeOptions();
    options.IPAddresses = {"not-an-ip"};
    EXPECT_THROW(GenerateSelfSignedCertificate(options), TErrorException);
}

////////////////////////////////////////////////////////////////////////////////

class TSelfSignedCertificateBusTest
    : public testing::Test
{
protected:
    NTesting::TPortHolder Port_ = NTesting::GetFreePort();
    TSelfSignedCertificate Certificate_ = GenerateSelfSignedCertificate(MakeOptions());
    IBusServerPtr Server_;

    void SetUp() override
    {
        auto config = NBus::NTcp::TBusServerConfig::CreateTcp(Port_);
        config->EncryptionMode = EEncryptionMode::Required;
        config->VerificationMode = EVerificationMode::None;
        config->CertificateChain = MakePemBlob(Certificate_.CertificatePem);
        config->PrivateKey = MakePemBlob(Certificate_.PrivateKeyPem);
        Server_ = CreateBusServer(config);
        Server_->Start(New<TEmptyBusHandler>());
    }

    void TearDown() override
    {
        WaitFor(Server_->Stop())
            .ThrowOnError();
    }

    TError Connect(EVerificationMode mode, const std::string& pinnedCertificatePem, const std::string& host)
    {
        auto config = NBus::NTcp::TBusClientConfig::CreateTcp(Format("%v:%v", host, static_cast<ui16>(Port_)));
        config->EncryptionMode = EEncryptionMode::Required;
        config->VerificationMode = mode;
        config->CertificateAuthority = MakePemBlob(pinnedCertificatePem);
        auto bus = CreateBusClient(config)->CreateBus(New<TEmptyBusHandler>());
        auto error = WaitFor(bus->GetReadyFuture());
        if (error.IsOK()) {
            EXPECT_TRUE(bus->IsEncrypted());
        }
        return error;
    }
};

TEST_F(TSelfSignedCertificateBusTest, PinnedCertificateIsAccepted)
{
    auto error = Connect(EVerificationMode::Ca, Certificate_.CertificatePem, "127.0.0.1");
    EXPECT_TRUE(error.IsOK()) << ToString(error);
}

TEST_F(TSelfSignedCertificateBusTest, PinnedCertificateIsAcceptedWithAddressCheck)
{
    auto error = Connect(EVerificationMode::Full, Certificate_.CertificatePem, "127.0.0.1");
    EXPECT_TRUE(error.IsOK()) << ToString(error);
}

TEST_F(TSelfSignedCertificateBusTest, OtherCertificateIsRejected)
{
    auto other = GenerateSelfSignedCertificate(MakeOptions());
    auto error = Connect(EVerificationMode::Ca, other.CertificatePem, "127.0.0.1");
    EXPECT_EQ(error.GetCode(), NBus::EErrorCode::SslError) << ToString(error);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NFlow
