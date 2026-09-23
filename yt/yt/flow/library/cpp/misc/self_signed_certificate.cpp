#include "self_signed_certificate.h"

#include <yt/yt/core/crypto/tls.h>

#include <yt/yt/core/misc/error.h>

#include <openssl/bn.h>
#include <openssl/evp.h>
#include <openssl/obj_mac.h>
#include <openssl/pem.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>

#include <memory>

namespace NYT::NFlow {

using namespace NCrypto;

////////////////////////////////////////////////////////////////////////////////

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TOpenSslDeleter
{
    void operator()(EVP_PKEY_CTX* ctx) const noexcept
    {
        EVP_PKEY_CTX_free(ctx);
    }

    void operator()(BIGNUM* number) const noexcept
    {
        BN_free(number);
    }

    void operator()(GENERAL_NAMES* names) const noexcept
    {
        GENERAL_NAMES_free(names);
    }
};

// Big enough to make serials unique, small enough to fit the 20-octet limit of RFC 5280.
constexpr int SerialBits = 159;

TEvpPKeyPtr GenerateKey()
{
    std::unique_ptr<EVP_PKEY_CTX, TOpenSslDeleter> ctx(EVP_PKEY_CTX_new_id(EVP_PKEY_EC, /*engine*/ nullptr));
    if (!ctx ||
        EVP_PKEY_keygen_init(ctx.get()) <= 0 ||
        EVP_PKEY_CTX_set_ec_paramgen_curve_nid(ctx.get(), NID_X9_62_prime256v1) <= 0 ||
        EVP_PKEY_CTX_set_ec_param_enc(ctx.get(), OPENSSL_EC_NAMED_CURVE) <= 0)
    {
        THROW_ERROR GetLastSslError("Failed to set up EC key generation");
    }

    EVP_PKEY* key = nullptr;
    if (EVP_PKEY_keygen(ctx.get(), &key) <= 0) {
        THROW_ERROR GetLastSslError("Failed to generate EC key");
    }
    return TEvpPKeyPtr(key);
}

void AddExtension(X509* certificate, int nid, const char* value)
{
    X509V3_CTX ctx;
    X509V3_set_ctx_nodb(&ctx);
    X509V3_set_ctx(&ctx, certificate, certificate, /*req*/ nullptr, /*crl*/ nullptr, /*flags*/ 0);
    auto* extension = X509V3_EXT_conf_nid(/*conf*/ nullptr, &ctx, nid, value);
    if (!extension) {
        THROW_ERROR GetLastSslError(Format("Failed to create certificate extension %v", OBJ_nid2sn(nid)));
    }
    int result = X509_add_ext(certificate, extension, /*loc*/ -1);
    X509_EXTENSION_free(extension);
    if (result != 1) {
        THROW_ERROR GetLastSslError(Format("Failed to add certificate extension %v", OBJ_nid2sn(nid)));
    }
}

void AddSubjectAltNames(X509* certificate, const TSelfSignedCertificateOptions& options)
{
    std::unique_ptr<GENERAL_NAMES, TOpenSslDeleter> names(sk_GENERAL_NAME_new_null());
    auto addName = [&] (int type, const std::string& value) {
        auto* name = a2i_GENERAL_NAME(/*out*/ nullptr, /*method*/ nullptr, /*ctx*/ nullptr, type, value.c_str(), /*isNc*/ 0);
        if (!name) {
            THROW_ERROR GetLastSslError(Format("Invalid subject alternative name %Qv", value));
        }
        sk_GENERAL_NAME_push(names.get(), name);
    };
    for (const auto& address : options.IPAddresses) {
        addName(GEN_IPADD, address);
    }
    for (const auto& dnsName : options.DnsNames) {
        addName(GEN_DNS, dnsName);
    }
    if (sk_GENERAL_NAME_num(names.get()) == 0) {
        return;
    }
    if (X509_add1_ext_i2d(certificate, NID_subject_alt_name, names.get(), /*crit*/ 0, X509V3_ADD_DEFAULT) != 1) {
        THROW_ERROR GetLastSslError("Failed to add subject alternative names");
    }
}

std::string WritePem(const std::function<int(BIO*)>& write)
{
    TBioPtr bio(BIO_new(BIO_s_mem()));
    if (!bio || write(bio.get()) != 1) {
        THROW_ERROR GetLastSslError("Failed to write PEM");
    }
    char* data = nullptr;
    auto size = BIO_get_mem_data(bio.get(), &data);
    return std::string(data, size);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace

////////////////////////////////////////////////////////////////////////////////

TSelfSignedCertificate GenerateSelfSignedCertificate(const TSelfSignedCertificateOptions& options)
{
    auto key = GenerateKey();

    TX509Ptr certificate(X509_new());
    if (!certificate || X509_set_version(certificate.get(), 2) != 1) {
        THROW_ERROR GetLastSslError("Failed to create X.509 certificate");
    }

    std::unique_ptr<BIGNUM, TOpenSslDeleter> serial(BN_new());
    if (!serial ||
        BN_rand(serial.get(), SerialBits, BN_RAND_TOP_ONE, BN_RAND_BOTTOM_ANY) != 1 ||
        !BN_to_ASN1_INTEGER(serial.get(), X509_get_serialNumber(certificate.get())))
    {
        THROW_ERROR GetLastSslError("Failed to set certificate serial number");
    }

    auto* name = X509_get_subject_name(certificate.get());
    if (X509_NAME_add_entry_by_txt(
            name,
            "CN",
            MBSTRING_UTF8,
            reinterpret_cast<const unsigned char*>(options.CommonName.data()),
            ssize(options.CommonName),
            /*loc*/ -1,
            /*set*/ 0) != 1 ||
        X509_set_issuer_name(certificate.get(), name) != 1)
    {
        THROW_ERROR GetLastSslError(Format("Failed to set certificate common name %Qv", options.CommonName));
    }

    if (!X509_gmtime_adj(X509_getm_notBefore(certificate.get()), -static_cast<long>(options.ClockSkew.Seconds())) ||
        !X509_time_adj_ex(
            X509_getm_notAfter(certificate.get()),
            static_cast<int>(options.Validity.Days()),
            static_cast<long>(options.Validity.Seconds() % TDuration::Days(1).Seconds()),
            /*inTm*/ nullptr))
    {
        THROW_ERROR GetLastSslError("Failed to set certificate validity");
    }

    if (X509_set_pubkey(certificate.get(), key.get()) != 1) {
        THROW_ERROR GetLastSslError("Failed to set certificate public key");
    }

    AddExtension(certificate.get(), NID_basic_constraints, "critical,CA:FALSE");
    // keyEncipherment is RSA-only; RFC 5480 forbids it for EC keys.
    AddExtension(certificate.get(), NID_key_usage, "critical,digitalSignature");
    AddExtension(certificate.get(), NID_ext_key_usage, "serverAuth");
    AddSubjectAltNames(certificate.get(), options);

    if (X509_sign(certificate.get(), key.get(), EVP_sha256()) <= 0) {
        THROW_ERROR GetLastSslError("Failed to sign certificate");
    }

    return {
        .CertificatePem = WritePem([&] (BIO* bio) {
            return PEM_write_bio_X509(bio, certificate.get());
        }),
        .PrivateKeyPem = WritePem([&] (BIO* bio) {
            return PEM_write_bio_PrivateKey(
                bio,
                key.get(),
                /*enc*/ nullptr,
                /*kstr*/ nullptr,
                /*klen*/ 0,
                /*cb*/ nullptr,
                /*u*/ nullptr);
        }),
        .CertificateSha256 = GetFingerprintSHA256(certificate),
    };
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow
