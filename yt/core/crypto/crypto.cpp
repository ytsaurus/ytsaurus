#include "crypto.h"

#include <yt/core/misc/error.h>

#include <util/string/hex.h>

#include <contrib/libs/openssl/crypto/hmac/hmac.h>
#include <contrib/libs/openssl/crypto/md5/md5.h>
#include <contrib/libs/openssl/crypto/evp/evp.h>
#include <contrib/libs/openssl/crypto/sha/sha.h>

namespace NYT {
namespace NCrypto {

////////////////////////////////////////////////////////////////////////////////

TMD5Hash MD5FromString(TStringBuf data)
{
    TMD5Hash hash;
    if (data.Size() != hash.size()) {
        THROW_ERROR_EXCEPTION("Invalid MD5 hash size")
            << TErrorAttribute("expected", hash.size())
            << TErrorAttribute("actual", data.Size());
    }

    std::copy(data.begin(), data.end(), hash.begin());
    return hash;
}

static_assert(
    sizeof(MD5_CTX) == sizeof(TMD5Hasher),
    "TMD5Hasher size must be exaclty equal to that of MD5_CTX");

TMD5Hasher::TMD5Hasher()
{
    MD5_Init(reinterpret_cast<MD5_CTX*>(State_.data()));
}

TMD5Hasher::TMD5Hasher(const TMD5State& data)
{
    State_ = data;
}

TMD5Hasher& TMD5Hasher::Append(TStringBuf data)
{
    MD5_Update(reinterpret_cast<MD5_CTX*>(State_.data()), data.Data(), data.Size());
    return *this;
}

TMD5Hasher& TMD5Hasher::Append(const TRef& data)
{
    MD5_Update(reinterpret_cast<MD5_CTX*>(State_.data()), data.Begin(), data.Size());
    return *this;
}

TMD5Hash TMD5Hasher::GetDigest()
{
    TMD5Hash hash;
    MD5_Final(
        reinterpret_cast<unsigned char*>(hash.data()),
        reinterpret_cast<MD5_CTX*>(State_.data()));
    return hash;
}

TString TMD5Hasher::GetHexDigestUpper()
{
    auto md5 = GetDigest();
    return HexEncode(md5.data(), md5.size());
}

TString TMD5Hasher::GetHexDigestLower()
{
    return to_lower(GetHexDigestUpper());
}

void TMD5Hasher::Persist(const TStreamPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, State_);
}

const TMD5State& TMD5Hasher::GetState() const
{
    return State_;
}

////////////////////////////////////////////////////////////////////////////////

TSha1Hash Sha1FromString(TStringBuf data)
{
    TSha1Hash hash;
    if (data.Size() != hash.size()) {
        THROW_ERROR_EXCEPTION("Invalid Sha1 hash size")
            << TErrorAttribute("expected", hash.size())
            << TErrorAttribute("actual", data.Size());
    }

    std::copy(data.begin(), data.end(), hash.begin());
    return hash;
}

static_assert(
    sizeof(SHA_CTX) == sizeof(TSha1Hasher),
    "TSha1Hasher size must be exaclty equal to that of SHA1_CTX");

TSha1Hasher::TSha1Hasher()
{
    SHA1_Init(reinterpret_cast<SHA_CTX*>(CtxStorage_.data()));
}

TSha1Hasher& TSha1Hasher::Append(TStringBuf data)
{
    SHA1_Update(reinterpret_cast<SHA_CTX*>(CtxStorage_.data()), data.Data(), data.Size());
    return *this;
}

TSha1Hash TSha1Hasher::GetDigest()
{
    TSha1Hash hash;
    SHA1_Final(
        reinterpret_cast<unsigned char*>(hash.data()),
        reinterpret_cast<SHA_CTX*>(CtxStorage_.data()));
    return hash;
}

TString TSha1Hasher::GetHexDigestUpper()
{
    auto sha1 = GetDigest();
    return HexEncode(sha1.data(), sha1.size());
}

TString TSha1Hasher::GetHexDigestLower()
{
    return to_lower(GetHexDigestUpper());
}

////////////////////////////////////////////////////////////////////////////////

TString CreateSha256Hmac(const TString& key, const TString& message)
{
    std::array<char, 256 / 8> hmac;
    unsigned int opensslIsInsane;
    auto result = HMAC(
        EVP_sha256(),
        key.data(),
        key.size(),
        reinterpret_cast<const unsigned char*>(message.data()),
        message.size(),
        reinterpret_cast<unsigned char*>(hmac.data()),
        &opensslIsInsane);
    YCHECK(nullptr != result);
    return to_lower(HexEncode(hmac.data(), hmac.size()));
}

bool ConstantTimeCompare(const TString& trusted, const TString& untrusted)
{
    int total = 0;

    size_t i = 0;
    size_t j = 0;
    while (true) {
        total |= trusted[i] ^ untrusted[j];

        if (i == untrusted.size()) {
            break;
        }

        ++i;

        if (j < trusted.size()) {
            ++j;
        } else {
            total |= 1;
        }

    }

    return total == 0;
}

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

void ToProto(NCrypto::NProto::TMD5Hasher* protoHasher, const std::optional<NYT::NCrypto::TMD5Hasher>& hasher)
{
    auto* outputBytes = protoHasher->mutable_state();
    outputBytes->clear();
    if (!hasher) {
        return;
    }

    const auto& state = hasher->GetState();
    outputBytes->assign(state.begin(), state.end());
}

void FromProto(std::optional<NYT::NCrypto::TMD5Hasher>* hasher, const NCrypto::NProto::TMD5Hasher& protoHasher)
{
    const auto& inputBytes = protoHasher.state();
    if (inputBytes.empty()) {
        return;
    }

    TMD5State state;
    std::copy(inputBytes.begin(), inputBytes.end(), state.data());

    hasher->emplace(state);
}

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

} // namespace NCrypto
} // namespace NYT

