#include "crypto.h"

#include <yt/core/misc/error.h>

#include <util/string/hex.h>

#include <contrib/libs/openssl/crypto/md5/md5.h>
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

TSHA1Hash SHA1FromString(TStringBuf data)
{
    TSHA1Hash hash;
    if (data.Size() != hash.size()) {
        THROW_ERROR_EXCEPTION("Invalid SHA1 hash size")
            << TErrorAttribute("expected", hash.size())
            << TErrorAttribute("actual", data.Size());
    }

    std::copy(data.begin(), data.end(), hash.begin());
    return hash;
}

static_assert(
    sizeof(SHA_CTX) == sizeof(TSHA1Hasher),
    "TSHA1Hasher size must be exaclty equal to that of SHA1_CTX");

TSHA1Hasher::TSHA1Hasher()
{
    SHA1_Init(reinterpret_cast<SHA_CTX*>(CtxStorage_.data()));
}

TSHA1Hasher& TSHA1Hasher::Append(TStringBuf data)
{
    SHA1_Update(reinterpret_cast<SHA_CTX*>(CtxStorage_.data()), data.Data(), data.Size());
    return *this;
}

TSHA1Hash TSHA1Hasher::GetDigest()
{
    TSHA1Hash hash;
    SHA1_Final(
        reinterpret_cast<unsigned char*>(hash.data()),
        reinterpret_cast<SHA_CTX*>(CtxStorage_.data()));
    return hash;
}

TString TSHA1Hasher::GetHexDigestUpper()
{
    auto sha1 = GetDigest();
    return HexEncode(sha1.data(), sha1.size());
}

TString TSHA1Hasher::GetHexDigestLower()
{
    return to_lower(GetHexDigestUpper());
}

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

void ToProto(NCrypto::NProto::TMD5Hasher* protoHasher, const TNullable<NYT::NCrypto::TMD5Hasher>& hasher)
{
    auto* outputBytes = protoHasher->mutable_state();
    outputBytes->clear();
    if (!hasher) {
        return;
    }

    const auto& state = hasher->GetState();
    outputBytes->assign(state.begin(), state.end());
}

void FromProto(TNullable<NYT::NCrypto::TMD5Hasher>* hasher, const NCrypto::NProto::TMD5Hasher& protoHasher)
{
    const auto& inputBytes = protoHasher.state();
    if (inputBytes.empty()) {
        return;
    }

    TMD5State state;
    std::copy(inputBytes.begin(), inputBytes.end(), state.data());

    hasher->Emplace(state);
}

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

} // namespace NCrypto
} // namespace NYT

