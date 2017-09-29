#include "crypto.h"

#include <yt/core/misc/error.h>

#include <util/string/hex.h>

#include <contrib/libs/openssl/crypto/md5/md5.h>
#include <contrib/libs/openssl/crypto/sha/sha.h>

namespace NYT {

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
    MD5_Init(reinterpret_cast<MD5_CTX*>(CtxStorage_.data()));
}

TMD5Hasher& TMD5Hasher::Append(TStringBuf data)
{
    MD5_Update(reinterpret_cast<MD5_CTX*>(CtxStorage_.data()), data.Data(), data.Size());
    return *this;
}

TMD5Hash TMD5Hasher::Digest()
{
    TMD5Hash hash;
    MD5_Final(
        reinterpret_cast<unsigned char*>(hash.data()),
        reinterpret_cast<MD5_CTX*>(CtxStorage_.data()));
    return hash;
}

TString TMD5Hasher::HexDigestUpper()
{
    auto md5 = Digest();
    return HexEncode(md5.data(), md5.size());
}

TString TMD5Hasher::HexDigestLower()
{
    return to_lower(HexDigestUpper());
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

TSHA1Hash TSHA1Hasher::Digest()
{
    TSHA1Hash hash;
    SHA1_Final(
        reinterpret_cast<unsigned char*>(hash.data()),
        reinterpret_cast<SHA_CTX*>(CtxStorage_.data()));
    return hash;
}

TString TSHA1Hasher::HexDigestUpper()
{
    auto sha1 = Digest();
    return HexEncode(sha1.data(), sha1.size());
}

TString TSHA1Hasher::HexDigestLower()
{
    return to_lower(HexDigestUpper());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

