#pragma once

#include <yt/core/crypto/proto/crypto.pb.h>
#include <yt/core/misc/ref.h>
#include <yt/core/misc/serialize.h>

#include <array>

#include <util/generic/strbuf.h>

namespace NYT::NCrypto {

////////////////////////////////////////////////////////////////////////////////

typedef std::array<char, 16> TMD5Hash;
typedef std::array<char, 92> TMD5State;

TMD5Hash MD5FromString(TStringBuf data);

class TMD5Hasher
{
public:
    TMD5Hasher();
    explicit TMD5Hasher(const TMD5State& data);

    TMD5Hasher& Append(TStringBuf data);
    TMD5Hasher& Append(const TRef& data);

    TMD5Hash GetDigest();
    TString GetHexDigestLower();
    TString GetHexDigestUpper();

    const TMD5State& GetState() const;

    void Persist(const TStreamPersistenceContext& context);

private:
    //! Erasing openssl struct type... brutally.
    TMD5State State_;
};

////////////////////////////////////////////////////////////////////////////////

typedef std::array<char, 20> TSha1Hash;

TSha1Hash Sha1FromString(TStringBuf data);

class TSha1Hasher
{
public:
    TSha1Hasher();

    TSha1Hasher& Append(TStringBuf data);

    TSha1Hash GetDigest();
    TString GetHexDigestLower();
    TString GetHexDigestUpper();

private:
    std::array<char, 96> CtxStorage_;
};

////////////////////////////////////////////////////////////////////////////////

TString CreateSha256Hmac(const TString& key, const TString& message);
bool ConstantTimeCompare(const TString& trusted, const TString& untrusted);

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

void ToProto(NCrypto::NProto::TMD5Hasher* protoHasher, const std::optional<NYT::NCrypto::TMD5Hasher>& hasher);
void FromProto(std::optional<NYT::NCrypto::TMD5Hasher>* hasher, const NCrypto::NProto::TMD5Hasher& protoHasher);

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCrypto
