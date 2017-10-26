#pragma once

#include <yt/core/crypto/proto/crypto.pb.h>
#include <yt/core/misc/ref.h>
#include <yt/core/misc/serialize.h>

#include <array>

#include <util/generic/strbuf.h>

namespace NYT {

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

private:
    //! Erasing openssl struct type... brutally.
    TMD5State State_;
};

////////////////////////////////////////////////////////////////////////////////

typedef std::array<char, 20> TSHA1Hash;

TSHA1Hash SHA1FromString(TStringBuf data);

class TSHA1Hasher
{
public:
    TSHA1Hasher();

    TSHA1Hasher& Append(TStringBuf data);

    TSHA1Hash GetDigest();
    TString GetHexDigestLower();
    TString GetHexDigestUpper();

private:
    std::array<char, 96> CtxStorage_;
};

////////////////////////////////////////////////////////////////////////////////

namespace NProto {

void ToProto(NProto::TMD5Hasher* protoHasher, const TNullable<NYT::TMD5Hasher>& hasher);
void FromProto(TNullable<NYT::TMD5Hasher>* hasher, const NProto::TMD5Hasher& protoHasher);

} // namespace NProto

////////////////////////////////////////////////////////////////////////////////

struct TMD5HasherSerializer
{
    template <class C>
    static void Save(C& context, const TMD5Hasher& value)
    {
        using NYT::Save;

        TNullable<TMD5State> state;
        Save(context, value.GetState());
    }

    template <class C>
    static void Load(C& context, TMD5Hasher& value)
    {
        using NYT::Load;

        TNullable<TMD5State> state;
        Load(context, state);
        value = TMD5Hasher(state.Get());
    }
};

template <class C>
struct TSerializerTraits<TMD5Hasher, C, void>
{
    typedef TMD5HasherSerializer TSerializer;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
