#pragma once

#include <array>

#include <util/generic/strbuf.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

typedef std::array<char, 16> TMD5Hash;

TMD5Hash MD5FromString(TStringBuf data);

class TMD5Hasher
{
public:
    TMD5Hasher();

    TMD5Hasher& Append(TStringBuf data);

    TMD5Hash Digest();
    TString HexDigestLower();
    TString HexDigestUpper();

private:
    //! Erasing openssl struct type... brutally.
    std::array<char, 92> CtxStorage_;
};

////////////////////////////////////////////////////////////////////////////////

typedef std::array<char, 20> TSHA1Hash;

TSHA1Hash SHA1FromString(TStringBuf data);

class TSHA1Hasher
{
public:
    TSHA1Hasher();

    TSHA1Hasher& Append(TStringBuf data);

    TSHA1Hash Digest();
    TString HexDigestLower();
    TString HexDigestUpper();

private:
    std::array<char, 96> CtxStorage_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
