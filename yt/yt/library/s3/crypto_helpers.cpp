#include "public.h"

#include <yt/yt/core/crypto/crypto.h>

#include <util/string/hex.h>

namespace NYT::NS3::NCrypto {

using namespace NYT::NCrypto;

////////////////////////////////////////////////////////////////////////////////

std::string Lowercase(const std::string& string)
{
    return to_lower(TString(string));
}

std::string Hex(const std::string& string)
{
    return to_lower(HexEncode(string));
}

std::string Sha256HashHex(TSharedRef data)
{
    TSha256Hasher hasher;
    hasher.Append(TStringBuf(data.begin(), data.size()));
    return hasher.GetHexDigestLowerCase();
}

std::string Sha256HashHex(const std::string& string)
{
    return Sha256HashHex(TSharedRef::FromString(string));
}

std::string HmacSha256(TStringBuf key, TStringBuf message)
{
    return CreateSha256HmacRaw(key, message);
}

std::string Trim(const std::string& string)
{
    return std::string(NYT::Trim(string));
}

std::string UriEncode(const std::string& string, bool isObjectPath)
{
    auto shouldEncode = [&] (char byte) {
        if (byte >= 'A' && byte <= 'Z') {
            return false;
        }
        if (byte >= 'a' && byte <= 'z') {
            return false;
        }
        if (byte >= '0' && byte <= '9') {
            return false;
        }
        if (byte == '-' || byte == '.' || byte == '_' || byte == '~') {
            return false;
        }
        if (isObjectPath && byte == '/') {
            return false;
        }

        return true;
    };

    std::string result;
    result.reserve(string.size());

    for (char byte : string) {
        if (shouldEncode(byte)) {
            result += '%';
            result += DigitToChar(byte >> 4);
            result += DigitToChar(byte & 15);
        } else {
            result += byte;
        }
    }

    return result;
}

std::string FormatTimeIso8601(TInstant time)
{
    std::string result;
    for (auto byte : time.ToStringUpToSeconds()) {
        if (byte != '-' && byte != ':') {
            result += byte;
        }
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NS3::NCrypto
