#include "public.h"

#include <yt/yt/core/crypto/crypto.h>

#include <util/string/hex.h>

namespace NYT::NS3::NCrypto {

using namespace NYT::NCrypto;

////////////////////////////////////////////////////////////////////////////////

TString Lowercase(const TString& string)
{
    return to_lower(string);
}

TString Hex(const TString& string)
{
    return to_lower(HexEncode(string));
}

TString Sha256HashHex(TSharedRef data)
{
    TSha256Hasher hasher;
    hasher.Append(TStringBuf(data.begin(), data.size()));
    return hasher.GetHexDigestLowerCase();
}

TString Sha256HashHex(const TString& string)
{
    return Sha256HashHex(TSharedRef::FromString(string));
}

TString HmacSha256(const TString& key, const TString& message)
{
    return CreateSha256HmacRaw(key, message);
}

TString Trim(const TString& string)
{
    int start = 0;
    int end = string.size();
    while (start < std::ssize(string) && string[start] == ' ') {
        ++start;
    }
    while (end > start && string[end - 1] == ' ') {
        --end;
    }

    const auto* data = string.Data();
    return TString(data + start, data + end);
}

TString UriEncode(const TString& string, bool isObjectPath)
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

    TString result;
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

TString FormatTimeIso8601(TInstant time)
{
    TString result;
    for (auto byte : time.ToStringUpToSeconds()) {
        if (byte != '-' && byte != ':') {
            result += byte;
        }
    }

    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NS3::NCrypto
