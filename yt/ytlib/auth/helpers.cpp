#include "helpers.h"

#include <yt/core/crypto/crypto.h>

#include <util/string/quote.h>
#include <util/string/url.h>

namespace NYT {
namespace NAuth {

////////////////////////////////////////////////////////////////////////////////

TString GetCryptoHash(TStringBuf secret)
{
    return NCrypto::TSha1Hasher()
        .Append(secret)
        .GetHexDigestLower();
}

////////////////////////////////////////////////////////////////////////////////

static const THashSet<TString> PrivateUrlParams{
    "userip",
    "oauth_token",
    "sessionid",
    "sslsessionid"
};

void TSafeUrlBuilder::AppendString(TStringBuf str)
{
    RealUrl_.AppendString(str);
    SafeUrl_.AppendString(str);
}

void TSafeUrlBuilder::AppendChar(char ch)
{
    RealUrl_.AppendChar(ch);
    SafeUrl_.AppendChar(ch);
}

void TSafeUrlBuilder::AppendParam(TStringBuf key, TStringBuf value)
{
    auto size = key.length() + 4 + CgiEscapeBufLen(value.length());

    char* realBegin = RealUrl_.Preallocate(size);
    char* realIt = realBegin;
    memcpy(realIt, key.c_str(), key.length());
    realIt += key.length();
    *realIt = '=';
    realIt += 1;
    auto realEnd = CGIEscape(realIt, value.c_str(), value.length());
    RealUrl_.Advance(realEnd - realBegin);

    char* safeBegin = SafeUrl_.Preallocate(size);
    char* safeEnd = safeBegin;
    if (PrivateUrlParams.has(key)) {
        memcpy(safeEnd, realBegin, realIt - realBegin);
        safeEnd += realIt - realBegin;
        memcpy(safeEnd, "***", 3);
        safeEnd += 3;
    } else {
        memcpy(safeEnd, realBegin, realEnd - realBegin);
        safeEnd += realEnd - realBegin;
    }
    SafeUrl_.Advance(safeEnd - safeBegin);
}

TString TSafeUrlBuilder::FlushRealUrl()
{
    return RealUrl_.Flush();
}

TString TSafeUrlBuilder::FlushSafeUrl()
{
    return SafeUrl_.Flush();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NAuth
} // namespace NYT

