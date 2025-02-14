#include "helpers.h"
#include "config.h"

#include <yt/yt/library/re2/re2.h>

#include <yt/yt/core/crypto/crypto.h>

#include <yt/yt/core/ytree/fluent.h>

#include <yt/yt_proto/yt/core/rpc/proto/rpc.pb.h>

#include <library/cpp/string_utils/quote/quote.h>
#include <library/cpp/string_utils/url/url.h>

#include <util/string/split.h>

namespace NYT::NAuth {

using namespace NCrypto;
using namespace NLogging;
using namespace NYson;
using namespace NYTree;
using namespace NNet;
using namespace NRpc::NProto;

////////////////////////////////////////////////////////////////////////////////

TString GetCryptoHash(TStringBuf secret)
{
    return NCrypto::TSha1Hasher()
        .Append(secret)
        .GetHexDigestLowerCase();
}

TString FormatUserIP(const TNetworkAddress& address)
{
    if (!address.IsIP()) {
        // Sometimes userIP is missing (e.g. user is connecting
        // from job using unix socket), but it is required by
        // Blackbox. Put placeholder in place of a real IP.
        static const TString LocalUserIP = "127.0.0.1";
        return LocalUserIP;
    }
    return ToString(
        address,
        TNetworkAddressFormatOptions{
            .IncludePort = false,
            .IncludeTcpProtocol = false,
        });
}

TString GetBlackboxCacheKeyFactorFromUserIP(
    EBlackboxCacheKeyMode mode,
    const TNetworkAddress& address)
{
    if (mode == EBlackboxCacheKeyMode::Credentials) {
        return TString();
    }

    if (mode == EBlackboxCacheKeyMode::CredentialsAndUserAddressProjectId &&
        address.IsIP6() &&
        address.ToIP6Address().IsMtn())
    {
        return Format("project_id=%v", TMtnAddress(address.ToIP6Address()).GetProjectId());
    }

    return Format("ip=%v", ToString(address));
}

TString GetLoginForTvmId(TTvmId tvmId)
{
    return Format("tvm:%v", tvmId);
}

////////////////////////////////////////////////////////////////////////////////

static const THashSet<TString> PrivateUrlParams{
    "userip",
    "oauth_token",
    "sessionid",
    "sslsessionid",
    "user_ticket",
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
    memcpy(realIt, key.data(), key.length());
    realIt += key.length();
    *realIt = '=';
    realIt += 1;
    auto realEnd = CGIEscape(realIt, value.data(), value.length());
    RealUrl_.Advance(realEnd - realBegin);

    char* safeBegin = SafeUrl_.Preallocate(size);
    char* safeEnd = safeBegin;
    if (PrivateUrlParams.contains(key)) {
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

THashedCredentials HashCredentials(const NRpc::NProto::TCredentialsExt& credentialsExt)
{
    THashedCredentials result;
    if (credentialsExt.has_token()) {
        result.TokenHash = GetCryptoHash(credentialsExt.token());
    }
    return result;
}

void Serialize(const THashedCredentials& hashedCredentials, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .OptionalItem("token_hash", hashedCredentials.TokenHash)
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

TString SignCsrfToken(const std::string& userId, const TString& key, TInstant now)
{
    auto msg = userId + ":" + ToString(now.TimeT());
    return CreateSha256Hmac(key, msg) + ":" + ToString(now.TimeT());
}

TError CheckCsrfToken(
    const TString& csrfToken,
    const std::string& userId,
    const TString& key,
    TInstant expirationTime)
{
    std::vector<TString> parts;
    StringSplitter(csrfToken).Split(':').AddTo(&parts);
    if (parts.size() != 2) {
        return TError("Malformed CSRF token");
    }

    auto signTime = TInstant::Seconds(FromString<time_t>(parts[1]));
    if (signTime < expirationTime) {
        return TError(NRpc::EErrorCode::InvalidCsrfToken, "CSRF token expired")
            << TErrorAttribute("sign_time", signTime);
    }

    auto msg = userId + ":" + ToString(signTime.TimeT());
    auto expectedToken = CreateSha256Hmac(key, msg);
    if (!ConstantTimeCompare(expectedToken, parts[0])) {
        return TError(NRpc::EErrorCode::InvalidCsrfToken, "Invalid CSFR token signature")
            << TErrorAttribute("provided_signature", parts[0])
            << TErrorAttribute("user_fingerprint", msg);
    }

    return {};
}

////////////////////////////////////////////////////////////////////////////////

TString ApplyStringReplacement(const TString& input, const TStringReplacementConfigPtr& replacement, const TLogger& logger)
{
    const auto& Logger = logger;

    auto output = input;

    // Config validation guarantees that exactly one of the transformation types is requested.

    int regexReplacementCount = 0;
    if (replacement->MatchPattern) {
        regexReplacementCount = RE2::GlobalReplace(
            &output,
            *replacement->MatchPattern,
            replacement->Replacement);
    }

    if (replacement->ToLower || replacement->ToUpper) {
        std::transform(output.cbegin(), output.cend(), output.begin(), [&replacement] (auto c) {
            return replacement->ToLower ? std::tolower(c) : std::toupper(c);
        });
    }

    YT_LOG_DEBUG(
        "Login transformation for OAuth user info applied (Login: %v -> %v, RegexReplacementCount: %v)",
        input,
        output,
        regexReplacementCount);

    return output;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth

