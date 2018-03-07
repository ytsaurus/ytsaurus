#pragma once

#include <yt/core/misc/hash.h>
#include <yt/core/misc/ref_counted.h>

#include <util/generic/string.h>

namespace NYT {
namespace NBlackbox {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TDefaultBlackboxServiceConfig)
DECLARE_REFCOUNTED_CLASS(TTokenAuthenticatorConfig)
DECLARE_REFCOUNTED_CLASS(TCachingTokenAuthenticatorConfig)
DECLARE_REFCOUNTED_CLASS(TCookieAuthenticatorConfig)
DECLARE_REFCOUNTED_CLASS(TCachingCookieAuthenticatorConfig)

DECLARE_REFCOUNTED_STRUCT(IBlackboxService)

DECLARE_REFCOUNTED_STRUCT(ICookieAuthenticator)
DECLARE_REFCOUNTED_STRUCT(ITokenAuthenticator)

struct TTokenCredentials
{
    TString Token;
    TString UserIP;
};

struct TCookieCredentials
{
    TString SessionId;
    TString SslSessionId;
    TString Host;
    TString UserIP;
};

struct TAuthenticationResult
{
    TString Login;
    TString Realm;
};

inline bool operator ==(
    const TTokenCredentials& lhs,
    const TTokenCredentials& rhs)
{
    return std::tie(lhs.Token, lhs.UserIP) == std::tie(rhs.Token, rhs.UserIP);
}

inline bool operator ==(
    const TCookieCredentials& lhs,
    const TCookieCredentials& rhs)
{
    return std::tie(lhs.SessionId, lhs.SslSessionId, lhs.Host, lhs.UserIP) ==
           std::tie(rhs.SessionId, rhs.SslSessionId, rhs.Host, rhs.UserIP);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NBlackbox
} // namespace NYT

template <>
struct hash<NYT::NBlackbox::TTokenCredentials>
{
    inline size_t operator()(const NYT::NBlackbox::TTokenCredentials& credentials) const
    {
        size_t result = 0;
        NYT::HashCombine(result, credentials.Token);
        NYT::HashCombine(result, credentials.UserIP);
        return result;
    }
};

template <>
struct hash<NYT::NBlackbox::TCookieCredentials>
{
    inline size_t operator()(const NYT::NBlackbox::TCookieCredentials& credentials) const
    {
        size_t result = 0;
        NYT::HashCombine(result, credentials.SessionId);
        NYT::HashCombine(result, credentials.SslSessionId);
        NYT::HashCombine(result, credentials.Host);
        NYT::HashCombine(result, credentials.UserIP);
        return result;
    }
};
