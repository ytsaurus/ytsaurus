#pragma once

#include <yt/core/misc/public.h>

#include <yt/core/net/address.h>

namespace NYT {
namespace NAuth {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TDefaultBlackboxServiceConfig)
DECLARE_REFCOUNTED_CLASS(TBlackboxTokenAuthenticatorConfig)
DECLARE_REFCOUNTED_CLASS(TCachingBlackboxTokenAuthenticatorConfig)
DECLARE_REFCOUNTED_CLASS(TCypressTokenAuthenticatorConfig)
DECLARE_REFCOUNTED_CLASS(TCachingCypressTokenAuthenticatorConfig)
DECLARE_REFCOUNTED_CLASS(TBlackboxCookieAuthenticatorConfig)
DECLARE_REFCOUNTED_CLASS(TCachingBlackboxCookieAuthenticatorConfig)
DECLARE_REFCOUNTED_CLASS(TAuthenticationManagerConfig)
DECLARE_REFCOUNTED_CLASS(TAuthenticationManager)

DECLARE_REFCOUNTED_STRUCT(IBlackboxService)
DECLARE_REFCOUNTED_STRUCT(ICookieAuthenticator)
DECLARE_REFCOUNTED_STRUCT(ITokenAuthenticator)

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EBlackboxStatus,
    ((Valid)    (0))
    ((NeedReset)(1))
    ((Expired)  (2))
    ((NoAuth)   (3))
    ((Disabled) (4))
    ((Invalid)  (5))
);

DEFINE_ENUM(EBlackboxException,
    ((Ok)                (0))
    ((Unknown)           (1))
    ((InvalidParameters) (2))
    ((DBFetchFailed)     (9))
    ((DBException)      (10))
    ((AccessDenied)     (21))
);

////////////////////////////////////////////////////////////////////////////////

struct TTokenCredentials
{
    TString Token;
    // NB: UserIP may be ignored for caching purposes.
    NNet::TNetworkAddress UserIP;
};

struct TCookieCredentials
{
    TString SessionId;
    TString SslSessionId;
    NNet::TNetworkAddress UserIP;
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
    return std::tie(lhs.Token) == std::tie(rhs.Token);
}

inline bool operator ==(
    const TCookieCredentials& lhs,
    const TCookieCredentials& rhs)
{
    return std::tie(lhs.SessionId, lhs.SslSessionId, lhs.UserIP) ==
           std::tie(rhs.SessionId, rhs.SslSessionId, rhs.UserIP);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NAuth
} // namespace NYT

template <>
struct hash<NYT::NAuth::TTokenCredentials>
{
    inline size_t operator()(const NYT::NAuth::TTokenCredentials& credentials) const
    {
        size_t result = 0;
        NYT::HashCombine(result, credentials.Token);
        return result;
    }
};

template <>
struct hash<NYT::NAuth::TCookieCredentials>
{
    inline size_t operator()(const NYT::NAuth::TCookieCredentials& credentials) const
    {
        size_t result = 0;
        NYT::HashCombine(result, credentials.SessionId);
        NYT::HashCombine(result, credentials.SslSessionId);
        NYT::HashCombine(result, credentials.UserIP);
        return result;
    }
};
