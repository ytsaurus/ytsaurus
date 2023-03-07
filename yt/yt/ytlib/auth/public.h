#pragma once

#include <yt/core/misc/public.h>

#include <yt/core/net/address.h>

namespace NYT::NAuth {

////////////////////////////////////////////////////////////////////////////////

extern const NProfiling::TProfiler AuthProfiler;

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TDefaultBlackboxServiceConfig)
DECLARE_REFCOUNTED_CLASS(TDefaultTvmServiceConfig)
DECLARE_REFCOUNTED_CLASS(TCachingDefaultTvmServiceConfig)
DECLARE_REFCOUNTED_CLASS(TBlackboxTicketAuthenticatorConfig)
DECLARE_REFCOUNTED_CLASS(TBlackboxTokenAuthenticatorConfig)
DECLARE_REFCOUNTED_CLASS(TCachingBlackboxTokenAuthenticatorConfig)
DECLARE_REFCOUNTED_CLASS(TCypressTokenAuthenticatorConfig)
DECLARE_REFCOUNTED_CLASS(TCachingTokenAuthenticatorConfig)
DECLARE_REFCOUNTED_CLASS(TCachingCypressTokenAuthenticatorConfig)
DECLARE_REFCOUNTED_CLASS(TBlackboxCookieAuthenticatorConfig)
DECLARE_REFCOUNTED_CLASS(TCachingCookieAuthenticatorConfig)
DECLARE_REFCOUNTED_CLASS(TCachingBlackboxCookieAuthenticatorConfig)
DECLARE_REFCOUNTED_CLASS(TDefaultSecretVaultServiceConfig)
DECLARE_REFCOUNTED_CLASS(TBatchingSecretVaultServiceConfig)
DECLARE_REFCOUNTED_CLASS(TCachingSecretVaultServiceConfig)
DECLARE_REFCOUNTED_CLASS(TAuthenticationManagerConfig)
DECLARE_REFCOUNTED_CLASS(TAuthenticationManager)

DECLARE_REFCOUNTED_STRUCT(IBlackboxService)
DECLARE_REFCOUNTED_STRUCT(ITvmService)

DECLARE_REFCOUNTED_STRUCT(ICookieAuthenticator)
DECLARE_REFCOUNTED_STRUCT(ITokenAuthenticator)
DECLARE_REFCOUNTED_STRUCT(ITicketAuthenticator)

DECLARE_REFCOUNTED_STRUCT(ISecretVaultService)

////////////////////////////////////////////////////////////////////////////////

// See https://doc.yandex-team.ru/blackbox/reference/method-sessionid-response-json.xml for reference.
DEFINE_ENUM_WITH_UNDERLYING_TYPE(EBlackboxStatus, i64,
    ((Valid)    (0))
    ((NeedReset)(1))
    ((Expired)  (2))
    ((NoAuth)   (3))
    ((Disabled) (4))
    ((Invalid)  (5))
);

// See https://doc.yandex-team.ru/blackbox/concepts/blackboxErrors.xml
DEFINE_ENUM_WITH_UNDERLYING_TYPE(EBlackboxException, i64,
    ((Ok)                (0))
    ((Unknown)           (1))
    ((InvalidParameters) (2))
    ((DBFetchFailed)     (9))
    ((DBException)      (10))
    ((AccessDenied)     (21))
);

DEFINE_ENUM(ESecretVaultErrorCode,
    ((UnknownError)           (18000))
    ((MalformedResponse)      (18001))
    ((NonexistentEntityError) (18002))
    ((DelegationAccessError)  (18003))
    ((DelegationTokenRevoked) (18004))
    ((UnexpectedStatus)       (18005))
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
    std::optional<TString> SslSessionId;

    NNet::TNetworkAddress UserIP;
};

struct TTicketCredentials
{
    TString Ticket;
};

struct TAuthenticationResult
{
    TString Login;
    TString Realm;
};

inline bool operator ==(
    const TCookieCredentials& lhs,
    const TCookieCredentials& rhs)
{
    return std::tie(lhs.SessionId, lhs.SslSessionId, lhs.UserIP) ==
           std::tie(rhs.SessionId, rhs.SslSessionId, rhs.UserIP);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth

template <>
struct THash<NYT::NAuth::TCookieCredentials>
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
