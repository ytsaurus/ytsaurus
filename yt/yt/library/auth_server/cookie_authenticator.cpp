#include "cookie_authenticator.h"

#include "config.h"
#include "helpers.h"
#include "private.h"
#include "auth_cache.h"
#include "credentials.h"

#include <yt/yt/core/rpc/authenticator.h>

namespace NYT::NAuth {

using namespace NYTree;
using namespace NYPath;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

struct TCookieAuthenticatorCacheKey
{
    THashMap<TString, TString> Cookies;
    TString UserIPFactor;

    operator size_t() const
    {
        size_t result = 0;

        std::vector<std::pair<TString, TString>> cookies(Cookies.begin(), Cookies.end());
        std::sort(cookies.begin(), cookies.end());
        for (const auto& cookie : cookies) {
            HashCombine(result, cookie);
        }

        HashCombine(result, UserIPFactor);

        return result;
    }

    bool operator == (const TCookieAuthenticatorCacheKey&) const = default;
};

class TCachingCookieAuthenticator
    : public ICookieAuthenticator
    , private TAuthCache<TCookieAuthenticatorCacheKey, TAuthenticationResult, NNet::TNetworkAddress>
{
public:
    TCachingCookieAuthenticator(
        TCachingCookieAuthenticatorConfigPtr config,
        ICookieAuthenticatorPtr underlying,
        NProfiling::TProfiler profiler)
        : TAuthCache(config->Cache, std::move(profiler))
        , Config_(std::move(config))
        , UnderlyingAuthenticator_(std::move(underlying))
    { }

    const std::vector<TStringBuf>& GetCookieNames() const override
    {
        return UnderlyingAuthenticator_->GetCookieNames();
    }

    bool CanAuthenticate(const TCookieCredentials& credentials) const override
    {
        return UnderlyingAuthenticator_->CanAuthenticate(credentials);
    }

    TFuture<TAuthenticationResult> Authenticate(const TCookieCredentials& credentials) override
    {
        return Get(
            TCookieAuthenticatorCacheKey{credentials.Cookies, GetBlackboxCacheKeyFactorFromUserIP(Config_->CacheKeyMode, credentials.UserIP)},
            credentials.UserIP);
    }

private:
    const TCachingCookieAuthenticatorConfigPtr Config_;
    const ICookieAuthenticatorPtr UnderlyingAuthenticator_;

    TFuture<TAuthenticationResult> DoGet(
        const TCookieAuthenticatorCacheKey& key,
        const NNet::TNetworkAddress& userIP) noexcept override
    {
        return UnderlyingAuthenticator_->Authenticate(TCookieCredentials{key.Cookies, userIP});
    }
};

////////////////////////////////////////////////////////////////////////////////

ICookieAuthenticatorPtr CreateCachingCookieAuthenticator(
    TCachingCookieAuthenticatorConfigPtr config,
    ICookieAuthenticatorPtr authenticator,
    NProfiling::TProfiler profiler)
{
    return New<TCachingCookieAuthenticator>(
        std::move(config),
        std::move(authenticator),
        std::move(profiler));
}

////////////////////////////////////////////////////////////////////////////////

class TCompositeCookieAuthenticator
    : public ICookieAuthenticator
{
public:
    explicit TCompositeCookieAuthenticator(std::vector<ICookieAuthenticatorPtr> authenticators)
        : Authenticators_(std::move(authenticators))
    {
        for (const auto& authenticator : Authenticators_) {
            const auto& cookieNames = authenticator->GetCookieNames();
            CookieNames_.insert(CookieNames_.end(), cookieNames.begin(), cookieNames.end());
        }
    }

    const std::vector<TStringBuf>& GetCookieNames() const override
    {
        return CookieNames_;
    }

    bool CanAuthenticate(const TCookieCredentials& credentials) const override
    {
        for (const auto& authenticator : Authenticators_) {
            if (authenticator->CanAuthenticate(credentials)) {
                return true;
            }
        }

        return false;
    }

    TFuture<TAuthenticationResult> Authenticate(
        const TCookieCredentials& credentials) override
    {
        for (const auto& authenticator : Authenticators_) {
            if (authenticator->CanAuthenticate(credentials)) {
                TCookieCredentials filteredCredentials{
                    .UserIP = credentials.UserIP,
                };
                const auto& cookies = credentials.Cookies;
                for (const auto& cookie : authenticator->GetCookieNames()) {
                    auto cookieIt = cookies.find(cookie);
                    if (cookieIt != cookies.end()) {
                        EmplaceOrCrash(filteredCredentials.Cookies, cookie, cookieIt->second);
                    }
                }

                return authenticator->Authenticate(filteredCredentials);
            }
        }

        YT_ABORT();
    }

private:
    const std::vector<ICookieAuthenticatorPtr> Authenticators_;

    std::vector<TStringBuf> CookieNames_;
};

////////////////////////////////////////////////////////////////////////////////

ICookieAuthenticatorPtr CreateCompositeCookieAuthenticator(
    std::vector<ICookieAuthenticatorPtr> authenticators)
{
    return New<TCompositeCookieAuthenticator>(std::move(authenticators));
}

////////////////////////////////////////////////////////////////////////////////

class TCookieAuthenticatorWrapper
    : public NRpc::IAuthenticator
{
public:
    explicit TCookieAuthenticatorWrapper(ICookieAuthenticatorPtr underlying)
        : Underlying_(std::move(underlying))
    {
        YT_VERIFY(Underlying_);
    }

    bool CanAuthenticate(const NRpc::TAuthenticationContext& context) override
    {
        if (!context.Header->HasExtension(NRpc::NProto::TCredentialsExt::credentials_ext)) {
            return false;
        }

        const auto& ext = context.Header->GetExtension(NRpc::NProto::TCredentialsExt::credentials_ext);
        if (!ext.has_session_id() && !ext.has_ssl_session_id()) {
            return false;
        }

        return context.UserIP.IsIP4() || context.UserIP.IsIP6();
    }

    TFuture<NRpc::TAuthenticationResult> AsyncAuthenticate(
        const NRpc::TAuthenticationContext& context) override
    {
        YT_ASSERT(CanAuthenticate(context));
        const auto& ext = context.Header->GetExtension(NRpc::NProto::TCredentialsExt::credentials_ext);
        TCookieCredentials credentials;
        auto& cookies = credentials.Cookies;
        cookies[BlackboxSessionIdCookieName] = ext.session_id();
        cookies[BlackboxSslSessionIdCookieName] = ext.ssl_session_id();
        credentials.UserIP = context.UserIP;
        return Underlying_->Authenticate(credentials).Apply(
            BIND([=] (const TAuthenticationResult& authResult) {
                NRpc::TAuthenticationResult rpcResult;
                rpcResult.User = authResult.Login;
                rpcResult.Realm = authResult.Realm;
                rpcResult.UserTicket = authResult.UserTicket;
                return rpcResult;
            }));
    }
private:
    const ICookieAuthenticatorPtr Underlying_;
};

NRpc::IAuthenticatorPtr CreateCookieAuthenticatorWrapper(ICookieAuthenticatorPtr underlying)
{
    return New<TCookieAuthenticatorWrapper>(std::move(underlying));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
