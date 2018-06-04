#include "cookie_authenticator.h"
#include "blackbox_service.h"
#include "helpers.h"
#include "private.h"

#include <yt/core/misc/async_expiring_cache.h>

#include <yt/core/crypto/crypto.h>

#include <yt/core/rpc/authenticator.h>

#include <util/string/split.h>

namespace NYT {
namespace NAuth {

using namespace NYTree;
using namespace NYPath;
using namespace NCrypto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = AuthLogger;

////////////////////////////////////////////////////////////////////////////////

// TODO(sandello): Indicate to end-used that cookie must be resigned.
class TBlackboxCookieAuthenticator
    : public ICookieAuthenticator
{
public:
    TBlackboxCookieAuthenticator(
        TBlackboxCookieAuthenticatorConfigPtr config,
        IBlackboxServicePtr blackbox)
        : Config_(std::move(config))
        , Blackbox_(std::move(blackbox))
    { }

    virtual TFuture<TAuthenticationResult> Authenticate(
        const TCookieCredentials& credentials) override
    {
        auto sessionIdMD5 = TMD5Hasher().Append(credentials.SessionId).GetHexDigestUpper();
        auto sslSessionIdMD5 = TMD5Hasher().Append(credentials.SslSessionId).GetHexDigestUpper();
        LOG_DEBUG(
            "Authenticating user via session cookie (SessionIdMD5: %v, SslSessionIdMD5: %v)",
            sessionIdMD5,
            sslSessionIdMD5);
        return Blackbox_->Call("sessionid", {
                {"sessionid", credentials.SessionId},
                {"sslsessionid", credentials.SslSessionId},
                {"host", credentials.Host},
                {"userip", credentials.UserIP}})
            .Apply(BIND(
                &TBlackboxCookieAuthenticator::OnCallResult,
                MakeStrong(this),
                std::move(sessionIdMD5),
                std::move(sslSessionIdMD5)));
    }

private:
    const TBlackboxCookieAuthenticatorConfigPtr Config_;
    const IBlackboxServicePtr Blackbox_;

private:
    TFuture<TAuthenticationResult> OnCallResult(
        const TString& sessionIdMD5,
        const TString& sslSessionIdMD5,
        const INodePtr& data)
    {
        auto result = OnCallResultImpl(data);
        if (!result.IsOK()) {
            LOG_DEBUG(result, "Authentication failed (SessionIdMD5: %v, SslSessionIdMD5: %v)", sessionIdMD5, sslSessionIdMD5);
            result.Attributes().Set("sessionid_md5", sessionIdMD5);
            result.Attributes().Set("sslsessionid_md5", sslSessionIdMD5);
        } else {
            LOG_DEBUG(
                "Authentication successful (SessionIdMD5: %v, SslSessionIdMD5: %v, Login: %v, Realm: %v)",
                sessionIdMD5,
                sslSessionIdMD5,
                result.Value().Login,
                result.Value().Realm);
        }
        return MakeFuture(result);
    }

    TErrorOr<TAuthenticationResult> OnCallResultImpl(const INodePtr& data)
    {
        // See https://doc.yandex-team.ru/blackbox/reference/method-sessionid-response-json.xml for reference.
        auto statusId = GetByYPath<int>(data, "/status/id");
        if (!statusId.IsOK()) {
            return TError("Blackbox returned invalid response");
        }

        if (statusId.Value() != EBlackboxStatus::Valid && statusId.Value() != EBlackboxStatus::NeedReset) {
            auto error = GetByYPath<TString>(data, "/error");
            auto reason = error.IsOK() ? error.Value() : "unknown";
            return TError("Blackbox rejected session cookie")
                << TErrorAttribute("reason", reason);
        }

        auto login = GetByYPath<TString>(data, "/login");

        // Sanity checks.
        if (!login.IsOK()) {
            return TError("Blackbox returned invalid response") << login;
        }

        TAuthenticationResult result;
        result.Login = login.Value();
        result.Realm = "blackbox:cookie";
        return result;
    }
};

ICookieAuthenticatorPtr CreateBlackboxCookieAuthenticator(
    TBlackboxCookieAuthenticatorConfigPtr config,
    IBlackboxServicePtr blackbox)
{
    return New<TBlackboxCookieAuthenticator>(std::move(config), std::move(blackbox));
}

////////////////////////////////////////////////////////////////////////////////

class TCachingCookieAuthenticator
    : public ICookieAuthenticator
    , private TAsyncExpiringCache<TCookieCredentials, TAuthenticationResult>
{
public:
    TCachingCookieAuthenticator(TAsyncExpiringCacheConfigPtr config, ICookieAuthenticatorPtr cookieAuthenticator)
        : TAsyncExpiringCache(std::move(config))
        , CookieAuthenticator_(std::move(cookieAuthenticator))
    { }

    virtual TFuture<TAuthenticationResult> Authenticate(const TCookieCredentials& credentials) override
    {
        return Get(credentials);
    }

private:
    virtual TFuture<TAuthenticationResult> DoGet(const TCookieCredentials& credentials) override
    {
        return CookieAuthenticator_->Authenticate(credentials);
    }

    const ICookieAuthenticatorPtr CookieAuthenticator_;
};

ICookieAuthenticatorPtr CreateCachingCookieAuthenticator(
    TAsyncExpiringCacheConfigPtr config,
    ICookieAuthenticatorPtr authenticator)
{
    return New<TCachingCookieAuthenticator>(std::move(config), std::move(authenticator));
}

////////////////////////////////////////////////////////////////////////////////

class TCookieAuthenticatorWrapper
    : public NRpc::IAuthenticator
{
public:
    explicit TCookieAuthenticatorWrapper(ICookieAuthenticatorPtr underlying)
        : Underlying_(std::move(underlying))
    { }

    virtual TFuture<NRpc::TAuthenticationResult> Authenticate(const NRpc::NProto::TRequestHeader& header) override
    {
        if (!header.HasExtension(NRpc::NProto::TCredentialsExt::credentials_ext)) {
            return Null;
        }

        const auto& ext = header.GetExtension(NRpc::NProto::TCredentialsExt::credentials_ext);
        if (!ext.has_session_id() && !ext.has_ssl_session_id()) {
            return Null;
        }

        TCookieCredentials credentials;
        credentials.SessionId = ext.session_id();
        credentials.SslSessionId = ext.ssl_session_id();
        credentials.Host = ext.domain();
        credentials.UserIP = ext.user_ip();
        return Underlying_->Authenticate(credentials).Apply(
            BIND([=] (const TAuthenticationResult& authResult) {
                NRpc::TAuthenticationResult rpcResult;
                rpcResult.User = authResult.Login;
                rpcResult.Realm = authResult.Realm;
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

} // namespace NAuth
} // namespace NYT
