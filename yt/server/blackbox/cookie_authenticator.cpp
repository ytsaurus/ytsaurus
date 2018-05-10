#include "cookie_authenticator.h"
#include "helpers.h"
#include "private.h"

#include <yt/core/misc/async_expiring_cache.h>

#include <yt/core/crypto/crypto.h>

#include <util/string/split.h>

namespace NYT {
namespace NBlackbox {

using namespace NYTree;
using namespace NYPath;
using namespace NCrypto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = BlackboxLogger;

////////////////////////////////////////////////////////////////////////////////

// TODO(sandello): Indicate to end-used that cookie must be resigned.
class TCookieAuthenticator
    : public ICookieAuthenticator
{
public:
    TCookieAuthenticator(TCookieAuthenticatorConfigPtr config, IBlackboxServicePtr blackbox)
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
                &TCookieAuthenticator::OnCallResult,
                MakeStrong(this),
                std::move(sessionIdMD5),
                std::move(sslSessionIdMD5)));
    }

private:
    TFuture<TAuthenticationResult> OnCallResult(const TString& sessionIdMD5, const TString& sslSessionIdMD5, const INodePtr& data)
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

        if (statusId.Value() != EBlackboxStatusId::Valid && statusId.Value() != EBlackboxStatusId::NeedReset) {
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

    const TCookieAuthenticatorConfigPtr Config_;
    const IBlackboxServicePtr Blackbox_;
};

ICookieAuthenticatorPtr CreateCookieAuthenticator(
    TCookieAuthenticatorConfigPtr config,
    IBlackboxServicePtr blackbox)
{
    return New<TCookieAuthenticator>(std::move(config), std::move(blackbox));
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

} // namespace NBlackbox
} // namespace NYT
