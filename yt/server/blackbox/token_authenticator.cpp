#include "token_authenticator.h"
#include "helpers.h"
#include "private.h"

#include <yt/core/misc/expiring_cache.h>
#include <yt/core/crypto/crypto.h>

#include <util/string/split.h>

namespace NYT {
namespace NBlackbox {

using namespace NYTree;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = BlackboxLogger;

////////////////////////////////////////////////////////////////////////////////

class TBlackboxTokenAuthenticator
    : public ITokenAuthenticator
{
public:
    TBlackboxTokenAuthenticator(TTokenAuthenticatorConfigPtr config, IBlackboxServicePtr blackbox)
        : Config_(std::move(config))
        , Blackbox_(std::move(blackbox))
    { }

    virtual TFuture<TAuthenticationResult> Authenticate(
        const TTokenCredentials& credentials) override
    {
        const auto& token = credentials.Token;
        const auto& userIP = credentials.UserIP;
        auto tokenMD5 = TMD5Hasher().Append(token).GetHexDigestUpper();
        LOG_DEBUG(
            "Authenticating user via token (TokenMD5: %v, UserIP: %v)",
            tokenMD5,
            userIP);
        return Blackbox_->Call("oauth", {{"oauth_token", token}, {"userip", userIP}})
            .Apply(BIND(
                &TBlackboxTokenAuthenticator::OnCallResult,
                MakeStrong(this),
                std::move(tokenMD5)));
    }

private:
    TFuture<TAuthenticationResult> OnCallResult(const TString& tokenMD5, const INodePtr& data)
    {
        auto result = OnCallResultImpl(data);
        if (!result.IsOK()) {
            LOG_DEBUG(result, "Authentication failed (TokenMD5: %v)", tokenMD5);
            result.Attributes().Set("token_md5", tokenMD5);
        } else {
            LOG_DEBUG(
                "Authentication successful (TokenMD5: %v, Login: %v, Realm: %v)",
                tokenMD5,
                result.Value().Login,
                result.Value().Realm);
        }
        return MakeFuture(result);
    }

    TErrorOr<TAuthenticationResult> OnCallResultImpl(const INodePtr& data)
    {
        // See https://doc.yandex-team.ru/blackbox/reference/method-oauth-response-json.xml for reference.
        auto statusId = GetByYPath<int>(data, "/status/id");
        if (!statusId.IsOK()) {
            return TError("Blackbox returned invalid response");
        }

        if (EBlackboxStatusId(statusId.Value()) != EBlackboxStatusId::Valid) {
            auto error = GetByYPath<TString>(data, "/error");
            auto reason = error.IsOK() ? error.Value() : "unknown";
            return TError("Blackbox rejected token")
                << TErrorAttribute("reason", reason);
        }

        auto login = GetByYPath<TString>(data, "/login");
        auto oauthClientId = GetByYPath<TString>(data, "/oauth/client_id");
        auto oauthClientName = GetByYPath<TString>(data, "/oauth/client_name");
        auto oauthScope = GetByYPath<TString>(data, "/oauth/scope");

        // Sanity checks.
        if (!login.IsOK() || !oauthClientId.IsOK() || !oauthClientName.IsOK() || !oauthScope.IsOK()) {
            auto error = TError("Blackbox returned invalid response");
            if (!login.IsOK()) error.InnerErrors().push_back(login);
            if (!oauthClientId.IsOK()) error.InnerErrors().push_back(oauthClientId);
            if (!oauthClientName.IsOK()) error.InnerErrors().push_back(oauthClientName);
            if (!oauthScope.IsOK()) error.InnerErrors().push_back(oauthScope);
            return error;
        }

        // Check that token provides valid scope.
        // `oauthScope` is space-delimited list of provided scopes.
        if (Config_->EnableScopeCheck) {
            bool matchedScope = false;
            TStringBuf providedScopes(oauthScope.Value());
            TStringBuf providedScope;
            while (providedScopes.NextTok(' ', providedScope)) {
                if (providedScope == Config_->Scope) {
                    matchedScope = true;
                }
            }
            if (!matchedScope) {
                return TError("Token does not provide a valid scope")
                    << TErrorAttribute("scope", oauthScope.Value());
            }
        }

        // Check that token was issued by a known application.
        TAuthenticationResult result;
        result.Login = login.Value();
        result.Realm = "blackbox:token:" + oauthClientId.Value() + ":" + oauthClientName.Value();
        return result;
    }

    const TTokenAuthenticatorConfigPtr Config_;
    const IBlackboxServicePtr Blackbox_;
};

ITokenAuthenticatorPtr CreateBlackboxTokenAuthenticator(
    TTokenAuthenticatorConfigPtr config,
    IBlackboxServicePtr blackbox)
{
    return New<TBlackboxTokenAuthenticator>(std::move(config), std::move(blackbox));
}

////////////////////////////////////////////////////////////////////////////////

class TCachingTokenAuthenticator
    : public ITokenAuthenticator
    , private TExpiringCache<TTokenCredentials, TAuthenticationResult>
{
public:
    TCachingTokenAuthenticator(TExpiringCacheConfigPtr config, ITokenAuthenticatorPtr tokenAuthenticator)
        : TExpiringCache(std::move(config))
        , TokenAuthenticator_(std::move(tokenAuthenticator))
    { }

    virtual TFuture<TAuthenticationResult> Authenticate(const TTokenCredentials& credentials) override
    {
        return Get(credentials);
    }

private:
    virtual TFuture<TAuthenticationResult> DoGet(const TTokenCredentials& credentials) override
    {
        return TokenAuthenticator_->Authenticate(credentials);
    }

    const ITokenAuthenticatorPtr TokenAuthenticator_;
};

ITokenAuthenticatorPtr CreateCachingTokenAuthenticator(
    TExpiringCacheConfigPtr config,
    ITokenAuthenticatorPtr authenticator)
{
    return New<TCachingTokenAuthenticator>(std::move(config), std::move(authenticator));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NBlackbox
} // namespace NYT
