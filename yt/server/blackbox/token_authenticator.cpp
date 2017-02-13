#include "token_authenticator.h"
#include "helpers.h"
#include "private.h"

#include <util/string/split.h>

namespace NYT {
namespace NBlackbox {

using namespace NYTree;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = BlackboxLogger;

////////////////////////////////////////////////////////////////////////////////

class TTokenAuthenticator
    : public ITokenAuthenticator
{
public:
    TTokenAuthenticator(TTokenAuthenticatorConfigPtr config, IBlackboxServicePtr blackbox)
        : Config_(std::move(config))
        , Blackbox_(std::move(blackbox))
    { }

    virtual TFuture<TAuthenticationResult> Authenticate(
        const Stroka& token,
        const Stroka& userIP) override
    {
        auto tokenMD5 = ComputeMD5(token);
        LOG_DEBUG(
            "Authenticating user via token (TokenMD5: %v)",
            tokenMD5);
        return Blackbox_->Call("oauth", {{"oauth_token", token}, {"userip", userIP}})
            .Apply(BIND(
                &TTokenAuthenticator::OnCallResult,
                MakeStrong(this),
                std::move(tokenMD5)));
    }

private:
    TFuture<TAuthenticationResult> OnCallResult(const Stroka& tokenMD5, const INodePtr& data)
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

        if (statusId.Value() != EBlackboxStatusId::Valid) {
            auto error = GetByYPath<Stroka>(data, "/error");
            auto reason = error.IsOK() ? error.Value() : "unknown";
            return TError("Blackbox rejected token")
                << TErrorAttribute("reason", reason);
        }

        auto login = GetByYPath<Stroka>(data, "/login");
        auto oauthClientId = GetByYPath<Stroka>(data, "/oauth/client_id");
        auto oauthScope = GetByYPath<Stroka>(data, "/oauth/scope");

        // Sanity checks.
        if (!login.IsOK() || !oauthClientId.IsOK() || !oauthScope.IsOK()) {
            auto error = TError("Blackbox returned invalid response");
            if (!login.IsOK()) error.InnerErrors().push_back(login);
            if (!oauthClientId.IsOK()) error.InnerErrors().push_back(oauthClientId);
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
        Stroka realm;
        if (Config_->EnableClientIdsCheck) {
            auto clientIdIt = Config_->ClientIds.find(oauthClientId.Value());
            if (clientIdIt == Config_->ClientIds.end()) {
                return TError("Token was issued by an unknown application")
                    << TErrorAttribute("client_id", oauthClientId.Value());
            }
            realm = "blackbox:token:" + clientIdIt->second;
        } else {
            realm = "blackbox:token:" + oauthClientId.Value();
        }

        TAuthenticationResult result;
        result.Login = login.Value();
        result.Realm = realm;
        return result;
    }

    const TTokenAuthenticatorConfigPtr Config_;
    const IBlackboxServicePtr Blackbox_;
};

ITokenAuthenticatorPtr CreateTokenAuthenticator(
    TTokenAuthenticatorConfigPtr config,
    IBlackboxServicePtr blackbox)
{
    return New<TTokenAuthenticator>(std::move(config), std::move(blackbox));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NBlackbox
} // namespace NYT
