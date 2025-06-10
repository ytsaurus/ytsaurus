#include "oauth_cookie_authenticator.h"

#include "config.h"
#include "credentials.h"
#include "cypress_user_manager.h"
#include "helpers.h"
#include "oauth_service.h"
#include "private.h"
#include "token_authenticator.h"

#include <yt/yt/client/security_client/public.h>

#include <yt/yt/core/crypto/crypto.h>

namespace NYT::NAuth {

using namespace NYTree;
using namespace NYPath;
using namespace NCrypto;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = AuthLogger;

////////////////////////////////////////////////////////////////////////////////

class TOAuthTokenAuthenticator
    : public ITokenAuthenticator
{
public:
    TOAuthTokenAuthenticator(
        TOAuthTokenAuthenticatorConfigPtr config,
        IOAuthServicePtr oauthService,
        ICypressUserManagerPtr userManager)
        : Config_(std::move(config))
        , OAuthService_(std::move(oauthService))
        , UserManager_(std::move(userManager))
    { }

    TFuture<TAuthenticationResult> Authenticate(
        const TTokenCredentials& credentials) override
    {
        const auto& token = credentials.Token;
        auto tokenHash = GetCryptoHash(token);
        auto userIP = FormatUserIP(credentials.UserIP);

        YT_LOG_DEBUG(
            "Authenticating user with token via OAuth (TokenHash: %v, UserIP: %v)",
            tokenHash,
            userIP);

        return OAuthService_->GetUserInfo(token)
            .Apply(BIND(
                &TOAuthTokenAuthenticator::OnGetUserInfo,
                MakeStrong(this),
                std::move(tokenHash)));
    }

private:
    const TOAuthTokenAuthenticatorConfigPtr Config_;
    const IOAuthServicePtr OAuthService_;
    const ICypressUserManagerPtr UserManager_;

    TAuthenticationResult OnGetUserInfo(
        const TString& tokenHash,
        const TOAuthUserInfoResult& userInfo)
    {
        auto error = EnsureUserExists(
            Config_->CreateUserIfNotExists,
            UserManager_,
            userInfo.Login,
            Config_->DefaultUserTags);

        if (!error.IsOK()) {
            YT_LOG_DEBUG(error, "Authentication via OAuth failed (TokenHash: %v)", tokenHash);
            error <<= TErrorAttribute("token_hash", tokenHash);
            THROW_ERROR error;
        }

        auto result = TAuthenticationResult{
            .Login = userInfo.Login,
            .Realm = TString(OAuthTokenRealm),
        };
        YT_LOG_DEBUG(
            "Authentication via OAuth successful (TokenHash: %v, Login: %v, Realm: %v)",
            tokenHash,
            result.Login,
            result.Realm);
        return result;
    }
};

////////////////////////////////////////////////////////////////////////////////

ITokenAuthenticatorPtr CreateOAuthTokenAuthenticator(
    TOAuthTokenAuthenticatorConfigPtr config,
    IOAuthServicePtr oauthService,
    ICypressUserManagerPtr userManager)
{
    return New<TOAuthTokenAuthenticator>(
        std::move(config),
        std::move(oauthService),
        std::move(userManager));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
