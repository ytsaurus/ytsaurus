#include "oauth_cookie_authenticator.h"

#include "config.h"
#include "cookie_authenticator.h"
#include "credentials.h"
#include "cypress_user_manager.h"
#include "helpers.h"
#include "oauth_service.h"
#include "private.h"

#include <yt/yt/client/security_client/public.h>

#include <yt/yt/core/crypto/crypto.h>

#include <yt/yt/core/rpc/dispatcher.h>

namespace NYT::NAuth {

using namespace NYTree;
using namespace NYPath;
using namespace NCrypto;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = AuthLogger;

////////////////////////////////////////////////////////////////////////////////

class TOAuthCookieAuthenticator
    : public ICookieAuthenticator
{
public:
    TOAuthCookieAuthenticator(
        TOAuthCookieAuthenticatorConfigPtr config,
        IOAuthServicePtr oauthService,
        ICypressUserManagerPtr userManager)
        : Config_(std::move(config))
        , OAuthService_(std::move(oauthService))
        , UserManager_(std::move(userManager))
    { }

    const std::vector<TStringBuf>& GetCookieNames() const override
    {
        static const std::vector<TStringBuf> cookieNames{
            OAuthAccessTokenCookieName,
        };
        return cookieNames;
    }

    bool CanAuthenticate(const TCookieCredentials& credentials) const override
    {
        return credentials.Cookies.contains(OAuthAccessTokenCookieName);
    }

    TFuture<TAuthenticationResult> Authenticate(
        const TCookieCredentials& credentials) override
    {
        const auto& cookies = credentials.Cookies;
        auto accessToken = GetOrCrash(cookies, OAuthAccessTokenCookieName);
        auto accessTokenMD5 = GetMD5HexDigestUpperCase(accessToken);
        auto userIP = FormatUserIP(credentials.UserIP);

        YT_TLOG_DEBUG("Authenticating user via OAuth cookie")
            .With("AccessTokenMD5", accessTokenMD5)
            .With("UserIP", userIP);

        return OAuthService_->GetUserInfo(accessToken)
            .Apply(BIND(
                &TOAuthCookieAuthenticator::OnGetUserInfo,
                MakeStrong(this),
                std::move(accessTokenMD5))
                .AsyncVia(NRpc::TDispatcher::Get()->GetLightInvoker()));
    }

private:
    const TOAuthCookieAuthenticatorConfigPtr Config_;
    const IOAuthServicePtr OAuthService_;
    const ICypressUserManagerPtr UserManager_;

    TAuthenticationResult OnGetUserInfo(
        const std::string& accessTokenMD5,
        const TOAuthUserInfoResult& userInfo)
    {
        auto error = EnsureUserExists(
            Config_->CreateUserIfNotExists,
            UserManager_,
            userInfo.Login,
            Config_->DefaultUserTags);

        if (!error.IsOK()) {
            YT_TLOG_DEBUG("Authentication via OAuth failed")
                .With("AccessTokenMD5", accessTokenMD5)
                .With(error);
            error.Add("access_token_md5", accessTokenMD5);
            THROW_ERROR error;
        }

        auto result = TAuthenticationResult{
            .Login = userInfo.Login,
            .Realm = std::string(OAuthCookieRealm),
        };
        YT_TLOG_DEBUG("Authentication via OAuth successful")
            .With("AccessTokenMD5", accessTokenMD5)
            .With("Login", result.Login)
            .With("Realm", result.Realm);
        return result;
    }
};

////////////////////////////////////////////////////////////////////////////////

ICookieAuthenticatorPtr CreateOAuthCookieAuthenticator(
    TOAuthCookieAuthenticatorConfigPtr config,
    IOAuthServicePtr oauthService,
    ICypressUserManagerPtr userManager)
{
    return New<TOAuthCookieAuthenticator>(
        std::move(config),
        std::move(oauthService),
        std::move(userManager));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAuth
